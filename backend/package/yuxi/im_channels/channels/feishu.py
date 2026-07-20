"""FeishuChannel - 飞书 lark-oapi WebSocket 长连接 IM 渠道实现。

基于 lark-oapi SDK 的 WebSocket 长连接收发出站/入站消息:
- 入站:EventDispatcherHandler 注册 im.message.receive_v1 -> 解析 P2ImMessageReceiveV1
  -> 构造 InboundMessage -> publish_inbound。文本(/ 开头识别为 COMMAND)、图片
  (message_resource.get 下载二进制 -> base64 data URL -> image_content_url)
- 出站:根据 OutboundMessage.phase 用 reply/patch/create 投递 interactive 卡片
  - pending:reply 创建 running card,缓存 card_msg_id(thread_ts 为 key)
  - streaming:patch 已缓存 card(无 card 静默跳过,中间态不强制创建)
  - final/error:patch 已缓存 card,失败 fallback reply 保证终态送达;无 card 直接 reply
  - 无 thread_ts:create 投递到 chat_id(非话题场景)
- 文件出站:image.create/file.create 上传换 key -> reply 或 create 发送 image/file 消息

lark SDK 的 ws.Client.start() 同步阻塞且内部 run_until_complete,必须在独立线程
跑;同时替换 lark_oapi.ws.client 模块级 loop,避免与主线程已运行 loop 冲突。
SDK 的 im.v1.* API 调用均为同步,全部经 asyncio.to_thread 包装避免阻塞事件循环。
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import threading
from pathlib import Path
from typing import Any

from yuxi.im_channels.base import Channel
from yuxi.im_channels.config import FeishuConfig
from yuxi.im_channels.message_bus import (
    InboundMessageType, OutboundMessage, ResolvedAttachment,
)
from yuxi.im_channels.truncation import truncate_text

logger = logging.getLogger(__name__)

# 飞书 interactive 卡片单条文本上限(与 FeishuConfig.max_text_length 默认值对齐)
_DEFAULT_MAX_TEXT_LENGTH = 30000
# 图片/文件上传大小上限(飞书限制:图片 10MB,文件 30MB)
_IMAGE_MAX_SIZE = 10 * 1024 * 1024
_FILE_MAX_SIZE = 30 * 1024 * 1024
# 图片下载后的 mime(飞书 message_resource.get 不返回 content-type,统一用 png)
_IMAGE_MIME = "image/png"


class FeishuChannel(Channel):
    """飞书 lark-oapi WebSocket 长连接渠道。

    start() 在独立线程跑 lark ws.Client.start(),主线程事件循环经
    run_coroutine_threadsafe 回调 _handle_incoming;stop() 关闭线程与缓存。
    """

    def __init__(self, name: str, bus, config: FeishuConfig) -> None:
        super().__init__(name, bus, config)
        self._config: FeishuConfig = config
        self._api_client: Any = None
        self._thread: threading.Thread | None = None
        self._main_loop: asyncio.AbstractEventLoop | None = None
        # thread_ts(源消息 ID) -> running card message_id,出站 patch 用
        self._running_card_ids: dict[str, str] = {}
        self._running_card_tasks: dict[str, asyncio.Task] = {}
        # chat_id -> chat_type('p2p'/'group'),出站时按单聊/群聊选 create 或 reply_in_thread
        self._chat_types: dict[str, str] = {}
        # lark builder 类引用(start 时延迟 import 填充,send_file 用)
        self._CreateMessageRequest = None
        self._CreateMessageRequestBody = None
        self._ReplyMessageRequest = None
        self._ReplyMessageRequestBody = None
        self._PatchMessageRequest = None
        self._PatchMessageRequestBody = None
        self._CreateImageRequest = None
        self._CreateImageRequestBody = None
        self._CreateFileRequest = None
        self._CreateFileRequestBody = None
        self._GetMessageResourceRequest = None

    async def start(self) -> None:
        """延迟 import lark,建 api_client,起独立线程跑 ws.Client.start()。"""
        import lark_oapi as lark
        from lark_oapi.api.im.v1 import (
            CreateFileRequest, CreateFileRequestBody,
            CreateImageRequest, CreateImageRequestBody,
            CreateMessageRequest, CreateMessageRequestBody,
            GetMessageResourceRequest,
            PatchMessageRequest, PatchMessageRequestBody,
            ReplyMessageRequest, ReplyMessageRequestBody,
        )

        self._lark = lark
        self._CreateMessageRequest = CreateMessageRequest
        self._CreateMessageRequestBody = CreateMessageRequestBody
        self._ReplyMessageRequest = ReplyMessageRequest
        self._ReplyMessageRequestBody = ReplyMessageRequestBody
        self._PatchMessageRequest = PatchMessageRequest
        self._PatchMessageRequestBody = PatchMessageRequestBody
        self._CreateImageRequest = CreateImageRequest
        self._CreateImageRequestBody = CreateImageRequestBody
        self._CreateFileRequest = CreateFileRequest
        self._CreateFileRequestBody = CreateFileRequestBody
        self._GetMessageResourceRequest = GetMessageResourceRequest

        self._api_client = (
            lark.Client.builder()
            .app_id(self._config.app_id)
            .app_secret(self._config.app_secret)
            .domain(self._config.domain)
            .build()
        )
        self._main_loop = asyncio.get_running_loop()
        self._running = True

        self._thread = threading.Thread(
            target=self._run_ws,
            args=(self._config.app_id, self._config.app_secret, self._config.domain),
            daemon=True,
            name="feishu-ws",
        )
        self._thread.start()
        logger.info("[feishu] ws client started, app_id=%s, domain=%s",
                    self._config.app_id, self._config.domain)

    def _run_ws(self, app_id: str, app_secret: str, domain: str) -> None:
        """独立线程跑 lark ws.Client.start(),替换模块级 loop 规避 uvloop 冲突。

        lark_oapi.ws.client 模块级 loop 在 import 时捕获主线程 loop;若主线程用
        uvloop 且已运行,start() 内 loop.run_until_complete 会 RuntimeError。
        这里新建本线程 loop 并替换模块级引用,让 start() 用本线程 loop。
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            import lark_oapi as lark
            import lark_oapi.ws.client as _ws_client_mod

            _ws_client_mod.loop = loop
            event_handler = (
                lark.EventDispatcherHandler.builder("", "")
                .register_p2_im_message_receive_v1(self._on_message)
                .build()
            )
            ws_client = lark.ws.Client(
                app_id=app_id,
                app_secret=app_secret,
                event_handler=event_handler,
                log_level=lark.LogLevel.INFO,
                domain=domain,
            )
            ws_client.start()
        except Exception:
            if self._running:
                logger.exception("[feishu] WebSocket error")

    def _on_message(self, event: Any) -> None:
        """lark 回调(在 ws 线程),转发到主线程事件循环处理。"""
        if not self._main_loop or not self._main_loop.is_running():
            logger.warning("[feishu] main loop not running, drop event")
            return
        fut = asyncio.run_coroutine_threadsafe(self._handle_incoming(event), self._main_loop)
        fut.add_done_callback(lambda f: _log_future_error(f, "handle_inbound"))

    async def stop(self) -> None:
        """停止线程,取消在途 running card 任务,清缓存。

        ws.Client.start() 无独立 stop 接口,靠守护线程随进程退出;此处取消 card 任务
        并清状态,避免重启时残留。
        """
        self._running = False
        for task in list(self._running_card_tasks.values()):
            task.cancel()
        self._running_card_tasks.clear()
        self._running_card_ids.clear()
        self._chat_types.clear()
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
        self._api_client = None
        logger.info("[feishu] channel stopped")

    async def _handle_incoming(self, event: Any) -> None:
        """解析 P2ImMessageReceiveV1 -> InboundMessage -> publish_inbound。

        text:/ 开头 COMMAND 否则 CHAT;image:下载二进制转 data URL 走 IMAGE;
        其他类型(audio/post 等)首版不支持,记日志跳过;空文本跳过。
        """
        message = event.event.message
        sender = event.event.sender
        chat_id = message.chat_id
        chat_type = message.chat_type
        message_id = message.message_id
        user_id = sender.sender_id.open_id
        msg_type = message.message_type
        # 记录 chat_id -> chat_type,出站 send 时按单聊/群聊选 create 或 reply_in_thread
        self._chat_types[chat_id] = chat_type

        if msg_type == "text":
            text = _parse_text_content(message.content)
            if not text:
                logger.info("[feishu] empty text, skip: chat=%s msg=%s", chat_id, message_id)
                return
            inbound_type = InboundMessageType.COMMAND if text.startswith("/") else InboundMessageType.CHAT
            inbound = self._make_inbound(
                chat_id=chat_id, user_id=user_id, text=text,
                chat_type=chat_type, msg_type=inbound_type, thread_ts=message_id,
            )
            await self.bus.publish_inbound(inbound)
            return

        if msg_type == "image":
            image_key = _parse_image_key(message.content)
            if not image_key:
                logger.warning("[feishu] image message without image_key, skip: msg=%s", message_id)
                return
            data_url = await self._download_image_as_data_url(message_id, image_key)
            if data_url is None:
                logger.warning("[feishu] image download failed, skip: chat=%s msg=%s", chat_id, message_id)
                return
            inbound = self._make_inbound(
                chat_id=chat_id, user_id=user_id, text="[图片]",
                chat_type=chat_type, msg_type=InboundMessageType.IMAGE, thread_ts=message_id,
                metadata={"image_key": image_key},
            )
            inbound.image_content_url = data_url
            await self.bus.publish_inbound(inbound)
            return

        logger.info("[feishu] unsupported msg_type=%s, skip: chat=%s msg=%s",
                    msg_type, chat_id, message_id)

    async def _download_image_as_data_url(self, message_id: str, image_key: str) -> str | None:
        """message_resource.get 下载图片二进制 -> base64 data URL。

        lark API 同步,经 asyncio.to_thread 包装。失败(非 success)返回 None。
        """
        if not self._api_client or not self._GetMessageResourceRequest:
            return None
        request = (
            self._GetMessageResourceRequest.builder()
            .message_id(message_id)
            .file_key(image_key)
            .type("image")
            .build()
        )
        response = await asyncio.to_thread(self._api_client.im.v1.message_resource.get, request)
        if not response.success():
            logger.warning("[feishu] message_resource.get failed: code=%s msg=%s",
                           response.code, response.msg)
            return None
        content = response.file.read()
        b64 = base64.b64encode(content).decode("ascii")
        return f"data:{_IMAGE_MIME};base64,{b64}"

    async def send(self, msg: OutboundMessage) -> None:
        """根据 phase 路由 reply/patch/create 投递 interactive 卡片。

        final 阶段超长截断到 max_text_length;streaming 无缓存 card 静默跳过;
        final/error patch 失败 fallback reply 保证终态送达。
        """
        if not self._api_client:
            logger.warning("[feishu] send called but no api_client, skip: chat=%s phase=%s",
                           msg.chat_id, msg.phase)
            return

        text = msg.text
        if msg.phase == "final":
            text = truncate_text(text, self._config.max_text_length or _DEFAULT_MAX_TEXT_LENGTH)
        await self._send_card_message(msg, text)

    async def _send_card_message(self, msg: OutboundMessage, text: str) -> None:
        """按 thread_ts / 缓存 card / is_final / phase 分发 patch/reply/create。

        - 有 thread_ts:已缓存 card 则 patch(失败时 final/error fallback reply,
          streaming 静默);无缓存则 pending/首帧 reply 创建并缓存,streaming 跳过
        - 无 thread_ts:create 投递到 chat_id(非话题场景)
        """
        source_message_id = msg.thread_ts
        if not source_message_id:
            await self._create_card(msg.chat_id, text)
            return

        card_msg_id = self._running_card_ids.get(source_message_id)
        if not card_msg_id:
            if msg.phase == "streaming":
                logger.info("[feishu] streaming without running card, skip: source=%s",
                            source_message_id)
                return
            chat_type = self._chat_types.get(msg.chat_id, "p2p")
            if chat_type == "group":
                card_msg_id = await self._reply_card(source_message_id, text)
            else:
                card_msg_id = await self._create_card(msg.chat_id, text)
            if card_msg_id:
                self._running_card_ids[source_message_id] = card_msg_id
            if msg.is_final:
                return
            return

        try:
            await self._patch_card(card_msg_id, text)
        except Exception:
            if msg.is_final:
                logger.exception("[feishu] patch failed, fallback: card=%s", card_msg_id)
                chat_type = self._chat_types.get(msg.chat_id, "p2p")
                if chat_type == "group":
                    await self._reply_card(source_message_id, text)
                else:
                    await self._create_card(msg.chat_id, text)
            else:
                logger.warning("[feishu] streaming patch failed, skip: card=%s", card_msg_id)

        if msg.is_final:
            self._running_card_ids.pop(source_message_id, None)

    async def _reply_card(self, message_id: str, text: str) -> str | None:
        """reply 创建 interactive 卡片,返回 card message_id。"""
        request = (
            self._ReplyMessageRequest.builder()
            .message_id(message_id)
            .request_body(
                self._ReplyMessageRequestBody.builder()
                .msg_type("interactive")
                .content(_build_card_content(text))
                .reply_in_thread(True)
                .build()
            )
            .build()
        )
        response = await asyncio.to_thread(self._api_client.im.v1.message.reply, request)
        return getattr(getattr(response, "data", None), "message_id", None)

    async def _patch_card(self, card_msg_id: str, text: str) -> None:
        """patch 更新已存在卡片的 content(原地更新)。失败(API 返回非 success)抛异常,
        由 _send_card_message 捕获后 fallback reply(终态)或静默(streaming)。
        """
        request = (
            self._PatchMessageRequest.builder()
            .message_id(card_msg_id)
            .request_body(
                self._PatchMessageRequestBody.builder()
                .content(_build_card_content(text))
                .build()
            )
            .build()
        )
        response = await asyncio.to_thread(self._api_client.im.v1.message.patch, request)
        if not response.success():
            raise RuntimeError(f"feishu patch failed: code={response.code} msg={response.msg}")

    async def _create_card(self, chat_id: str, text: str) -> str | None:
        """create 在目标 chat_id 新建 interactive 卡片,返回 card message_id(供后续 patch)。"""
        request = (
            self._CreateMessageRequest.builder()
            .receive_id_type("chat_id")
            .request_body(
                self._CreateMessageRequestBody.builder()
                .receive_id(chat_id)
                .msg_type("interactive")
                .content(_build_card_content(text))
                .build()
            )
            .build()
        )
        response = await asyncio.to_thread(self._api_client.im.v1.message.create, request)
        return getattr(getattr(response, "data", None), "message_id", None)

    async def send_file(self, msg: OutboundMessage, attachment: ResolvedAttachment) -> bool:
        """上传图片/文件到飞书换 key,reply 或 create 发送 image/file 消息。

        大小超限(图片 10MB / 文件 30MB)跳过;上传失败返回 False。
        """
        if not self._api_client:
            logger.warning("[feishu] send_file without api_client, skip: %s", attachment.filename)
            return False

        if attachment.is_image and attachment.size > _IMAGE_MAX_SIZE:
            logger.warning("[feishu] image too large (%d bytes), skip: %s",
                           attachment.size, attachment.filename)
            return False
        if not attachment.is_image and attachment.size > _FILE_MAX_SIZE:
            logger.warning("[feishu] file too large (%d bytes), skip: %s",
                           attachment.size, attachment.filename)
            return False

        try:
            if attachment.is_image:
                file_key = await self._upload_image(attachment.actual_path)
                msg_type = "image"
                content = json.dumps({"image_key": file_key})
            else:
                file_key = await self._upload_file(attachment.actual_path, attachment.filename)
                msg_type = "file"
                content = json.dumps({"file_key": file_key})
        except Exception:
            logger.exception("[feishu] upload failed: %s", attachment.filename)
            return False

        if not file_key:
            return False

        if msg.thread_ts:
            chat_type = self._chat_types.get(msg.chat_id, "p2p")
            if chat_type == "group":
                request = (
                    self._ReplyMessageRequest.builder()
                    .message_id(msg.thread_ts)
                    .request_body(
                        self._ReplyMessageRequestBody.builder()
                        .msg_type(msg_type)
                        .content(content)
                        .reply_in_thread(True)
                        .build()
                    )
                    .build()
                )
                await asyncio.to_thread(self._api_client.im.v1.message.reply, request)
            else:
                request = (
                    self._CreateMessageRequest.builder()
                    .receive_id_type("chat_id")
                    .request_body(
                        self._CreateMessageRequestBody.builder()
                        .receive_id(msg.chat_id)
                        .msg_type(msg_type)
                        .content(content)
                        .build()
                    )
                    .build()
                )
                await asyncio.to_thread(self._api_client.im.v1.message.create, request)
        else:
            request = (
                self._CreateMessageRequest.builder()
                .receive_id_type("chat_id")
                .request_body(
                    self._CreateMessageRequestBody.builder()
                    .receive_id(msg.chat_id)
                    .msg_type(msg_type)
                    .content(content)
                    .build()
                )
                .build()
            )
            await asyncio.to_thread(self._api_client.im.v1.message.create, request)
        logger.info("[feishu] file sent: %s (type=%s)", attachment.filename, msg_type)
        return True

    async def _upload_image(self, path: Path) -> str | None:
        """image.create 上传图片,返回 image_key。"""
        with open(str(path), "rb") as f:
            request = (
                self._CreateImageRequest.builder()
                .request_body(
                    self._CreateImageRequestBody.builder()
                    .image_type("message")
                    .image(f)
                    .build()
                )
                .build()
            )
            response = await asyncio.to_thread(self._api_client.im.v1.image.create, request)
        if not response.success():
            raise RuntimeError(f"feishu image upload failed: code={response.code} msg={response.msg}")
        return response.data.image_key

    async def _upload_file(self, path: Path, filename: str) -> str | None:
        """file.create 上传文件,返回 file_key。file_type 按后缀推断。"""
        file_type = _infer_file_type(path)
        with open(str(path), "rb") as f:
            request = (
                self._CreateFileRequest.builder()
                .request_body(
                    self._CreateFileRequestBody.builder()
                    .file_type(file_type)
                    .file_name(filename)
                    .file(f)
                    .build()
                )
                .build()
            )
            response = await asyncio.to_thread(self._api_client.im.v1.file.create, request)
        if not response.success():
            raise RuntimeError(f"feishu file upload failed: code={response.code} msg={response.msg}")
        return response.data.file_key


# ---------- 模块级辅助函数 ----------

def _build_card_content(text: str) -> str:
    """把文本包成飞书 interactive 卡片 JSON(markdown element 渲染 markdown)。"""
    card = {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "elements": [{"tag": "markdown", "content": text}],
    }
    return json.dumps(card)


def _infer_file_type(path: Path) -> str:
    """按后缀返回飞书 file_type;未知后缀用 stream。"""
    suffix = path.suffix.lower()
    if suffix in (".xls", ".xlsx", ".csv"):
        return "xls"
    if suffix in (".ppt", ".pptx"):
        return "ppt"
    if suffix == ".pdf":
        return "pdf"
    if suffix in (".doc", ".docx"):
        return "doc"
    return "stream"


def _parse_text_content(content_json: str) -> str:
    """解析飞书文本消息 content(JSON 字符串 {"text": "..."}) -> 去除 @ 机器人标记后的纯文本。

    飞书文本消息 content 形如 '{"text":"@_user_1 hello"}',mentions 里有 @ 信息,
    首版仅取 text 字段(@ 用户的占位符会留在文本里,后续若需清理再补)。
    """
    try:
        content = json.loads(content_json)
    except (json.JSONDecodeError, TypeError):
        return ""
    text = content.get("text", "") if isinstance(content, dict) else ""
    return (text or "").strip()


def _parse_image_key(content_json: str) -> str | None:
    """解析飞书图片消息 content '{"image_key":"..."}' -> image_key。"""
    try:
        content = json.loads(content_json)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(content, dict):
        return None
    key = content.get("image_key")
    return key if isinstance(key, str) and key else None


def _log_future_error(fut: Any, name: str) -> None:
    """run_coroutine_threadsafe future 的 done_callback,把异常打到日志。"""
    try:
        exc = fut.exception()
        if exc:
            logger.error("[feishu] %s failed: %s", name, exc)
    except Exception:
        pass
