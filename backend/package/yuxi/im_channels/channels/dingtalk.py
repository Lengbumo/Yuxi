"""DingtalkChannel - 钉钉 Stream 模式 IM 渠道实现。

基于 dingtalk-stream SDK 的 WebSocket 长连接收发出站/入站消息:
- 入站:ChatbotHandler 回调 -> 解析 ChatbotMessage -> 构造 InboundMessage -> publish_inbound
- 出站:根据 OutboundMessage.phase 调用 reply_markdown 投递到 session_webhook
- 文本:phase=pending/final/error 各发一条 markdown;phase=streaming 首版静默跳过
  (钉钉 markdown 卡片不支持原地更新,流式需用 AI 卡片,后续版本再支持)
- 图片入站:downloadCode -> 下载二进制 -> base64 data URL -> InboundMessage.image_content_url
- 文件出站:upload_to_dingtalk 换 mediaId -> POST session_webhook 发送 image/file

SDK 的 reply_* / get_image_download_url / upload_to_dingtalk 内部用同步 requests,
全部经 asyncio.to_thread 调用,避免阻塞事件循环。
"""
from __future__ import annotations

import asyncio
import base64
import logging

import httpx

from yuxi.im_channels.base import Channel
from yuxi.im_channels.config import DingtalkConfig
from yuxi.im_channels.message_bus import (
    InboundMessageType, OutboundMessage, ResolvedAttachment,
)
from yuxi.im_channels.truncation import truncate_text

logger = logging.getLogger(__name__)

# 钉钉 markdown 卡片单条文本上限(与 DingtalkConfig.max_text_length 默认值对齐)
_DEFAULT_MAX_TEXT_LENGTH = 5000


class DingtalkChannel(Channel):
    """钉钉 Stream 长连接渠道。

    start() 创建 DingTalkStreamClient 并后台运行其 start() 协程(SDK 内部维持
    WebSocket + 自动重连);stop() 取消后台任务,websocket 在 async with 退出时关闭。
    """

    def __init__(self, name: str, bus, config: DingtalkConfig) -> None:
        super().__init__(name, bus, config)
        self._config: DingtalkConfig = config
        self._client = None
        self._handler: _DingtalkHandler | None = None
        self._task: asyncio.Task | None = None
        self._http: httpx.AsyncClient | None = None
        # chat_id -> 最近一条 ChatbotMessage,出站时取其 session_webhook 回复
        # 钉钉 session_webhook 绑定会话而非单条消息,同 chat_id 任意 webhook 都能回复
        self._incomings: dict[str, object] = {}

    async def start(self) -> None:
        """启动 SDK WebSocket 长连接,注册 ChatbotHandler。"""
        import dingtalk_stream

        credential = dingtalk_stream.Credential(self._config.app_key, self._config.app_secret)
        self._client = dingtalk_stream.DingTalkStreamClient(credential)
        self._handler = _DingtalkHandler(self)
        self._client.register_callback_handler(
            dingtalk_stream.ChatbotMessage.TOPIC, self._handler,
        )
        self._http = httpx.AsyncClient(timeout=httpx.Timeout(30.0))
        # SDK 的 start() 是 async 无限重连循环,放后台任务运行
        self._task = asyncio.create_task(self._client.start(), name="dingtalk-stream")
        self._running = True
        logger.info("[dingtalk] stream client started, app_key=%s", self._config.app_key)

    async def stop(self) -> None:
        """取消后台任务,关闭 http 客户端。SDK websocket 由 async with 退出时关闭。"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        if self._http:
            await self._http.aclose()
            self._http = None
        self._client = None
        self._handler = None
        self._incomings.clear()
        logger.info("[dingtalk] stream client stopped")

    async def send(self, msg: OutboundMessage) -> None:
        """根据 phase 调用 reply_markdown 投递消息。

        - pending:回复"正在思考..."提示
        - streaming:首版不支持流式更新,静默跳过
        - final:最终答案(超长截断到 max_text_length)
        - error:错误提示
        """
        if msg.phase == "streaming":
            return
        incoming = self._incomings.get(msg.chat_id)
        if incoming is None or self._handler is None:
            logger.warning("[dingtalk] no incoming context for chat=%s, drop %s",
                           msg.chat_id, msg.phase)
            return

        title = _phase_title(msg.phase)
        text = msg.text
        if msg.phase == "final":
            text = truncate_text(text, self._config.max_text_length or _DEFAULT_MAX_TEXT_LENGTH)
        await asyncio.to_thread(self._handler.reply_markdown, title, text, incoming)

    async def send_file(self, msg: OutboundMessage, attachment: ResolvedAttachment) -> bool:
        """上传文件到钉钉换 mediaId,再 POST session_webhook 发送 image/file。"""
        incoming = self._incomings.get(msg.chat_id)
        if incoming is None or self._client is None or self._http is None:
            logger.warning("[dingtalk] send_file missing context: chat=%s file=%s",
                           msg.chat_id, attachment.filename)
            return False

        content = attachment.actual_path.read_bytes()
        filetype = "image" if attachment.is_image else "file"
        media_id = await asyncio.to_thread(
            self._client.upload_to_dingtalk,
            content, filetype, attachment.filename, attachment.mime_type,
        )
        if not media_id:
            logger.warning("[dingtalk] upload failed, file=%s", attachment.filename)
            return False

        payload = _build_media_payload(
            media_id, attachment, getattr(incoming, "sender_staff_id", None),
        )
        resp = await self._http.post(incoming.session_webhook, json=payload)
        if resp.status_code != 200:
            logger.warning("[dingtalk] send_file webhook failed: %s %s",
                           resp.status_code, resp.text[:200])
            return False
        return True

    async def _handle_incoming(self, incoming: object) -> None:
        """SDK 回调入口:把 ChatbotMessage 转成 InboundMessage 发布到 bus。

        文本:msg_type=COMMAND(text 以 / 开头) 或 CHAT
        图片:下载二进制 -> base64 data URL -> msg_type=IMAGE
        其他类型(richText 等)首版不支持,记日志跳过。
        """
        chat_id = incoming.conversation_id or ""
        user_id = incoming.sender_staff_id or incoming.sender_id or ""
        user_name = incoming.sender_nick or ""
        chat_type = "group" if incoming.conversation_type == "2" else "p2p"

        # 缓存 incoming 供出站回复使用(chat_id 维度,新消息覆盖旧消息)
        self._incomings[chat_id] = incoming

        if incoming.message_type == "text":
            text = (incoming.text.content or "").strip() if incoming.text else ""
            msg_type = InboundMessageType.COMMAND if text.startswith("/") else InboundMessageType.CHAT
            inbound = self._make_inbound(
                chat_id=chat_id, user_id=user_id, text=text,
                chat_type=chat_type, user_name=user_name,
                msg_type=msg_type, thread_ts=incoming.message_id,
            )
            await self.bus.publish_inbound(inbound)
            return

        if incoming.message_type == "picture" and incoming.image_content is not None:
            data_url = await self._download_image_as_data_url(incoming.image_content.download_code)
            if data_url is None:
                logger.warning("[dingtalk] image download failed, skip: chat=%s msg=%s",
                               chat_id, incoming.message_id)
                return
            inbound = self._make_inbound(
                chat_id=chat_id, user_id=user_id, text="[图片]",
                chat_type=chat_type, user_name=user_name,
                msg_type=InboundMessageType.IMAGE, thread_ts=incoming.message_id,
                metadata={"download_code": incoming.image_content.download_code},
            )
            inbound.image_content_url = data_url
            await self.bus.publish_inbound(inbound)
            return

        logger.info("[dingtalk] unsupported msg_type=%s, skip", incoming.message_type)

    async def _download_image_as_data_url(self, download_code: str) -> str | None:
        """downloadCode -> 下载链接 -> 二进制 -> base64 data URL。

        get_image_download_url 是 SDK 同步方法(requests.post),用 to_thread 包装。
        二进制下载走 httpx async,避免阻塞事件循环。
        """
        if self._handler is None or self._http is None:
            return None
        download_url = await asyncio.to_thread(self._handler.get_image_download_url, download_code)
        if not download_url:
            return None
        resp = await self._http.get(download_url)
        if resp.status_code != 200:
            return None
        mime = resp.headers.get("content-type", "image/png").split(";")[0].strip()
        b64 = base64.b64encode(resp.content).decode("ascii")
        return f"data:{mime};base64,{b64}"


def _phase_title(phase: str) -> str:
    """phase -> markdown 卡片标题(钉钉 markdown 卡片标题必填)。"""
    if phase == "pending":
        return "处理中"
    if phase == "error":
        return "出错了"
    return "回复"


def _build_media_payload(
    media_id: str, attachment: ResolvedAttachment, sender_staff_id: str | None,
) -> dict:
    """构造 image/file 类型 session_webhook 消息体。

    与 SDK reply_text/reply_markdown 一致,at.atUserIds 指向发送人(sender_staff_id)。
    """
    at_user_ids = [sender_staff_id] if sender_staff_id else []
    if attachment.is_image:
        return {
            "msgtype": "image",
            "image": {"mediaId": media_id},
            "at": {"atUserIds": at_user_ids},
        }
    return {
        "msgtype": "file",
        "file": {"mediaId": media_id},
        "at": {"atUserIds": at_user_ids},
    }


class _DingtalkHandler:
    """SDK ChatbotHandler 适配器,把钉钉回调转发给 DingtalkChannel。

    内部持有一个 ChatbotHandler 子类实例,继承 reply_text/reply_markdown/
    get_image_download_url 等方法,dingtalk_client 属性由 SDK 在
    register_callback_handler 时注入到内部实例。仅覆盖 process 实现入站转发。

    采用组合而非继承,是为了让 DingtalkChannel 可以在单元测试中用任意 mock
    替换 _handler,而无需 import 真实 SDK。
    """

    def __init__(self, channel: DingtalkChannel) -> None:
        import dingtalk_stream

        self._dingtalk_stream = dingtalk_stream

        class _Inner(dingtalk_stream.ChatbotHandler):
            async def process(self, callback):
                try:
                    incoming = dingtalk_stream.ChatbotMessage.from_dict(callback.data)
                    await channel._handle_incoming(incoming)
                except Exception:
                    logger.exception("[dingtalk] process callback failed")
                return dingtalk_stream.AckMessage.STATUS_OK, "OK"

        self._inner = _Inner()

    # 委托给内部 ChatbotHandler 子类实例,供 DingtalkChannel 经 to_thread 调用
    def reply_markdown(self, title: str, text: str, incoming_message) -> None:
        self._inner.reply_markdown(title, text, incoming_message)

    def reply_text(self, text: str, incoming_message) -> None:
        self._inner.reply_text(text, incoming_message)

    def get_image_download_url(self, download_code: str) -> str:
        return self._inner.get_image_download_url(download_code)

    # SDK register_callback_handler 会设置 handler.dingtalk_client,
    # 委托到内部 _inner 实例上(SDK 方法依赖该属性调 OpenAPI)
    @property
    def dingtalk_client(self):
        return self._inner.dingtalk_client

    @dingtalk_client.setter
    def dingtalk_client(self, value):
        self._inner.dingtalk_client = value

    def pre_start(self):
        return self._inner.pre_start()

    async def raw_process(self, callback_message):
        return await self._inner.raw_process(callback_message)
