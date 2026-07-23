"""FeishuChannel 单元测试。

用 SimpleNamespace 鸭子类型构造 lark P2ImMessageReceiveV1 事件,_FakeApiClient
模拟 lark.Client.im.v1.{message,image,file,message_resource} 链式调用,
覆盖:
- _handle_incoming 文本/命令/图片/不支持类型/空文本/user_name 留空
- send 各 phase(pending 创建 card+缓存、streaming patch、final patch+fallback、error)
- send_file 图片/文件上传 + 大小限制
- _on_outbound 路由与过滤
- 纯函数 _build_card_content / _infer_file_type
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import tempfile
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pytest

from yuxi.im_channels.config import FeishuConfig
from yuxi.im_channels.message_bus import (
    InboundMessage, InboundMessageType, MessageBus, OutboundMessage, ResolvedAttachment,
)


# ---------- 辅助构造 ----------

def _make_event(
    *,
    msg_type: str = "text",
    content: dict | str | None = None,
    chat_id: str = "oc_chat001",
    chat_type: str = "p2p",
    message_id: str = "om_msg001",
    root_id: str | None = None,
    open_id: str = "ou_sender001",
) -> SimpleNamespace:
    """构造类 P2ImMessageReceiveV1 事件(鸭子类型,字段路径与 lark SDK 一致)。

    content 默认按 msg_type 自动填:text->{"text":...}, image->{"image_key":"img_k001"}。
    也可传 dict 或 JSON 字符串覆盖。
    """
    if content is None:
        if msg_type == "text":
            content_json = '{"text":"你好"}'
        elif msg_type == "image":
            content_json = '{"image_key":"img_k001"}'
        else:
            content_json = "{}"
    elif isinstance(content, dict):
        content_json = json.dumps(content)
    else:
        content_json = content

    message = SimpleNamespace(
        chat_id=chat_id,
        chat_type=chat_type,
        message_id=message_id,
        message_type=msg_type,
        content=content_json,
        root_id=root_id,
    )
    sender = SimpleNamespace(
        sender_id=SimpleNamespace(open_id=open_id, union_id="on_dup", user_id="u_dup"),
        sender_type="user",
    )
    return SimpleNamespace(event=SimpleNamespace(message=message, sender=sender))


class _FakeResponse:
    """模拟 lark API 响应:success()/data/file 字段。"""

    def __init__(self, *, success: bool = True, data=None, file: BytesIO | None = None,
                 code: int = 0, msg: str = "ok") -> None:
        self._success = success
        self.data = data
        self.file = file
        self.code = code
        self.msg = msg

    def success(self) -> bool:
        return self._success


class _FakeMessageApi:
    """记录 create/patch/reply 调用,返回 _FakeResponse。"""

    def __init__(self, *, card_msg_id: str = "om_card001",
                 patch_fails: bool = False) -> None:
        self.creates: list = []
        self.patches: list = []
        self.replies: list = []
        self._card_msg_id = card_msg_id
        self._patch_fails = patch_fails

    def create(self, request) -> _FakeResponse:
        self.creates.append(request)
        return _FakeResponse(data=SimpleNamespace(message_id=self._card_msg_id))

    def patch(self, request) -> _FakeResponse:
        self.patches.append(request)
        if self._patch_fails:
            return _FakeResponse(success=False, code=230002, msg="card not found")
        return _FakeResponse()

    def reply(self, request) -> _FakeResponse:
        self.replies.append(request)
        return _FakeResponse(data=SimpleNamespace(message_id=self._card_msg_id))


class _FakeImageApi:
    def __init__(self, *, image_key: str = "img_key_resp", success: bool = True) -> None:
        self.creates: list = []
        self._image_key = image_key
        self._success = success

    def create(self, request) -> _FakeResponse:
        self.creates.append(request)
        return _FakeResponse(success=self._success, data=SimpleNamespace(image_key=self._image_key))


class _FakeFileApi:
    def __init__(self, *, file_key: str = "file_key_resp", success: bool = True) -> None:
        self.creates: list = []
        self._file_key = file_key
        self._success = success

    def create(self, request) -> _FakeResponse:
        self.creates.append(request)
        return _FakeResponse(success=self._success, data=SimpleNamespace(file_key=self._file_key))


class _FakeMessageResourceApi:
    """message_resource.get:返回二进制 BytesIO。"""

    def __init__(self, *, content: bytes = b"\x89PNG\r\n\x1a\nfake_png",
                 success: bool = True) -> None:
        self.gets: list = []
        self._content = content
        self._success = success

    def get(self, request) -> _FakeResponse:
        self.gets.append(request)
        return _FakeResponse(success=self._success, file=BytesIO(self._content))


class _FakeApiClient:
    """模拟 lark.Client,链式 im.v1.{message,image,file,message_resource}。"""

    def __init__(self, *, message: _FakeMessageApi | None = None,
                 image: _FakeImageApi | None = None,
                 file: _FakeFileApi | None = None,
                 message_resource: _FakeMessageResourceApi | None = None) -> None:
        v1 = SimpleNamespace(
            message=message or _FakeMessageApi(),
            image=image or _FakeImageApi(),
            file=file or _FakeFileApi(),
            message_resource=message_resource or _FakeMessageResourceApi(),
        )
        self.im = SimpleNamespace(v1=v1)


def _make_channel(
    *,
    max_text_length: int = 30000,
    api_client: _FakeApiClient | None = None,
):
    """构造未启动的 FeishuChannel,直接注入 _api_client 与 builder 类引用。

    start() 里才延迟 import lark builder 类;测试不调 start,需手动填充
    _CreateMessageRequest 等引用,否则 send/send_file/_download_image 会因
    builder 为 None 早退。用真实 lark builder 类(测试环境已装),_FakeApiClient
    接管实际 API 调用,builder 只负责构造 request 对象。
    """
    from lark_oapi.api.im.v1 import (
        CreateFileRequest, CreateFileRequestBody,
        CreateImageRequest, CreateImageRequestBody,
        CreateMessageRequest, CreateMessageRequestBody,
        GetMessageResourceRequest,
        PatchMessageRequest, PatchMessageRequestBody,
        ReplyMessageRequest, ReplyMessageRequestBody,
    )

    from yuxi.im_channels.channels.feishu import FeishuChannel

    bus = MessageBus()
    config = FeishuConfig(
        enabled=True,
        app_id="cli_fake",
        app_secret="secret_fake",
        max_text_length=max_text_length,
    )
    ch = FeishuChannel(name="feishu", bus=bus, config=config)
    ch._api_client = api_client or _FakeApiClient()
    ch._CreateMessageRequest = CreateMessageRequest
    ch._CreateMessageRequestBody = CreateMessageRequestBody
    ch._ReplyMessageRequest = ReplyMessageRequest
    ch._ReplyMessageRequestBody = ReplyMessageRequestBody
    ch._PatchMessageRequest = PatchMessageRequest
    ch._PatchMessageRequestBody = PatchMessageRequestBody
    ch._CreateImageRequest = CreateImageRequest
    ch._CreateImageRequestBody = CreateImageRequestBody
    ch._CreateFileRequest = CreateFileRequest
    ch._CreateFileRequestBody = CreateFileRequestBody
    ch._GetMessageResourceRequest = GetMessageResourceRequest
    return ch


def _make_attachment(
    *,
    is_image: bool = True,
    filename: str = "img.png",
    mime: str = "image/png",
    content: bytes = b"fake_image_data",
    size: int | None = None,
) -> tuple[ResolvedAttachment, Path]:
    """构造 ResolvedAttachment 与临时文件,返回 (attachment, path)。

    tempfile.mkstemp 返回的 fd 必须显式关闭,否则 Windows 上无法删除文件。
    size 默认用 content 长度,可覆盖以测试大小限制。
    """
    fd, path = tempfile.mkstemp(suffix=filename)
    os.close(fd)
    tmp = Path(path)
    tmp.write_bytes(content)
    att = ResolvedAttachment(
        virtual_path=f"/home/gem/user-data/outputs/{filename}",
        actual_path=tmp,
        filename=filename,
        mime_type=mime,
        size=size if size is not None else len(content),
        is_image=is_image,
    )
    return att, tmp


async def _get_inbound(ch, timeout: float = 2.0) -> InboundMessage | None:
    """带超时地从 bus 取 inbound,避免代码 bug 导致 publish 未发生时测试永久卡死。"""
    try:
        return await asyncio.wait_for(ch.bus.get_inbound(), timeout=timeout)
    except TimeoutError:
        return None


# ---------- _handle_incoming 测试 ----------

@pytest.mark.asyncio
async def test_handle_incoming_text_publishes_chat():
    """文本消息(非 / 开头)发布为 CHAT 类型,chat_type 透传。"""
    ch = _make_channel()
    event = _make_event(msg_type="text", content={"text": "你好"})

    await ch._handle_incoming(event)

    inbound = await _get_inbound(ch)
    assert inbound.channel_name == "feishu"
    assert inbound.chat_id == "oc_chat001"
    assert inbound.user_id == "ou_sender001"
    assert inbound.chat_type == "p2p"
    assert inbound.msg_type == InboundMessageType.CHAT
    assert inbound.text == "你好"
    assert inbound.thread_ts == "om_msg001"


@pytest.mark.asyncio
async def test_handle_incoming_slash_publishes_command():
    """以 / 开头的文本发布为 COMMAND 类型。"""
    ch = _make_channel()
    event = _make_event(msg_type="text", content={"text": "/help"})

    await ch._handle_incoming(event)

    inbound = await _get_inbound(ch)
    assert inbound.msg_type == InboundMessageType.COMMAND
    assert inbound.text == "/help"


@pytest.mark.asyncio
async def test_handle_incoming_group_chat_type_passthrough():
    """飞书 chat_type='group' 直接透传为 Yuxi chat_type,无需转换。"""
    ch = _make_channel()
    event = _make_event(msg_type="text", content={"text": "hi"}, chat_type="group")

    await ch._handle_incoming(event)

    inbound = await _get_inbound(ch)
    assert inbound.chat_type == "group"


@pytest.mark.asyncio
async def test_handle_incoming_user_name_empty():
    """EventSender 无 name 字段,user_name 留空。"""
    ch = _make_channel()
    event = _make_event(msg_type="text", content={"text": "hi"})

    await ch._handle_incoming(event)

    inbound = await _get_inbound(ch)
    assert inbound.user_name == ""


@pytest.mark.asyncio
async def test_handle_incoming_image_downloads_and_publishes_image():
    """图片消息:message_resource.get 下载二进制 -> base64 data URL -> IMAGE 类型。"""
    image_bytes = b"\x89PNG\r\n\x1a\nfake_png_body"
    api = _FakeApiClient(message_resource=_FakeMessageResourceApi(content=image_bytes))
    ch = _make_channel(api_client=api)
    event = _make_event(msg_type="image", content={"image_key": "img_k001"})

    await ch._handle_incoming(event)

    inbound = await _get_inbound(ch)
    assert inbound.msg_type == InboundMessageType.IMAGE
    assert inbound.text == "[图片]"
    expected_b64 = base64.b64encode(image_bytes).decode("ascii")
    assert inbound.image_content_url == f"data:image/png;base64,{expected_b64}"
    assert len(api.im.v1.message_resource.gets) == 1


@pytest.mark.asyncio
async def test_handle_incoming_image_download_failure_skips():
    """图片下载失败(response.success()=False)时跳过,不发布 inbound。"""
    api = _FakeApiClient(message_resource=_FakeMessageResourceApi(success=False))
    ch = _make_channel(api_client=api)
    event = _make_event(msg_type="image", content={"image_key": "img_k001"})

    await ch._handle_incoming(event)

    assert ch.bus.inbound_queue.empty()


@pytest.mark.asyncio
async def test_handle_incoming_unsupported_type_skips():
    """不支持的消息类型(audio 等)记日志跳过,不发布 inbound。"""
    ch = _make_channel()
    event = _make_event(msg_type="audio", content={"file_key": "f1"})

    await ch._handle_incoming(event)

    assert ch.bus.inbound_queue.empty()


@pytest.mark.asyncio
async def test_handle_incoming_empty_text_skips():
    """空文本(text.strip() 为空)跳过,不发布 inbound。"""
    ch = _make_channel()
    event = _make_event(msg_type="text", content={"text": "   "})

    await ch._handle_incoming(event)

    assert ch.bus.inbound_queue.empty()


# ---------- send 测试 ----------

@pytest.mark.asyncio
async def test_send_pending_creates_running_card_and_caches_id():
    """phase=pending 用 reply 创建 running card,缓存 card_msg_id 到 thread_ts。"""
    api = _FakeApiClient(message=_FakeMessageApi(card_msg_id="om_card999"))
    ch = _make_channel(api_client=api)

    await ch.send(OutboundMessage(
        channel_name="feishu", chat_id="c1", thread_id="t1",
        text="正在思考...", phase="pending", thread_ts="om_msg001",
    ))

    assert len(api.im.v1.message.creates) == 1
    assert ch._running_card_ids["om_msg001"] == "om_card999"


@pytest.mark.asyncio
async def test_send_streaming_patches_existing_card():
    """phase=streaming patch 已缓存的 running card。"""
    api = _FakeApiClient(message=_FakeMessageApi())
    ch = _make_channel(api_client=api)
    ch._running_card_ids["om_msg001"] = "om_card999"

    await ch.send(OutboundMessage(
        channel_name="feishu", chat_id="c1", thread_id="t1",
        text="streaming chunk", phase="streaming", thread_ts="om_msg001",
    ))

    assert len(api.im.v1.message.patches) == 1
    assert api.im.v1.message.patches[0].message_id == "om_card999"


@pytest.mark.asyncio
async def test_send_streaming_without_card_skips():
    """phase=streaming 无缓存 card 时静默跳过(中间态不强制创建,下帧还会到)。"""
    api = _FakeApiClient(message=_FakeMessageApi())
    ch = _make_channel(api_client=api)

    await ch.send(OutboundMessage(
        channel_name="feishu", chat_id="c1", thread_id="t1",
        text="chunk", phase="streaming", thread_ts="om_msg001",
    ))

    assert api.im.v1.message.patches == []
    assert api.im.v1.message.creates == []
    assert api.im.v1.message.creates == []


@pytest.mark.asyncio
async def test_send_final_patches_existing_card_and_clears_cache():
    """phase=final patch 已缓存的 running card,完成后清缓存。"""
    api = _FakeApiClient(message=_FakeMessageApi())
    ch = _make_channel(api_client=api)
    ch._running_card_ids["om_msg001"] = "om_card999"

    await ch.send(OutboundMessage(
        channel_name="feishu", chat_id="c1", thread_id="t1",
        text="final answer", phase="final", is_final=True, thread_ts="om_msg001",
    ))

    assert len(api.im.v1.message.patches) == 1
    assert "om_msg001" not in ch._running_card_ids


@pytest.mark.asyncio
async def test_send_final_patch_failure_falls_back_to_reply():
    """phase=final patch 失败时 fallback reply 保证终态送达。"""
    api = _FakeApiClient(message=_FakeMessageApi(patch_fails=True))
    ch = _make_channel(api_client=api)
    ch._running_card_ids["om_msg001"] = "om_card999"

    await ch.send(OutboundMessage(
        channel_name="feishu", chat_id="c1", thread_id="t1",
        text="final answer", phase="final", is_final=True, thread_ts="om_msg001",
    ))

    assert len(api.im.v1.message.patches) == 1
    assert len(api.im.v1.message.creates) == 1


@pytest.mark.asyncio
async def test_send_final_without_card_replies():
    """phase=final 无缓存 card 时直接 reply(可能是首次即终态)。"""
    api = _FakeApiClient(message=_FakeMessageApi())
    ch = _make_channel(api_client=api)

    await ch.send(OutboundMessage(
        channel_name="feishu", chat_id="c1", thread_id="t1",
        text="final answer", phase="final", is_final=True, thread_ts="om_msg001",
    ))

    assert len(api.im.v1.message.creates) == 1
    assert api.im.v1.message.patches == []


@pytest.mark.asyncio
async def test_send_final_truncates_long_text():
    """phase=final 文本超 max_text_length 触发截断。"""
    api = _FakeApiClient(message=_FakeMessageApi())
    ch = _make_channel(max_text_length=100, api_client=api)

    long_text = "A" * 200
    await ch.send(OutboundMessage(
        channel_name="feishu", chat_id="c1", thread_id="t1",
        text=long_text, phase="final", is_final=True, thread_ts="om_msg001",
    ))

    assert len(api.im.v1.message.creates) == 1
    request = api.im.v1.message.creates[0]
    content = json.loads(request.request_body.content)
    markdown_text = content["elements"][0]["content"]
    assert len(markdown_text) <= 100
    assert markdown_text.startswith("A")


@pytest.mark.asyncio
async def test_send_error_patches_with_fallback():
    """phase=error patch 已缓存 card,失败时 fallback reply。"""
    api = _FakeApiClient(message=_FakeMessageApi(patch_fails=True))
    ch = _make_channel(api_client=api)
    ch._running_card_ids["om_msg001"] = "om_card999"

    await ch.send(OutboundMessage(
        channel_name="feishu", chat_id="c1", thread_id="t1",
        text="出错了", phase="error", is_final=True, thread_ts="om_msg001",
    ))

    assert len(api.im.v1.message.patches) == 1
    assert len(api.im.v1.message.creates) == 1
    assert "om_msg001" not in ch._running_card_ids


@pytest.mark.asyncio
async def test_send_without_api_client_skips():
    """无 api_client(send 在 start 前被调)静默跳过,不抛异常。"""
    ch = _make_channel()
    ch._api_client = None

    await ch.send(OutboundMessage(
        channel_name="feishu", chat_id="c1", thread_id="t1",
        text="hi", phase="final", thread_ts="om_msg001",
    ))
    # 未抛异常即通过


@pytest.mark.asyncio
async def test_send_without_thread_ts_creates_card():
    """无 thread_ts(出站消息无源消息 ID)时走 create 投递到 chat_id。"""
    api = _FakeApiClient(message=_FakeMessageApi())
    ch = _make_channel(api_client=api)

    await ch.send(OutboundMessage(
        channel_name="feishu", chat_id="oc_chatX", thread_id="t1",
        text="hi", phase="final",
    ))

    assert len(api.im.v1.message.creates) == 1


# ---------- send_file 测试 ----------

@pytest.mark.asyncio
async def test_send_file_image_uploads_and_replies():
    """图片附件:image.create 上传 + reply 发送 image 消息。"""
    api = _FakeApiClient()
    ch = _make_channel(api_client=api)
    att, tmp = _make_attachment(is_image=True, filename="img.png")
    try:
        msg = OutboundMessage(
            channel_name="feishu", chat_id="c1", thread_id="t1",
            text="answer", phase="final", thread_ts="om_msg001",
        )
        ok = await ch.send_file(msg, att)
        assert ok is True
        assert len(api.im.v1.image.creates) == 1
        assert len(api.im.v1.message.creates) == 1
    finally:
        tmp.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_send_file_non_image_uploads_and_replies():
    """非图片附件:file.create 上传 + reply 发送 file 消息。"""
    api = _FakeApiClient()
    ch = _make_channel(api_client=api)
    att, tmp = _make_attachment(is_image=False, filename="report.pdf", mime="application/pdf")
    try:
        msg = OutboundMessage(
            channel_name="feishu", chat_id="c1", thread_id="t1",
            text="answer", phase="final", thread_ts="om_msg001",
        )
        ok = await ch.send_file(msg, att)
        assert ok is True
        assert len(api.im.v1.file.creates) == 1
        assert len(api.im.v1.message.creates) == 1
    finally:
        tmp.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_send_file_image_too_large_skips():
    """图片超 10MB 跳过,不调用 image.create。"""
    api = _FakeApiClient()
    ch = _make_channel(api_client=api)
    att, tmp = _make_attachment(is_image=True, size=11 * 1024 * 1024)
    try:
        msg = OutboundMessage(
            channel_name="feishu", chat_id="c1", thread_id="t1",
            text="answer", phase="final", thread_ts="om_msg001",
        )
        ok = await ch.send_file(msg, att)
        assert ok is False
        assert api.im.v1.image.creates == []
    finally:
        tmp.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_send_file_file_too_large_skips():
    """非图片文件超 30MB 跳过,不调用 file.create。"""
    api = _FakeApiClient()
    ch = _make_channel(api_client=api)
    att, tmp = _make_attachment(is_image=False, filename="big.pdf", size=31 * 1024 * 1024)
    try:
        msg = OutboundMessage(
            channel_name="feishu", chat_id="c1", thread_id="t1",
            text="answer", phase="final", thread_ts="om_msg001",
        )
        ok = await ch.send_file(msg, att)
        assert ok is False
        assert api.im.v1.file.creates == []
    finally:
        tmp.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_send_file_upload_failure_returns_false():
    """上传 API 返回 success()=False 时 send_file 返回 False。"""
    api = _FakeApiClient(image=_FakeImageApi(success=False))
    ch = _make_channel(api_client=api)
    att, tmp = _make_attachment(is_image=True)
    try:
        msg = OutboundMessage(
            channel_name="feishu", chat_id="c1", thread_id="t1",
            text="answer", phase="final", thread_ts="om_msg001",
        )
        ok = await ch.send_file(msg, att)
        assert ok is False
        assert api.im.v1.message.creates == []
    finally:
        tmp.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_send_file_without_api_client_returns_false():
    """无 api_client 时 send_file 返回 False。"""
    ch = _make_channel()
    ch._api_client = None
    att, tmp = _make_attachment()
    try:
        ok = await ch.send_file(OutboundMessage(
            channel_name="feishu", chat_id="c1", thread_id="t1",
            text="a", phase="final", thread_ts="om_msg001",
        ), att)
        assert ok is False
    finally:
        tmp.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_send_file_without_thread_ts_creates():
    """无 thread_ts 时走 create 投递到 chat_id。"""
    api = _FakeApiClient()
    ch = _make_channel(api_client=api)
    att, tmp = _make_attachment()
    try:
        ok = await ch.send_file(OutboundMessage(
            channel_name="feishu", chat_id="oc_chatX", thread_id="t1",
            text="a", phase="final",
        ), att)
        assert ok is True
        assert len(api.im.v1.message.creates) == 1
    finally:
        tmp.unlink(missing_ok=True)


# ---------- _on_outbound 集成测试 ----------

@pytest.mark.asyncio
async def test_on_outbound_routes_to_send():
    """_on_outbound 把目标 channel 匹配的消息路由到 send。"""
    api = _FakeApiClient(message=_FakeMessageApi())
    ch = _make_channel(api_client=api)
    await ch._on_outbound(OutboundMessage(
        channel_name="feishu", chat_id="c1", thread_id="t1",
        text="hi", phase="final", thread_ts="om_msg001",
    ))
    assert len(api.im.v1.message.creates) == 1


@pytest.mark.asyncio
async def test_on_outbound_ignores_other_channels():
    """_on_outbound 忽略非本 channel 的消息。"""
    api = _FakeApiClient(message=_FakeMessageApi())
    ch = _make_channel(api_client=api)
    await ch._on_outbound(OutboundMessage(
        channel_name="dingtalk", chat_id="c1", thread_id="t1",
        text="hi", phase="final", thread_ts="om_msg001",
    ))
    assert api.im.v1.message.creates == []
    assert api.im.v1.message.creates == []


@pytest.mark.asyncio
async def test_on_outbound_skips_files_when_send_fails():
    """send 失败时不调 send_file,避免部分投递。"""
    api = _FakeApiClient()
    ch = _make_channel(api_client=api)

    async def _fail_send(msg): raise RuntimeError("send failed")
    ch.send = _fail_send  # type: ignore[assignment]

    att, tmp = _make_attachment()
    try:
        await ch._on_outbound(OutboundMessage(
            channel_name="feishu", chat_id="c1", thread_id="t1",
            text="hi", phase="final", thread_ts="om_msg001", attachments=[att],
        ))
        assert api.im.v1.image.creates == []
    finally:
        tmp.unlink(missing_ok=True)


# ---------- 纯函数测试 ----------

def test_build_card_content_wraps_markdown():
    """_build_card_content 把文本包成 markdown element 的卡片 JSON。"""
    from yuxi.im_channels.channels.feishu import _build_card_content

    content = _build_card_content("hello **world**")
    card = json.loads(content)
    assert card["elements"][0]["tag"] == "markdown"
    assert card["elements"][0]["content"] == "hello **world**"


def test_infer_file_type_by_extension():
    """_infer_file_type 按后缀返回飞书 file_type。"""
    from yuxi.im_channels.channels.feishu import _infer_file_type

    assert _infer_file_type(Path("a.pdf")) == "pdf"
    assert _infer_file_type(Path("a.xlsx")) == "xls"
    assert _infer_file_type(Path("a.pptx")) == "ppt"
    assert _infer_file_type(Path("a.docx")) == "doc"
    assert _infer_file_type(Path("a.zip")) == "stream"
    assert _infer_file_type(Path("noext")) == "stream"
