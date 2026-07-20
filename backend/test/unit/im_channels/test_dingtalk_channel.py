"""DingtalkChannel 单元测试。

用 mock 替换 SDK 客户端/handler,httpx.MockTransport 模拟图片下载与 webhook POST,
覆盖:
- _handle_incoming 文本/命令/图片/不支持类型 解析
- send 的 phase 路由(pending/final/error/streaming 跳过)
- send_file 图片/文件上传 + webhook 投递
- final 超长截断到 max_text_length
"""
from __future__ import annotations

import base64
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from yuxi.im_channels.channels.dingtalk import (
    DingtalkChannel, _build_media_payload, _phase_title,
)
from yuxi.im_channels.config import DingtalkConfig
from yuxi.im_channels.message_bus import (
    InboundMessageType, MessageBus, OutboundMessage, ResolvedAttachment,
)


# ---------- 辅助构造 ----------

def _make_incoming(
    *,
    msg_type: str = "text",
    text: str = "你好",
    download_code: str | None = None,
    conversation_id: str = "cid_001",
    conversation_type: str = "1",
    sender_staff_id: str = "staff_001",
    sender_id: str = "sender_001",
    sender_nick: str = "张三",
    message_id: str = "msg_001",
    session_webhook: str = "https://oapi.dingtalk.com/robot/sendBySession?key=fake",
) -> SimpleNamespace:
    """构造类 ChatbotMessage 对象(鸭子类型,字段名与 SDK 一致)。"""
    text_content = SimpleNamespace(content=text) if msg_type == "text" else None
    image_content = SimpleNamespace(download_code=download_code) if msg_type == "picture" else None
    return SimpleNamespace(
        message_type=msg_type,
        text=text_content,
        image_content=image_content,
        conversation_id=conversation_id,
        conversation_type=conversation_type,
        sender_staff_id=sender_staff_id,
        sender_id=sender_id,
        sender_nick=sender_nick,
        message_id=message_id,
        session_webhook=session_webhook,
    )


def _make_channel(
    *,
    max_text_length: int = 5000,
    with_http: bool = True,
    transport: httpx.MockTransport | None = None,
) -> DingtalkChannel:
    """构造未启动的 DingtalkChannel,便于直接测试 _handle_incoming/send/send_file。"""
    bus = MessageBus()
    config = DingtalkConfig(
        enabled=True,
        app_key="fake_key",
        app_secret="fake_secret",
        robot_code="robot_001",
        max_text_length=max_text_length,
    )
    ch = DingtalkChannel(name="dingtalk", bus=bus, config=config)
    if with_http:
        ch._http = httpx.AsyncClient(transport=transport or httpx.MockTransport(lambda r: httpx.Response(404)))
    return ch


class _FakeHandler:
    """记录 reply_markdown / get_image_download_url 调用的假 handler。"""
    def __init__(self, *, download_url: str = "https://download.fake/img.png") -> None:
        self.replies: list[tuple[str, str, object]] = []
        self._download_url = download_url

    def reply_markdown(self, title: str, text: str, incoming_message) -> None:
        self.replies.append((title, text, incoming_message))

    def reply_text(self, text: str, incoming_message) -> None:
        self.replies.append(("", text, incoming_message))

    def get_image_download_url(self, download_code: str) -> str:
        return self._download_url


class _FakeClient:
    """记录 upload_to_dingtalk 调用的假 SDK 客户端。"""
    def __init__(self, *, media_id: str | None = "media_001") -> None:
        self.uploads: list[tuple[bytes, str, str, str]] = []
        self._media_id = media_id

    def upload_to_dingtalk(self, content: bytes, filetype: str, filename: str, mimetype: str) -> str | None:
        self.uploads.append((content, filetype, filename, mimetype))
        return self._media_id


# ---------- _handle_incoming 测试 ----------

@pytest.mark.asyncio
async def test_handle_incoming_text_publishes_chat():
    """文本消息(非 / 开头)发布为 CHAT 类型。"""
    ch = _make_channel()
    incoming = _make_incoming(msg_type="text", text="你好")

    await ch._handle_incoming(incoming)

    inbound = await ch.bus.get_inbound()
    assert inbound.channel_name == "dingtalk"
    assert inbound.chat_id == "cid_001"
    assert inbound.user_id == "staff_001"
    assert inbound.user_name == "张三"
    assert inbound.chat_type == "p2p"
    assert inbound.msg_type == InboundMessageType.CHAT
    assert inbound.text == "你好"
    assert inbound.thread_ts == "msg_001"


@pytest.mark.asyncio
async def test_handle_incoming_slash_publishes_command():
    """以 / 开头的文本发布为 COMMAND 类型。"""
    ch = _make_channel()
    incoming = _make_incoming(msg_type="text", text="/help")

    await ch._handle_incoming(incoming)

    inbound = await ch.bus.get_inbound()
    assert inbound.msg_type == InboundMessageType.COMMAND
    assert inbound.text == "/help"


@pytest.mark.asyncio
async def test_handle_incoming_group_chat_type():
    """conversation_type='2' 解析为 group。"""
    ch = _make_channel()
    incoming = _make_incoming(msg_type="text", text="hi", conversation_type="2")

    await ch._handle_incoming(incoming)

    inbound = await ch.bus.get_inbound()
    assert inbound.chat_type == "group"


@pytest.mark.asyncio
async def test_handle_incoming_picture_downloads_and_publishes_image():
    """图片消息:下载二进制 -> base64 data URL -> IMAGE 类型 inbound。"""
    image_bytes = b"\x89PNG\r\n\x1a\n" + b"fake_png_body"
    transport = httpx.MockTransport(lambda req: httpx.Response(
        200, content=image_bytes, headers={"content-type": "image/png"},
    ))
    ch = _make_channel(transport=transport)
    ch._handler = _FakeHandler(download_url="https://download.fake/img.png")
    incoming = _make_incoming(msg_type="picture", download_code="dc_001")

    await ch._handle_incoming(incoming)

    inbound = await ch.bus.get_inbound()
    assert inbound.msg_type == InboundMessageType.IMAGE
    assert inbound.text == "[图片]"
    expected_b64 = base64.b64encode(image_bytes).decode("ascii")
    assert inbound.image_content_url == f"data:image/png;base64,{expected_b64}"
    assert inbound.metadata["download_code"] == "dc_001"


@pytest.mark.asyncio
async def test_handle_incoming_picture_download_failure_skips():
    """图片下载失败(非 200)时跳过,不发布 inbound。"""
    transport = httpx.MockTransport(lambda req: httpx.Response(404))
    ch = _make_channel(transport=transport)
    ch._handler = _FakeHandler(download_url="https://download.fake/img.png")
    incoming = _make_incoming(msg_type="picture", download_code="dc_001")

    await ch._handle_incoming(incoming)

    assert ch.bus.inbound_queue.empty()


@pytest.mark.asyncio
async def test_handle_incoming_unsupported_type_skips():
    """不支持的消息类型(richText 等)记日志跳过,不发布 inbound。"""
    ch = _make_channel()
    incoming = _make_incoming(msg_type="richText", text="ignored")

    await ch._handle_incoming(incoming)

    assert ch.bus.inbound_queue.empty()


@pytest.mark.asyncio
async def test_handle_incoming_caches_for_reply():
    """_handle_incoming 把 incoming 缓存到 _incomings,供出站 send 使用。"""
    ch = _make_channel()
    incoming = _make_incoming(msg_type="text", text="hi", conversation_id="chat_X")

    await ch._handle_incoming(incoming)

    assert ch._incomings["chat_X"] is incoming


# ---------- send 测试 ----------

@pytest.mark.asyncio
async def test_send_pending_replies_markdown():
    """phase=pending 调用 reply_markdown,标题为"处理中"。"""
    ch = _make_channel()
    handler = _FakeHandler()
    ch._handler = handler
    incoming = _make_incoming(text="hi")
    ch._incomings["c1"] = incoming

    await ch.send(OutboundMessage(
        channel_name="dingtalk", chat_id="c1", thread_id="t1",
        text="正在思考...", phase="pending",
    ))

    assert len(handler.replies) == 1
    title, text, inc = handler.replies[0]
    assert title == "处理中"
    assert text == "正在思考..."
    assert inc is incoming


@pytest.mark.asyncio
async def test_send_final_truncates_long_text():
    """phase=final 时文本超 max_text_length 触发截断。"""
    ch = _make_channel(max_text_length=100)
    handler = _FakeHandler()
    ch._handler = handler
    ch._incomings["c1"] = _make_incoming()

    long_text = "A" * 200
    await ch.send(OutboundMessage(
        channel_name="dingtalk", chat_id="c1", thread_id="t1",
        text=long_text, phase="final",
    ))

    assert len(handler.replies) == 1
    title, text, _ = handler.replies[0]
    assert title == "回复"
    # truncate_text 在超长时追加截断提示,总长度 <= max_length
    assert len(text) <= 100
    assert text.startswith("A")


@pytest.mark.asyncio
async def test_send_final_keeps_short_text():
    """phase=final 短文本不截断。"""
    ch = _make_channel(max_text_length=5000)
    handler = _FakeHandler()
    ch._handler = handler
    ch._incomings["c1"] = _make_incoming()

    await ch.send(OutboundMessage(
        channel_name="dingtalk", chat_id="c1", thread_id="t1",
        text="short answer", phase="final",
    ))

    assert len(handler.replies) == 1
    _, text, _ = handler.replies[0]
    assert text == "short answer"


@pytest.mark.asyncio
async def test_send_error_replies_with_error_title():
    """phase=error 标题为"出错了"。"""
    ch = _make_channel()
    handler = _FakeHandler()
    ch._handler = handler
    ch._incomings["c1"] = _make_incoming()

    await ch.send(OutboundMessage(
        channel_name="dingtalk", chat_id="c1", thread_id="t1",
        text="出错了", phase="error",
    ))

    assert len(handler.replies) == 1
    title, text, _ = handler.replies[0]
    assert title == "出错了"
    assert text == "出错了"


@pytest.mark.asyncio
async def test_send_streaming_skips():
    """phase=streaming 首版不支持流式更新,静默跳过,不调 reply_markdown。"""
    ch = _make_channel()
    handler = _FakeHandler()
    ch._handler = handler
    ch._incomings["c1"] = _make_incoming()

    await ch.send(OutboundMessage(
        channel_name="dingtalk", chat_id="c1", thread_id="t1",
        text="streaming chunk", phase="streaming",
    ))

    assert handler.replies == []


@pytest.mark.asyncio
async def test_send_without_incoming_context_skips():
    """没有缓存的 incoming(无 session_webhook)时跳过,不抛异常。"""
    ch = _make_channel()
    handler = _FakeHandler()
    ch._handler = handler

    await ch.send(OutboundMessage(
        channel_name="dingtalk", chat_id="unknown_chat", thread_id="t1",
        text="hi", phase="final",
    ))

    assert handler.replies == []


# ---------- send_file 测试 ----------

def _make_attachment(*, is_image: bool = True, filename: str = "img.png",
                     mime: str = "image/png", content: bytes = b"fake_image") -> tuple[ResolvedAttachment, Path, bytes]:
    """构造 ResolvedAttachment 与临时文件,返回 (attachment, path, content)。

    tempfile.mkstemp 返回的 fd 必须显式关闭,否则 Windows 上无法删除文件。
    """
    import os
    import tempfile
    fd, path = tempfile.mkstemp(suffix=filename)
    os.close(fd)
    tmp = Path(path)
    tmp.write_bytes(content)
    att = ResolvedAttachment(
        virtual_path=f"/home/gem/user-data/outputs/{filename}",
        actual_path=tmp,
        filename=filename,
        mime_type=mime,
        size=len(content),
        is_image=is_image,
    )
    return att, tmp, content


@pytest.mark.asyncio
async def test_send_file_image_uploads_and_posts():
    """图片附件:upload_to_dingtalk + POST session_webhook 发送 image 消息。"""
    captured = {"webhook_payload": None}
    async def handler(request: httpx.Request) -> httpx.Response:
        captured["webhook_payload"] = request.read().decode()
        return httpx.Response(200, json={"errcode": 0})
    transport = httpx.MockTransport(handler)

    ch = _make_channel(transport=transport)
    fake_client = _FakeClient(media_id="media_001")
    ch._client = fake_client
    ch._incomings["c1"] = _make_incoming(sender_staff_id="staff_001")

    att, tmp, content = _make_attachment(is_image=True, filename="img.png", content=b"PNG_BODY")
    try:
        msg = OutboundMessage(channel_name="dingtalk", chat_id="c1", thread_id="t1", text="see image")
        ok = await ch.send_file(msg, att)
        assert ok is True
    finally:
        tmp.unlink(missing_ok=True)

    # 验证 upload 调用
    assert len(fake_client.uploads) == 1
    up_content, filetype, filename, mime = fake_client.uploads[0]
    assert up_content == b"PNG_BODY"
    assert filetype == "image"
    assert filename == "img.png"
    assert mime == "image/png"
    # 验证 webhook payload
    import json
    payload = json.loads(captured["webhook_payload"])
    assert payload["msgtype"] == "image"
    assert payload["image"]["mediaId"] == "media_001"
    assert payload["at"]["atUserIds"] == ["staff_001"]


@pytest.mark.asyncio
async def test_send_file_non_image_uses_file_payload():
    """非图片附件:msgtype=file。"""
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"errcode": 0})
    transport = httpx.MockTransport(handler)

    ch = _make_channel(transport=transport)
    fake_client = _FakeClient(media_id="media_file_001")
    ch._client = fake_client
    ch._incomings["c1"] = _make_incoming(sender_staff_id="staff_001")

    att, tmp, content = _make_attachment(
        is_image=False, filename="report.pdf", mime="application/pdf", content=b"%PDF-1.4",
    )
    try:
        msg = OutboundMessage(channel_name="dingtalk", chat_id="c1", thread_id="t1", text="see pdf")
        ok = await ch.send_file(msg, att)
        assert ok is True
    finally:
        tmp.unlink(missing_ok=True)

    assert len(fake_client.uploads) == 1
    _, filetype, filename, _ = fake_client.uploads[0]
    assert filetype == "file"
    assert filename == "report.pdf"


@pytest.mark.asyncio
async def test_send_file_upload_failure_returns_false():
    """upload_to_dingtalk 返回 None 时 send_file 返回 False,不调 webhook。"""
    webhook_called = {"count": 0}
    async def handler(request: httpx.Request) -> httpx.Response:
        webhook_called["count"] += 1
        return httpx.Response(200)
    transport = httpx.MockTransport(handler)

    ch = _make_channel(transport=transport)
    fake_client = _FakeClient(media_id=None)
    ch._client = fake_client
    ch._incomings["c1"] = _make_incoming()

    att, tmp, _ = _make_attachment()
    try:
        msg = OutboundMessage(channel_name="dingtalk", chat_id="c1", thread_id="t1", text="img")
        ok = await ch.send_file(msg, att)
        assert ok is False
        assert webhook_called["count"] == 0
    finally:
        tmp.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_send_file_without_context_returns_false():
    """无 incoming 上下文时 send_file 返回 False。"""
    ch = _make_channel()
    ch._client = _FakeClient()

    att, tmp, _ = _make_attachment()
    try:
        msg = OutboundMessage(channel_name="dingtalk", chat_id="unknown", thread_id="t1", text="img")
        ok = await ch.send_file(msg, att)
        assert ok is False
    finally:
        tmp.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_send_file_webhook_failure_returns_false():
    """webhook 返回非 200 时 send_file 返回 False。"""
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="internal error")
    transport = httpx.MockTransport(handler)

    ch = _make_channel(transport=transport)
    ch._client = _FakeClient(media_id="media_001")
    ch._incomings["c1"] = _make_incoming()

    att, tmp, _ = _make_attachment()
    try:
        msg = OutboundMessage(channel_name="dingtalk", chat_id="c1", thread_id="t1", text="img")
        ok = await ch.send_file(msg, att)
        assert ok is False
    finally:
        tmp.unlink(missing_ok=True)


# ---------- 纯函数测试 ----------

def test_phase_title_mapping():
    assert _phase_title("pending") == "处理中"
    assert _phase_title("error") == "出错了"
    assert _phase_title("final") == "回复"
    assert _phase_title("streaming") == "回复"


def test_build_media_payload_image():
    att = ResolvedAttachment(
        virtual_path="/x.png", actual_path=Path("/tmp/x.png"),
        filename="x.png", mime_type="image/png", size=10, is_image=True,
    )
    payload = _build_media_payload("media_001", att, "staff_001")
    assert payload["msgtype"] == "image"
    assert payload["image"]["mediaId"] == "media_001"
    assert payload["at"]["atUserIds"] == ["staff_001"]


def test_build_media_payload_file_without_sender():
    att = ResolvedAttachment(
        virtual_path="/x.pdf", actual_path=Path("/tmp/x.pdf"),
        filename="x.pdf", mime_type="application/pdf", size=10, is_image=False,
    )
    payload = _build_media_payload("media_002", att, None)
    assert payload["msgtype"] == "file"
    assert payload["file"]["mediaId"] == "media_002"
    assert payload["at"]["atUserIds"] == []


# ---------- _on_outbound 集成(经 bus 触发 send) ----------

@pytest.mark.asyncio
async def test_on_outbound_routes_to_send():
    """_on_outbound 经 bus 订阅后,匹配 channel 的消息触发 send。"""
    ch = _make_channel()
    handler = _FakeHandler()
    ch._handler = handler
    ch._incomings["c1"] = _make_incoming()
    ch.bus.subscribe_outbound(ch._on_outbound)

    await ch.bus.publish_outbound(OutboundMessage(
        channel_name="dingtalk", chat_id="c1", thread_id="t1",
        text="hello", phase="final",
    ))

    assert len(handler.replies) == 1
    _, text, _ = handler.replies[0]
    assert text == "hello"


@pytest.mark.asyncio
async def test_on_outbound_ignores_other_channels():
    """_on_outbound 忽略非 dingtalk 的消息。"""
    ch = _make_channel()
    handler = _FakeHandler()
    ch._handler = handler
    ch.bus.subscribe_outbound(ch._on_outbound)

    await ch.bus.publish_outbound(OutboundMessage(
        channel_name="feishu", chat_id="c1", thread_id="t1",
        text="hello", phase="final",
    ))

    assert handler.replies == []
