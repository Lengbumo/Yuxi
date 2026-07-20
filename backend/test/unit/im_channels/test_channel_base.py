"""Channel 抽象基类行为测试。"""
import pytest
from yuxi.im_channels.base import Channel
from yuxi.im_channels.message_bus import (
    InboundMessageType, MessageBus, OutboundMessage, ResolvedAttachment,
)


class _DummyChannel(Channel):
    """测试用具体 Channel,记录 send 调用。"""
    def __init__(self, name, bus, config):
        super().__init__(name, bus, config)
        self.sent: list[OutboundMessage] = []
        self.file_uploads: list[tuple[OutboundMessage, ResolvedAttachment]] = []
        self.started = False
        self.stopped = False

    async def start(self): self.started = True
    async def stop(self): self.stopped = True
    async def send(self, msg): self.sent.append(msg)
    async def send_file(self, msg, attachment):
        self.file_uploads.append((msg, attachment))
        return True


@pytest.mark.asyncio
async def test_on_outbound_only_forwards_matching_channel():
    """_on_outbound 只转发目标 channel 匹配的消息。"""
    bus = MessageBus()
    ch = _DummyChannel("feishu", bus, {})
    bus.subscribe_outbound(ch._on_outbound)

    # 发给 dingtalk 的消息不应被 feishu channel 处理
    await bus.publish_outbound(OutboundMessage(channel_name="dingtalk", chat_id="c1", thread_id="t1", text="hi"))
    assert ch.sent == []

    # 发给 feishu 的消息应被处理
    await bus.publish_outbound(OutboundMessage(channel_name="feishu", chat_id="c1", thread_id="t1", text="hello"))
    assert len(ch.sent) == 1
    assert ch.sent[0].text == "hello"


@pytest.mark.asyncio
async def test_on_outbound_skips_files_when_text_send_fails():
    """文本发送失败时不尝试上传文件,避免部分投递。"""
    bus = MessageBus()

    class _FailSendChannel(_DummyChannel):
        async def send(self, msg): raise RuntimeError("send failed")

    ch = _FailSendChannel("feishu", bus, {})
    bus.subscribe_outbound(ch._on_outbound)

    attachment = ResolvedAttachment(
        virtual_path="/home/gem/user-data/outputs/x.png",
        actual_path=__import__("pathlib").Path("/tmp/x.png"),
        filename="x.png", mime_type="image/png", size=100, is_image=True,
    )
    msg = OutboundMessage(channel_name="feishu", chat_id="c1", thread_id="t1", text="hi", attachments=[attachment])
    await bus.publish_outbound(msg)

    assert ch.file_uploads == []  # 文件未上传


@pytest.mark.asyncio
async def test_make_inbound_factory():
    """_make_inbound 正确构造 InboundMessage。"""
    bus = MessageBus()
    ch = _DummyChannel("feishu", bus, {})
    msg = ch._make_inbound(
        chat_id="c1", user_id="u1", text="hi",
        chat_type="p2p", msg_type=InboundMessageType.CHAT,
        thread_ts="msg_1", metadata={"k": "v"},
    )
    assert msg.channel_name == "feishu"
    assert msg.chat_id == "c1"
    assert msg.metadata == {"k": "v"}
