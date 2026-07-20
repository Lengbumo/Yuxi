"""MessageBus pub/sub 与消息数据结构测试。"""
import asyncio
import pytest
from yuxi.im_channels.message_bus import (
    InboundMessage, InboundMessageType, MessageBus, OutboundMessage,
)


@pytest.mark.asyncio
async def test_publish_and_get_inbound_preserves_order():
    """inbound 队列保持发布顺序。"""
    bus = MessageBus()
    msgs = [
        InboundMessage(channel_name="feishu", chat_id="c1", chat_type="p2p",
                       user_id="u1", user_name="张三", text=f"msg{i}",
                       msg_type=InboundMessageType.CHAT)
        for i in range(3)
    ]
    for m in msgs:
        await bus.publish_inbound(m)

    received = []
    for _ in range(3):
        received.append(await asyncio.wait_for(bus.get_inbound(), timeout=1.0))
    assert [m.text for m in received] == ["msg0", "msg1", "msg2"]


@pytest.mark.asyncio
async def test_outbound_dispatches_to_all_listeners():
    """outbound 分发给所有订阅者。"""
    bus = MessageBus()
    received_a, received_b = [], []

    async def listener_a(msg): received_a.append(msg)
    async def listener_b(msg): received_b.append(msg)

    bus.subscribe_outbound(listener_a)
    bus.subscribe_outbound(listener_b)

    msg = OutboundMessage(channel_name="feishu", chat_id="c1", thread_id="t1", text="hi")
    await bus.publish_outbound(msg)

    assert len(received_a) == 1
    assert len(received_b) == 1
    assert received_a[0].text == "hi"


@pytest.mark.asyncio
async def test_unsubscribe_outbound_stops_delivery():
    """取消订阅后不再收到消息。"""
    bus = MessageBus()
    received = []

    async def listener(msg): received.append(msg)

    bus.subscribe_outbound(listener)
    bus.unsubscribe_outbound(listener)

    await bus.publish_outbound(OutboundMessage(channel_name="feishu", chat_id="c1", thread_id="t1", text="hi"))
    assert received == []


@pytest.mark.asyncio
async def test_outbound_listener_exception_does_not_break_others():
    """一个 listener 异常不影响其他 listener。"""
    bus = MessageBus()
    received = []

    async def bad_listener(msg): raise RuntimeError("boom")
    async def good_listener(msg): received.append(msg)

    bus.subscribe_outbound(bad_listener)
    bus.subscribe_outbound(good_listener)

    await bus.publish_outbound(OutboundMessage(channel_name="feishu", chat_id="c1", thread_id="t1", text="hi"))
    assert len(received) == 1
