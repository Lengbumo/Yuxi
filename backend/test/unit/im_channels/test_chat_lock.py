"""chat_id 串行锁测试。"""
import asyncio
import pytest
from unittest.mock import MagicMock
from yuxi.im_channels.manager import ChannelManager
from yuxi.im_channels.message_bus import InboundMessage, InboundMessageType, MessageBus


@pytest.mark.asyncio
async def test_same_chat_id_serialized():
    """同一 chat_id 的消息串行处理。"""
    bus = MessageBus()
    manager = ChannelManager(
        bus=bus, store=None, session_factory=MagicMock(), default_agent_slug="x",
    )
    order: list[int] = []
    lock_holders: list[int] = []

    async def fake_handle(msg, *, idx):
        # _get_chat_lock 是 async 函数,需先 await 拿到 Lock 再 async with
        lock = await manager._get_chat_lock(f"{msg.channel_name}:{msg.chat_id}")
        async with lock:
            order.append(idx)
            await asyncio.sleep(0.05)
            lock_holders.append(idx)

    # 并发启动 3 个同 chat_id 任务
    msgs = [
        InboundMessage(channel_name="feishu", chat_id="c1", chat_type="p2p",
                       user_id="u1", user_name="", text=str(i),
                       msg_type=InboundMessageType.CHAT)
        for i in range(3)
    ]
    await asyncio.gather(*[fake_handle(m, idx=i) for i, m in enumerate(msgs)])

    # 串行:order 与 lock_holders 顺序一致(无交错)
    assert order == lock_holders


@pytest.mark.asyncio
async def test_different_chat_id_parallel():
    """不同 chat_id 可并行。"""
    bus = MessageBus()
    manager = ChannelManager(
        bus=bus, store=None, session_factory=MagicMock(), default_agent_slug="x",
    )

    parallel_count = {"current": 0, "max": 0}

    async def fake_handle(chat_id):
        lock = await manager._get_chat_lock(f"feishu:{chat_id}")
        async with lock:
            parallel_count["current"] += 1
            parallel_count["max"] = max(parallel_count["max"], parallel_count["current"])
            await asyncio.sleep(0.05)
            parallel_count["current"] -= 1

    await asyncio.gather(*[fake_handle(f"c{i}") for i in range(3)])
    assert parallel_count["max"] >= 2  # 至少 2 个并行
