"""CommandParser 测试。"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from yuxi.im_channels.commands import CommandParser
from yuxi.im_channels.message_bus import InboundMessage, InboundMessageType, MessageBus


@pytest.fixture
def setup():
    bus = MessageBus()
    store = MagicMock()
    store.update_agent_slug = AsyncMock(return_value=True)
    store.reset_binding = AsyncMock(return_value=True)
    store.get_active_run = AsyncMock(return_value=None)
    store.get_binding = AsyncMock(return_value=None)  # /status 用,返回 tuple|None
    store.get_binding_record = AsyncMock(return_value=None)  # /cancel 用,返回实体|None
    store.clear_active_run = AsyncMock()
    store.set_active_run = AsyncMock()  # 用于断言命令路径不触发 run
    store.list_user_agents = AsyncMock(return_value=[{"slug": "default-chatbot"}, {"slug": "deep-research"}])
    session_factory = MagicMock()
    return CommandParser(
        store=store, session_factory=session_factory, default_agent_slug="default-chatbot",
    ), bus, store


def _msg(text):
    return InboundMessage(
        channel_name="feishu", chat_id="c1", chat_type="p2p",
        user_id="o_1", user_name="张三", text=text,
        msg_type=InboundMessageType.COMMAND, thread_ts="m1",
    )


@pytest.mark.asyncio
async def test_help_command(setup):
    parser, bus, store = setup
    outbound = []
    bus.subscribe_outbound(lambda m: outbound.append(m))
    await parser.handle(_msg("/help"), bus=bus)
    assert "可用命令" in outbound[-1].text


@pytest.mark.asyncio
async def test_unknown_command(setup):
    parser, bus, store = setup
    outbound = []
    bus.subscribe_outbound(lambda m: outbound.append(m))
    await parser.handle(_msg("/foobar"), bus=bus)
    assert "未知命令" in outbound[-1].text


@pytest.mark.asyncio
async def test_agent_use_updates_binding(setup):
    parser, bus, store = setup
    outbound = []
    bus.subscribe_outbound(lambda m: outbound.append(m))
    await parser.handle(_msg("/agent use deep-research"), bus=bus)
    store.update_agent_slug.assert_awaited_once_with("feishu", "c1", "deep-research")
    assert "已切换" in outbound[-1].text


@pytest.mark.asyncio
async def test_agent_use_rejects_invalid_slug(setup):
    """slug 含特殊字符被拒绝。"""
    parser, bus, store = setup
    outbound = []
    bus.subscribe_outbound(lambda m: outbound.append(m))
    await parser.handle(_msg("/agent use ../etc"), bus=bus)
    store.update_agent_slug.assert_not_called()
    assert "无效" in outbound[-1].text or "格式" in outbound[-1].text


@pytest.mark.asyncio
async def test_new_command_resets_binding(setup):
    parser, bus, store = setup
    outbound = []
    bus.subscribe_outbound(lambda m: outbound.append(m))
    await parser.handle(_msg("/new"), bus=bus)
    store.reset_binding.assert_awaited_once_with("feishu", "c1")


@pytest.mark.asyncio
async def test_cancel_no_active_run(setup):
    """无活跃 run 时提示无运行中的任务(get_binding_record 返回 None)。"""
    parser, bus, store = setup
    outbound = []
    bus.subscribe_outbound(lambda m: outbound.append(m))
    await parser.handle(_msg("/cancel"), bus=bus)
    assert "无运行中的任务" in outbound[-1].text


@pytest.mark.asyncio
async def test_command_path_does_not_call_agent(setup):
    """COMMAND 消息不进入 _handle_chat,不调 set_active_run(即不触发 run 提交)。"""
    parser, bus, store = setup
    await parser.handle(_msg("/help"), bus=bus)
    store.set_active_run.assert_not_called()
