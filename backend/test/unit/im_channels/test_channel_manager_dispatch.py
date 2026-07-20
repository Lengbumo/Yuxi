"""ChannelManager dispatcher 测试,覆盖 CHAT 路径与错误矩阵。

mock service 层函数(create_agent_invocation_run_view / stream_agent_run_events /
load_agent_run_result)与 store,验证 pending→streaming→final 全链路与错误矩阵。
不验证 mock 被调用,只验证出站消息的 phase/text 真实行为。
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from yuxi.im_channels.manager import ChannelManager
from yuxi.im_channels.message_bus import InboundMessage, InboundMessageType, MessageBus


def _make_user(uid: str = "feishu_o_1"):
    """构造简单 User 对象(只需 uid 字段供 service 层调用)。"""
    user = MagicMock()
    user.uid = uid
    return user


@pytest.fixture
def setup_manager(monkeypatch):
    """构造 manager,mock store 与 service 层函数。

    manager._load_user 默认返回 mock User;create_agent_invocation_run_view
    默认返回 {run_id, thread_id};stream_agent_run_events 默认空迭代;
    load_agent_run_result 默认返回 succeeded + output。
    """
    bus = MessageBus()
    store = MagicMock()
    store.get_or_create_user = AsyncMock(return_value=("feishu_o_1", "yxkey_user"))
    store.get_binding = AsyncMock(return_value=None)  # 首次未命中
    store.insert_binding = AsyncMock()
    store.set_active_run = AsyncMock()
    store.clear_active_run = AsyncMock()

    # mock service 层函数(patch manager 模块里的引用)
    monkeypatch.setattr(
        "yuxi.im_channels.manager.create_agent_invocation_run_view",
        AsyncMock(return_value={"run_id": "run_1", "thread_id": "thd_new", "status": "queued"}),
    )
    monkeypatch.setattr(
        "yuxi.im_channels.manager.load_agent_run_result",
        AsyncMock(return_value={"status": "succeeded", "output": "答案", "error": None}),
    )

    session_factory = MagicMock()
    manager = ChannelManager(
        bus=bus, store=store, session_factory=session_factory,
        default_agent_slug="default-chatbot",
    )
    # _load_user 默认返回 mock User(避免开真 session)
    manager._load_user = AsyncMock(return_value=_make_user())
    return manager, bus, store


def _empty_stream(*args, **kwargs):
    """空 SSE 流迭代器(立即结束,不 yield 任何行)。"""
    return
    yield  # noqa  使其成为 async generator


@pytest.mark.asyncio
async def test_chat_path_publishes_pending_then_final(setup_manager, monkeypatch):
    """CHAT 路径:首次 binding 未命中 -> pending -> submit run -> insert_binding -> final。"""
    manager, bus, store = setup_manager
    monkeypatch.setattr("yuxi.im_channels.manager.stream_agent_run_events", _empty_stream)

    outbound: list = []
    bus.subscribe_outbound(lambda m: outbound.append(m))

    msg = InboundMessage(
        channel_name="feishu", chat_id="c1", chat_type="p2p",
        user_id="o_1", user_name="张三", text="你好",
        msg_type=InboundMessageType.CHAT,
    )
    await manager._handle_message(msg)

    phases = [m.phase for m in outbound]
    assert "pending" in phases
    assert "final" in phases
    assert outbound[-1].text == "答案"
    # 首次 binding 未命中,insert_binding 应被调(thread_id 从 run_response 回填)
    store.insert_binding.assert_awaited_once()


@pytest.mark.asyncio
async def test_chat_path_existing_binding_skips_insert(setup_manager, monkeypatch):
    """已有 binding 时复用 thread_id,不调 insert_binding。"""
    manager, bus, store = setup_manager
    store.get_binding = AsyncMock(return_value=("thd_existing", "default-chatbot"))
    monkeypatch.setattr("yuxi.im_channels.manager.stream_agent_run_events", _empty_stream)

    outbound: list = []
    bus.subscribe_outbound(lambda m: outbound.append(m))

    msg = InboundMessage(
        channel_name="feishu", chat_id="c1", chat_type="p2p",
        user_id="o_1", user_name="张三", text="hi",
        msg_type=InboundMessageType.CHAT,
    )
    await manager._handle_message(msg)

    store.insert_binding.assert_not_awaited()
    assert outbound[-1].text == "答案"


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code,expected_keyword", [
    (404, "Agent 不可用"),
    (403, "无权"),
    (409, "正在处理"),
    (429, "系统繁忙"),
    (500, "服务暂时不可用"),
])
async def test_http_errors(setup_manager, monkeypatch, status_code, expected_keyword):
    """create_agent_invocation_run_view 抛 HTTPException 时按 status_code 映射 error 文案。"""
    manager, bus, store = setup_manager
    monkeypatch.setattr(
        "yuxi.im_channels.manager.create_agent_invocation_run_view",
        AsyncMock(side_effect=HTTPException(status_code=status_code, detail="test")),
    )

    outbound: list = []
    bus.subscribe_outbound(lambda m: outbound.append(m))

    msg = InboundMessage(
        channel_name="feishu", chat_id="c1", chat_type="p2p",
        user_id="o_1", user_name="张三", text="hi",
        msg_type=InboundMessageType.CHAT,
    )
    await manager._handle_message(msg)

    assert any(m.phase == "error" and expected_keyword in m.text for m in outbound)


@pytest.mark.asyncio
async def test_chat_path_user_not_found(setup_manager, monkeypatch):
    """_load_user 返回 None 时发 error 提示。"""
    manager, bus, store = setup_manager
    manager._load_user = AsyncMock(return_value=None)
    monkeypatch.setattr("yuxi.im_channels.manager.stream_agent_run_events", _empty_stream)

    outbound: list = []
    bus.subscribe_outbound(lambda m: outbound.append(m))

    msg = InboundMessage(
        channel_name="feishu", chat_id="c1", chat_type="p2p",
        user_id="o_1", user_name="张三", text="hi",
        msg_type=InboundMessageType.CHAT,
    )
    await manager._handle_message(msg)

    assert any(m.phase == "error" and "用户不存在" in m.text for m in outbound)


@pytest.mark.asyncio
async def test_chat_path_resolve_user_failure(setup_manager, monkeypatch):
    """get_or_create_user 抛异常时发身份验证失败 error。"""
    manager, bus, store = setup_manager
    store.get_or_create_user = AsyncMock(side_effect=RuntimeError("resolve failed"))
    monkeypatch.setattr("yuxi.im_channels.manager.stream_agent_run_events", _empty_stream)

    outbound: list = []
    bus.subscribe_outbound(lambda m: outbound.append(m))

    msg = InboundMessage(
        channel_name="feishu", chat_id="c1", chat_type="p2p",
        user_id="o_1", user_name="张三", text="hi",
        msg_type=InboundMessageType.CHAT,
    )
    await manager._handle_message(msg)

    assert any(m.phase == "error" and "无法验证身份" in m.text for m in outbound)
