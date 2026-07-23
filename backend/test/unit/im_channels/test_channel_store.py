"""ChannelStore 测试,验证 binding 竞态与 active_run 生命周期。"""
from __future__ import annotations

import pytest

from yuxi.im_channels.models import IMChannelBinding
from yuxi.im_channels.store import ChannelStore


@pytest.mark.asyncio
async def test_get_or_create_user_uses_cache(async_db_session):
    """首次 get_or_create_user 返回空,create_user 后命中缓存。"""
    call_count = {"n": 0}

    async def fake_resolve(channel: str, im_user_id: str, im_user_name: str):
        call_count["n"] += 1
        return f"{channel}_{im_user_id}", "yxkey_cached"

    store = ChannelStore(session_factory=lambda: async_db_session, resolve_fn=fake_resolve)
    # 首次:空串,不自动创建
    uid0, key0 = await store.get_or_create_user("feishu", "o_1", "张三")
    assert uid0 == "" and key0 == ""
    # create_user 触发 resolve_fn
    uid1, key1 = await store.create_user("feishu", "o_1", "张三")
    assert uid1 == "feishu_o_1" and key1 == "yxkey_cached"
    assert call_count["n"] == 1
    # 二次:命中缓存
    uid2, key2 = await store.get_or_create_user("feishu", "o_1", "张三")
    assert uid2 == uid1 and key2 == key1
    assert call_count["n"] == 1


@pytest.mark.asyncio
async def test_get_binding_miss_returns_none(async_db_session):
    """get_binding 未命中返回 None。"""
    store = ChannelStore(session_factory=lambda: async_db_session, resolve_fn=None)
    assert await store.get_binding("feishu", "chat_1") is None


@pytest.mark.asyncio
async def test_insert_binding_then_get_hit(async_db_session):
    """insert_binding 写入后 get_binding 命中返回 (thread_id, agent_slug)。"""
    store = ChannelStore(session_factory=lambda: async_db_session, resolve_fn=None)

    await store.insert_binding(
        "feishu", "chat_1", "p2p", "feishu_o_1", "thd_new", "default-chatbot",
    )

    result = await store.get_binding("feishu", "chat_1")
    assert result == ("thd_new", "default-chatbot")


@pytest.mark.asyncio
async def test_insert_binding_unique_constraint_silent(async_db_session):
    """并发竞态:同 chat_id 重复 insert 触发 IntegrityError,静默回滚不抛。"""
    store = ChannelStore(session_factory=lambda: async_db_session, resolve_fn=None)

    await store.insert_binding(
        "feishu", "chat_1", "p2p", "feishu_o_1", "thd_1", "default-chatbot",
    )
    # 第二次 insert 同 chat_id,UNIQUE 约束触发,应静默不抛
    await store.insert_binding(
        "feishu", "chat_1", "p2p", "feishu_o_1", "thd_2", "default-chatbot",
    )

    # 原 binding 保留(thread_id 仍是第一次的)
    result = await store.get_binding("feishu", "chat_1")
    assert result == ("thd_1", "default-chatbot")


@pytest.mark.asyncio
async def test_update_agent_slug(async_db_session):
    store = ChannelStore(session_factory=lambda: async_db_session, resolve_fn=None)

    # 直接插入 binding
    binding = IMChannelBinding(
        im_channel="feishu", chat_id="c1", chat_type="p2p",
        yuxi_uid="feishu_o_1", conversation_thread_id="t1", current_agent_slug="default-chatbot",
    )
    async_db_session.add(binding)
    await async_db_session.commit()

    ok = await store.update_agent_slug("feishu", "c1", "deep-research")
    assert ok is True

    _, slug = await store.get_binding("feishu", "c1")
    assert slug == "deep-research"


@pytest.mark.asyncio
async def test_active_run_lifecycle(async_db_session):
    store = ChannelStore(session_factory=lambda: async_db_session, resolve_fn=None)

    binding = IMChannelBinding(
        im_channel="feishu", chat_id="c1", chat_type="p2p",
        yuxi_uid="feishu_o_1", conversation_thread_id="t1", current_agent_slug="default-chatbot",
    )
    async_db_session.add(binding)
    await async_db_session.commit()

    assert await store.get_active_run("feishu", "c1") is None

    await store.set_active_run("feishu", "c1", "run_1", owner_uid="feishu_o_1")
    assert await store.get_active_run("feishu", "c1") == "run_1"

    await store.clear_active_run("feishu", "c1")
    assert await store.get_active_run("feishu", "c1") is None


@pytest.mark.asyncio
async def test_get_binding_record_returns_entity(async_db_session):
    """get_binding_record 返回 binding 实体(含 active_run_owner_uid,供 /cancel)。"""
    store = ChannelStore(session_factory=lambda: async_db_session, resolve_fn=None)

    binding = IMChannelBinding(
        im_channel="feishu", chat_id="c1", chat_type="p2p",
        yuxi_uid="feishu_o_1", conversation_thread_id="t1", current_agent_slug="default-chatbot",
    )
    async_db_session.add(binding)
    await async_db_session.commit()

    record = await store.get_binding_record("feishu", "c1")
    assert record is not None
    assert record.conversation_thread_id == "t1"
    assert record.yuxi_uid == "feishu_o_1"
