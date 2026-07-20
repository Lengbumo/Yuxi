"""IM 渠道模型字段与约束测试。"""
from __future__ import annotations

import pytest

from yuxi.im_channels.models import IMChannelBinding, IMChannelUser


@pytest.mark.asyncio
async def test_im_channel_user_unique_constraint(async_db_session):
    """同一 channel + im_user_id 唯一。"""
    user1 = IMChannelUser(
        im_channel="feishu",
        im_user_id="o_xxx",
        yuxi_uid="feishu_o_xxx",
        im_user_name="张三",
    )
    async_db_session.add(user1)
    await async_db_session.commit()

    user2 = IMChannelUser(
        im_channel="feishu",
        im_user_id="o_xxx",
        yuxi_uid="feishu_o_xxx_dup",
    )
    async_db_session.add(user2)
    with pytest.raises(Exception):  # IntegrityError
        await async_db_session.commit()


@pytest.mark.asyncio
async def test_im_channel_binding_unique_constraint(async_db_session):
    """同一 channel + chat_id 唯一。"""
    binding1 = IMChannelBinding(
        im_channel="feishu",
        chat_id="chat_xxx",
        chat_type="p2p",
        yuxi_uid="feishu_o_xxx",
        conversation_thread_id="thd_xxx",
        current_agent_slug="default-chatbot",
    )
    async_db_session.add(binding1)
    await async_db_session.commit()

    binding2 = IMChannelBinding(
        im_channel="feishu",
        chat_id="chat_xxx",
        chat_type="p2p",
        yuxi_uid="feishu_o_xxx",
        conversation_thread_id="thd_yyy",
        current_agent_slug="default-chatbot",
    )
    async_db_session.add(binding2)
    with pytest.raises(Exception):
        await async_db_session.commit()


@pytest.mark.asyncio
async def test_im_channel_user_api_key_plain_persisted(async_db_session):
    """api_key_plain 字段可持久化明文(IM 渠道固有需求)。"""
    from sqlalchemy import select

    user = IMChannelUser(
        im_channel="feishu",
        im_user_id="o_plain",
        yuxi_uid="feishu_o_plain",
        api_key_id=None,
        api_key_plain="yxkey_plain_secret_value",
        im_user_name="张三",
    )
    async_db_session.add(user)
    await async_db_session.commit()

    # 重新从 DB 查询,验证明文已持久化
    stmt = select(IMChannelUser).where(IMChannelUser.im_user_id == "o_plain")
    reloaded = (await async_db_session.execute(stmt)).scalar_one()
    assert reloaded.api_key_plain == "yxkey_plain_secret_value"


@pytest.mark.asyncio
async def test_im_channel_user_api_key_set_null(async_db_session):
    """api_key_id ON DELETE SET NULL,删除 API Key 后 IM 用户保留。"""
    from yuxi.storage.postgres.models_business import APIKey, User

    # 先创建 Yuxi 用户,APIKey.user_id 外键需要
    user_row = User(username="im_test_user", uid="test:set_null", password_hash="hash")
    async_db_session.add(user_row)
    await async_db_session.commit()
    await async_db_session.refresh(user_row)

    api_key = APIKey(
        user_id=user_row.id,
        name="test",
        key_hash="a" * 64,
        key_prefix="sk-test",
        created_by=user_row.uid,
    )
    async_db_session.add(api_key)
    await async_db_session.commit()
    await async_db_session.refresh(api_key)

    im_user = IMChannelUser(
        im_channel="feishu",
        im_user_id="o_set_null",
        yuxi_uid="feishu_o_set_null",
        api_key_id=api_key.id,
    )
    async_db_session.add(im_user)
    await async_db_session.commit()
    await async_db_session.refresh(im_user)

    # 删除 API Key,DB 层 ON DELETE SET NULL 应将 api_key_id 置空
    await async_db_session.delete(api_key)
    await async_db_session.commit()

    # 重新从 DB 加载,验证 ON DELETE SET NULL 已生效
    await async_db_session.refresh(im_user)
    assert im_user.api_key_id is None
