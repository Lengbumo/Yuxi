"""IM 渠道用户相关服务。

ensure_im_default_department: 启动时确保默认部门存在
resolve_im_user: IM 用户解析与创建(im-worker 直接调,不通过 HTTP)

User/Department 的创建与查询遵循 ARCHITECTURE 规范,通过 UserRepository/DepartmentRepository 访问,
不直接 session.add 模型。APIKey 无对应 repository,保留直接操作(与 Yuxi 惯例一致)。
"""
from __future__ import annotations

import logging
import os
import secrets
import time
from collections import defaultdict, deque

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from yuxi.im_channels.models import IMChannelUser
from yuxi.repositories.department_repository import DepartmentRepository
from yuxi.repositories.user_repository import UserRepository
from yuxi.storage.postgres.models_business import APIKey, User
from yuxi.utils.auth_utils import AuthUtils
from yuxi.utils.datetime_utils import utc_now_naive

logger = logging.getLogger(__name__)

# 简单内存级速率限制(每 channel 每分钟 N 次);多进程部署需换 Redis
_resolve_timestamps: dict[str, deque[float]] = defaultdict(deque)


async def ensure_im_default_department(session: AsyncSession) -> int:
    """启动时确保 IM 默认部门存在,返回部门 id。"""
    dept_name = os.getenv("IM_DEFAULT_DEPARTMENT", "IM用户").strip()
    dept = await DepartmentRepository().get_by_name_with_db(session, dept_name)
    if dept is None:
        dept = await DepartmentRepository().create_with_db(
            session,
            {"name": dept_name, "description": "IM 渠道自动创建用户默认部门"},
        )
        logger.info("[IM] Created default department %r (id=%s)", dept_name, dept.id)
    return dept.id


def _check_rate_limit(channel: str, limit: int) -> None:
    """每个 IM 渠道每分钟最多创建 limit 个新用户,超出返回 429。"""
    now = time.time()
    window = 60.0
    timestamps = _resolve_timestamps[channel]
    while timestamps and now - timestamps[0] > window:
        timestamps.popleft()
    if len(timestamps) >= limit:
        raise HTTPException(status_code=429, detail="Too many new IM users from this channel")
    timestamps.append(now)


async def resolve_im_user(
    session: AsyncSession,
    *,
    im_channel: str,
    im_user_id: str,
    im_user_name: str | None,
    rate_limit: int,
) -> tuple[str, str]:
    """解析 IM 用户到 Yuxi uid + api_key。

    首次创建用户 + API Key,返回明文 api_key(仅此一次)。
    二次调用返回同一 uid,api_key 返回空串(明文已不可恢复)。

    uid 命名:{channel}_{im_user_id}(用下划线而非冒号,避开沙盒路径 _SAFE_ID_RE
    只允许 [A-Za-z0-9_-] 的约束,冒号在 Windows 路径里也是盘符分隔符)。
    """
    _check_rate_limit(im_channel, rate_limit)

    # 已存在:更新 last_seen_at 与 im_user_name,返回空 api_key
    stmt = select(IMChannelUser).where(
        IMChannelUser.im_channel == im_channel,
        IMChannelUser.im_user_id == im_user_id,
    )
    existing = (await session.execute(stmt)).scalar_one_or_none()
    if existing:
        existing.last_seen_at = utc_now_naive()
        if im_user_name:
            existing.im_user_name = im_user_name
        await session.commit()
        return existing.yuxi_uid, ""

    # 首次:创建 Yuxi 用户 + API Key + IM 渠道用户绑定
    dept_name = os.getenv("IM_DEFAULT_DEPARTMENT", "IM用户").strip()
    dept = await DepartmentRepository().get_by_name_with_db(session, dept_name)
    if dept is None:
        # lifespan 已 ensure,理论不会走到这里;若真走到说明启动逻辑有缺陷,直接抛 500
        raise HTTPException(status_code=500, detail="IM default department not initialized")

    yuxi_uid = f"{im_channel}_{im_user_id}"
    password_hash = AuthUtils.hash_password(secrets.token_urlsafe(32))
    user = await UserRepository().create_with_db(
        session,
        {
            "username": im_user_name or im_user_id,
            "uid": yuxi_uid,
            "password_hash": password_hash,
            "role": "user",
            "department_id": dept.id,
        },
    )
    # user.id 在 create_with_db 的 flush+refresh 后已可用

    full_key, key_hash, key_prefix = AuthUtils.generate_api_key()
    api_key = APIKey(
        key_hash=key_hash,
        key_prefix=key_prefix,
        name=f"IM-{im_channel}-{im_user_id}",
        user_id=user.id,
        department_id=dept.id,
        created_by=str(user.id),
    )
    session.add(api_key)
    await session.flush()  # 拿到 api_key.id

    record = IMChannelUser(
        im_channel=im_channel,
        im_user_id=im_user_id,
        yuxi_uid=yuxi_uid,
        api_key_id=api_key.id,
        api_key_plain=full_key,  # 明文持久化(IM 渠道固有需求,im-worker 代表 IM 用户调 AgentCall)
        im_user_name=im_user_name,
    )
    session.add(record)
    await session.commit()
    logger.info(
        "[IM] Created Yuxi user for %s:%s (uid=%s, api_key_id=%s)",
        im_channel,
        im_user_id,
        yuxi_uid,
        api_key.id,
    )
    return yuxi_uid, full_key


async def match_user_by_account(session: AsyncSession, account: str) -> User | None:
    """按账号名匹配已有 Yuxi 用户(username 或 uid 精确匹配)。

    返回 None 表示无匹配,需走自动创建流程。
    """
    account = account.strip()
    if not account:
        return None
    result = await session.execute(
        select(User).where((User.username == account) | (User.uid == account), User.is_deleted == 0)
    )
    return result.scalar_one_or_none()
