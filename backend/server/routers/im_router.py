"""IM 渠道管理路由。

/api/im/channels/status: 管理员查看渠道状态
"""
from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from server.utils.auth_middleware import get_admin_user, get_db
from yuxi.im_channels.models import IMChannelBinding, IMChannelUser

router = APIRouter(prefix="/im", tags=["im"])


@router.get("/channels/status", dependencies=[Depends(get_admin_user)])
async def get_channels_status(session: AsyncSession = Depends(get_db)):
    """管理员查看 IM 渠道状态(用户数、会话绑定数)。"""
    user_count_stmt = select(IMChannelUser.im_channel, func.count(IMChannelUser.id)).group_by(IMChannelUser.im_channel)
    binding_count_stmt = select(IMChannelBinding.im_channel, func.count(IMChannelBinding.id)).group_by(
        IMChannelBinding.im_channel
    )

    user_counts = {ch: n for ch, n in (await session.execute(user_count_stmt)).all()}
    binding_counts = {ch: n for ch, n in (await session.execute(binding_count_stmt)).all()}

    return {
        "channels": {
            ch: {
                "users": user_counts.get(ch, 0),
                "bindings": binding_counts.get(ch, 0),
            }
            for ch in ("feishu", "dingtalk")
        }
    }
