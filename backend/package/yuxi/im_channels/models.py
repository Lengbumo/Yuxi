"""IM 渠道用户与会话绑定 ORM 模型。"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from yuxi.storage.postgres.models_business import Base


class IMChannelUser(Base):
    """IM 用户 <-> Yuxi 用户绑定。

    首次发言时由 user_service.resolve_im_user 创建,yuxi_uid 形如 'feishu_{open_id}'。
    api_key_id 关联用户级 API Key,删除 Key 时 ON DELETE SET NULL 保留 IM 用户记录,
    由业务层检测到 NULL 后回查并重建。
    """

    __tablename__ = "im_channel_users"
    __table_args__ = (
        UniqueConstraint("im_channel", "im_user_id", name="uq_im_channel_user"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    im_channel: Mapped[str] = mapped_column(String(32), nullable=False)
    im_user_id: Mapped[str] = mapped_column(String(128), nullable=False)
    yuxi_uid: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    api_key_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("api_keys.id", ondelete="SET NULL"),
        nullable=True,
    )
    # IM 渠道固有需求:明文持久化 api_key,供 im-worker 代表 IM 用户调 AgentCall。
    # Yuxi api_keys 表只存 SHA-256 hash 无法恢复,内存缓存重启即丢,故需明文存储。
    # 安全约束:不暴露给前端接口,不进日志,/api/im/channels/status 只返回聚合计数。
    api_key_plain: Mapped[str | None] = mapped_column(String(128), nullable=True)
    im_user_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class IMChannelBinding(Base):
    """IM 会话 <-> Yuxi conversation thread 绑定。

    单聊按人 chat_id = 平台 user_id;群聊按群 chat_id = 平台 chat_id。
    active_run_id / active_run_owner_uid 用于 /cancel 命令的跨用户鉴权。
    """

    __tablename__ = "im_channel_bindings"
    __table_args__ = (
        UniqueConstraint("im_channel", "chat_id", name="uq_im_channel_binding"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    im_channel: Mapped[str] = mapped_column(String(32), nullable=False)
    chat_id: Mapped[str] = mapped_column(String(128), nullable=False)
    chat_type: Mapped[str] = mapped_column(String(16), nullable=False)
    yuxi_uid: Mapped[str] = mapped_column(String(64), nullable=False)
    conversation_thread_id: Mapped[str] = mapped_column(String(64), nullable=False)
    current_agent_slug: Mapped[str] = mapped_column(String(128), nullable=False)
    active_run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    active_run_owner_uid: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
