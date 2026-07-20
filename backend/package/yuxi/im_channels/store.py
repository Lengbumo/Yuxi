"""ChannelStore - Postgres-backed IM 用户与会话绑定存储。

提供 get_or_create_user / get_binding / insert_binding / update_agent_slug /
reset_binding / set_active_run / clear_active_run / get_active_run /
get_binding_record / get_api_key_by_uid / list_user_agents 等方法,供 ChannelManager 调用。

binding 创建用 UNIQUE 约束 + IntegrityError 回查,防并发竞态。
api_key 内存缓存(double-check lock)避免每次消息都查库。
active_run_id 与 active_run_owner_uid 一起 set/clear,保证 /cancel 鉴权一致性。

thread 创建时机:不再由 store 先建 conversation,而是 _handle_chat 调
create_agent_invocation_run_view 时由 service 层一并建 conversation 并返回 thread_id,
store 只负责记录 binding(thread_id 由 manager 回填)。
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, UTC

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from yuxi.im_channels.models import IMChannelBinding, IMChannelUser
from yuxi.storage.postgres.models_business import User

logger = logging.getLogger(__name__)

# resolve_fn: (channel, im_user_id, im_user_name) -> (yuxi_uid, api_key_plain)
ResolveFn = Callable[[str, str, str], Awaitable[tuple[str, str]]]


class ChannelStore:
    """IM 用户与会话绑定的持久化存储。

    - session_factory:生产环境为 async_sessionmaker,每次返回新 session;
      测试环境可为 lambda 返回共享 session。
    - resolve_fn:首次解析 IM 用户时调用,负责创建 Yuxi 用户 + API Key,返回 (uid, api_key_plain)。
    """

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession] | Callable[[], AsyncSession],
        resolve_fn: ResolveFn | None,
    ) -> None:
        self._session_factory = session_factory
        self._resolve_fn = resolve_fn
        # api_key 内存缓存:(channel, im_user_id) -> (yuxi_uid, api_key_plain)
        # 避免每条消息都查库;im-worker 重启后从 im_channel_users.api_key_plain 重建
        self._api_key_cache: dict[tuple[str, str], tuple[str, str]] = {}
        self._api_key_cache_lock = asyncio.Lock()
        # 待匹配 IM 用户:(channel, im_user_id) 集合,内存态,重启丢失(用户重输即可)
        self._pending_links: set[tuple[str, str]] = set()

    def mark_pending(self, channel: str, im_user_id: str) -> None:
        self._pending_links.add((channel, im_user_id))

    def is_pending(self, channel: str, im_user_id: str) -> bool:
        return (channel, im_user_id) in self._pending_links

    def clear_pending(self, channel: str, im_user_id: str) -> None:
        self._pending_links.discard((channel, im_user_id))

    async def _session(self) -> AsyncSession:
        """获取一个 AsyncSession。生产中为 async_sessionmaker 返回的新 session。"""
        return self._session_factory()

    async def get_or_create_user(
        self, channel: str, im_user_id: str, im_user_name: str,
    ) -> tuple[str, str]:
        """返回 (yuxi_uid, api_key_plain)。

        首次:调 resolve_fn 创建 IMChannelUser 并持久化 api_key_plain。
        二次:命中内存缓存,直接返回。
        冷启动缓存缺失但 DB 已有记录:从 api_key_plain 字段恢复缓存。
        """
        cache_key = (channel, im_user_id)
        # fast path:无锁读缓存(单事件循环下 dict 读是原子的)
        if cache_key in self._api_key_cache:
            return self._api_key_cache[cache_key]

        async with self._api_key_cache_lock:
            # double-check lock:防止并发首次解析重复创建
            if cache_key in self._api_key_cache:
                return self._api_key_cache[cache_key]

            async with await self._session() as session:
                stmt = select(IMChannelUser).where(
                    IMChannelUser.im_channel == channel,
                    IMChannelUser.im_user_id == im_user_id,
                )
                existing = (await session.execute(stmt)).scalar_one_or_none()
                if existing is not None:
                    # 冷启动:DB 已有记录,从 api_key_plain 恢复缓存
                    # (api_key_plain 在首次创建时已持久化,见 resolve_im_user)
                    cached = (existing.yuxi_uid, existing.api_key_plain or "")
                    self._api_key_cache[cache_key] = cached
                    return cached

                # 首次:不自动创建,返回空串交由上层引导用户输入账号匹配
                self._api_key_cache[cache_key] = ("", "")
                return "", ""

    async def create_user(self, channel: str, im_user_id: str, im_user_name: str) -> tuple[str, str]:
        """调 resolve_fn 创建 Yuxi 用户 + APIKey + IMChannelUser,并更新缓存。

        在用户完成账号匹配(或确认自动创建)后由 manager 调用。
        """
        cache_key = (channel, im_user_id)
        if self._resolve_fn is None:
            raise RuntimeError("resolve_fn not configured")
        async with self._api_key_cache_lock:
            yuxi_uid, api_key = await self._resolve_fn(channel, im_user_id, im_user_name)
            self._api_key_cache[cache_key] = (yuxi_uid, api_key)
            return yuxi_uid, api_key

    async def link_to_existing_user(
        self, channel: str, im_user_id: str, im_user_name: str, yuxi_uid: str,
    ) -> str:
        """将 IM 用户关联到已存在的 Yuxi 用户(创建 APIKey + IMChannelUser 记录,不建新 User)。

        返回 api_key_plain,失败抛 RuntimeError。
        """

        from yuxi.repositories.user_repository import UserRepository
        from yuxi.storage.postgres.models_business import APIKey
        from yuxi.utils.auth_utils import AuthUtils

        cache_key = (channel, im_user_id)
        async with await self._session() as session:
            user_repo = UserRepository()
            user = await user_repo.get_by_uid_with_db(session, yuxi_uid)
            if user is None:
                raise RuntimeError(f"User {yuxi_uid} not found")

            full_key, key_hash, key_prefix = AuthUtils.generate_api_key()
            api_key = APIKey(
                key_hash=key_hash, key_prefix=key_prefix,
                name=f"IM-{channel}-{im_user_id}",
                user_id=user.id, department_id=user.department_id,
                created_by=str(user.id),
            )
            session.add(api_key)
            await session.flush()

            record = IMChannelUser(
                im_channel=channel, im_user_id=im_user_id,
                yuxi_uid=yuxi_uid, api_key_id=api_key.id,
                api_key_plain=full_key, im_user_name=im_user_name,
            )
            session.add(record)
            await session.commit()
            logger.info("[store] linked IM user %s:%s -> existing uid=%s", channel, im_user_id, yuxi_uid)

        self._api_key_cache[cache_key] = (yuxi_uid, full_key)
        return full_key

    async def get_binding(
        self, channel: str, chat_id: str,
    ) -> tuple[str, str] | None:
        """查 existing binding,命中返回 (conversation_thread_id, current_agent_slug),未命中返回 None。

        thread 创建不在 store 职责内:manager 调 create_agent_invocation_run_view 时由
        service 层一并建 conversation 并返回真实 thread_id,再调 insert_binding 回填。
        """
        async with await self._session() as session:
            stmt = select(IMChannelBinding).where(
                IMChannelBinding.im_channel == channel,
                IMChannelBinding.chat_id == chat_id,
            )
            binding = (await session.execute(stmt)).scalar_one_or_none()
            if binding is None:
                return None
            return binding.conversation_thread_id, binding.current_agent_slug

    async def insert_binding(
        self, channel: str, chat_id: str, chat_type: str, yuxi_uid: str,
        thread_id: str, agent_slug: str,
    ) -> None:
        """插入新 binding(thread_id 由 manager 从 run_response 回填)。

        并发竞态:UNIQUE 约束触发 IntegrityError,rollback 后静默(另一协程已插入同 chat_id 的 binding)。
        """
        async with await self._session() as session:
            binding = IMChannelBinding(
                im_channel=channel,
                chat_id=chat_id,
                chat_type=chat_type,
                yuxi_uid=yuxi_uid,
                conversation_thread_id=thread_id,
                current_agent_slug=agent_slug,
            )
            session.add(binding)
            try:
                await session.commit()
            except IntegrityError:
                # 并发竞态:另一协程已插入同 chat_id 的 binding,回滚即可
                await session.rollback()

    async def update_agent_slug(self, channel: str, chat_id: str, new_slug: str) -> bool:
        """切换当前 binding 的 agent slug。binding 不存在返回 False。"""
        async with await self._session() as session:
            stmt = select(IMChannelBinding).where(
                IMChannelBinding.im_channel == channel,
                IMChannelBinding.chat_id == chat_id,
            )
            binding = (await session.execute(stmt)).scalar_one_or_none()
            if binding is None:
                return False
            binding.current_agent_slug = new_slug
            binding.updated_at = datetime.now(UTC)
            await session.commit()
            return True

    async def reset_binding(self, channel: str, chat_id: str) -> bool:
        """删除 binding(用于 /reset 命令)。binding 不存在返回 False。"""
        async with await self._session() as session:
            stmt = select(IMChannelBinding).where(
                IMChannelBinding.im_channel == channel,
                IMChannelBinding.chat_id == chat_id,
            )
            binding = (await session.execute(stmt)).scalar_one_or_none()
            if binding is None:
                return False
            await session.delete(binding)
            await session.commit()
            return True

    async def set_active_run(
        self, channel: str, chat_id: str, run_id: str, *, owner_uid: str,
    ) -> None:
        """记录当前 binding 的活跃 run_id 与 owner_uid(/cancel 鉴权用)。

        binding 不存在视为调用方逻辑错误,抛 RuntimeError。
        """
        async with await self._session() as session:
            stmt = select(IMChannelBinding).where(
                IMChannelBinding.im_channel == channel,
                IMChannelBinding.chat_id == chat_id,
            )
            binding = (await session.execute(stmt)).scalar_one_or_none()
            if binding is None:
                raise RuntimeError(
                    f"binding not found for {channel}:{chat_id}, cannot set active run"
                )
            binding.active_run_id = run_id
            binding.active_run_owner_uid = owner_uid
            binding.updated_at = datetime.now(UTC)
            await session.commit()

    async def get_active_run(self, channel: str, chat_id: str) -> str | None:
        """返回当前 binding 的 active_run_id,无活跃 run 返回 None。"""
        async with await self._session() as session:
            stmt = select(IMChannelBinding.active_run_id).where(
                IMChannelBinding.im_channel == channel,
                IMChannelBinding.chat_id == chat_id,
            )
            return (await session.execute(stmt)).scalar_one_or_none()

    async def get_binding_record(self, channel: str, chat_id: str) -> IMChannelBinding | None:
        """返回 binding 实体(含 active_run_id / active_run_owner_uid,供 /cancel 鉴权)。"""
        async with await self._session() as session:
            stmt = select(IMChannelBinding).where(
                IMChannelBinding.im_channel == channel,
                IMChannelBinding.chat_id == chat_id,
            )
            return (await session.execute(stmt)).scalar_one_or_none()

    async def clear_active_run(self, channel: str, chat_id: str) -> None:
        """清除 active_run_id 与 active_run_owner_uid(run 结束或取消时调用)。

        binding 不存在静默返回(幂等,允许 run 结束时 binding 已被 reset)。
        """
        async with await self._session() as session:
            stmt = select(IMChannelBinding).where(
                IMChannelBinding.im_channel == channel,
                IMChannelBinding.chat_id == chat_id,
            )
            binding = (await session.execute(stmt)).scalar_one_or_none()
            if binding is None:
                return
            binding.active_run_id = None
            binding.active_run_owner_uid = None
            binding.updated_at = datetime.now(UTC)
            await session.commit()

    async def get_api_key_by_uid(self, channel: str, im_user_id: str) -> str | None:
        """从 im_channel_users 表查 api_key_plain(明文持久化,IM 渠道固有需求)。

        im-worker 去 HTTP 化后,service 层用 uid 鉴权不再需要明文 api_key;
        该方法保留供后续可能的重试/审计场景,当前 /cancel 走 uid 路径不调它。
        """
        async with await self._session() as session:
            stmt = select(IMChannelUser.api_key_plain).where(
                IMChannelUser.im_channel == channel,
                IMChannelUser.im_user_id == im_user_id,
            )
            return (await session.execute(stmt)).scalar_one_or_none()

    async def list_user_agents(self, yuxi_uid: str) -> list[dict]:
        """列出该 IM 用户在 Yuxi 可见的 Agent(供 /agent list 命令)。

        用 yuxi_uid 加载 User,调 AgentRepository.list_visible,返回 [{slug, name}, ...]。
        """
        from yuxi.repositories.agent_repository import AgentRepository

        async with await self._session() as session:
            result = await session.execute(select(User).where(User.uid == yuxi_uid))
            user = result.scalar_one_or_none()
            if user is None:
                return []
            agents = await AgentRepository(session).list_visible(user=user)
            return [{"slug": agent.slug, "name": agent.name} for agent in agents]
