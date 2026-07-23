"""CommandParser - IM 命令解析与执行。

支持命令:/help /new /status /agent list /agent use <slug> /cancel
slug 用正则 ^[a-z0-9-]+$ 校验,防命令注入。

/cancel 调 service 层 request_cancel_agent_run(用 active_run_owner_uid 鉴权),
不通过 HTTP。
"""
from __future__ import annotations

import logging
import re

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from yuxi.im_channels.message_bus import InboundMessage, MessageBus, OutboundMessage
from yuxi.services.agent_run_service import request_cancel_agent_run

logger = logging.getLogger(__name__)

_SLUG_PATTERN = re.compile(r"^[a-z0-9-]+$")

_HELP_TEXT = """可用命令:
/help - 显示本帮助
/new - 重置当前会话(下次发言开启新对话)
/status - 查看当前 Agent 与会话状态
/agent list - 列出可切换的 Agent
/agent use <slug> - 切换 Agent
/cancel - 取消当前运行中的任务"""


class CommandParser:
    def __init__(
        self,
        *,
        store,
        session_factory: async_sessionmaker[AsyncSession],
        default_agent_slug: str,
    ) -> None:
        self.store = store
        self._session_factory = session_factory
        self._default_agent_slug = default_agent_slug

    async def handle(self, msg: InboundMessage, *, bus: MessageBus) -> None:
        text = msg.text.strip()
        parts = text.split(maxsplit=2)
        command = parts[0].lower().lstrip("/")

        if command == "help":
            await self._reply(msg, bus, _HELP_TEXT)
        elif command == "new":
            await self.store.reset_binding(msg.channel_name, msg.chat_id)
            await self._reply(msg, bus, "会话已重置,下次发言将开启新对话")
        elif command == "status":
            binding = await self.store.get_binding(msg.channel_name, msg.chat_id)
            if binding:
                thread_id, agent_slug = binding
                await self._reply(msg, bus, f"当前 Agent: {agent_slug}\n会话 ID: {thread_id}")
            else:
                await self._reply(msg, bus, "当前无活跃会话,发送任意消息开始")
        elif command == "agent":
            await self._handle_agent(msg, bus, parts[1:])
        elif command == "cancel":
            await self._handle_cancel(msg, bus)
        else:
            await self._reply(msg, bus, f"未知命令: /{command}\n\n{_HELP_TEXT}")

    async def _handle_agent(self, msg: InboundMessage, bus: MessageBus, args: list[str]) -> None:
        if not args:
            await self._reply(msg, bus, "用法:/agent list 或 /agent use <slug>")
            return

        sub = args[0].lower()
        if sub == "list":
            # yuxi_uid 形如 '{channel}_{im_user_id}',InboundMessage.user_id 即 IM 平台 user_id,
            # 需拼成完整 uid 再查;manager 的 get_or_create_user 已返回 yuxi_uid,
            # 但命令路径未走 manager 的 user 解析,这里用 channel_user_id 拼接
            yuxi_uid = f"{msg.channel_name}_{msg.user_id}"
            agents = await self.store.list_user_agents(yuxi_uid=yuxi_uid)
            names = "\n".join(f"- {a['slug']}" for a in agents)
            await self._reply(msg, bus, f"可切换的 Agent:\n{names}")
        elif sub == "use":
            if len(args) < 2:
                await self._reply(msg, bus, "用法:/agent use <slug>")
                return
            slug = args[1]
            if not _SLUG_PATTERN.fullmatch(slug):
                await self._reply(msg, bus, "⚠️ Agent slug 格式无效,只允许小写字母、数字与连字符")
                return
            ok = await self.store.update_agent_slug(msg.channel_name, msg.chat_id, slug)
            if ok:
                await self._reply(msg, bus, f"已切换到 Agent: {slug}")
            else:
                await self._reply(msg, bus, "⚠️ 当前无活跃会话,请先发送任意消息开始对话")
        else:
            await self._reply(msg, bus, f"未知子命令: /agent {sub}\n用法:/agent list 或 /agent use <slug>")

    async def _handle_cancel(self, msg: InboundMessage, bus: MessageBus) -> None:
        """取消当前 binding 的活跃 run,用 active_run_owner_uid 鉴权调 service 层。

        群聊场景:owner_uid 是发起 run 的用户 uid,service 层 request_cancel_agent_run
        按 current_uid=owner_uid 校验归属,非 owner 发起 /cancel 会因归属不符失败。
        """
        binding = await self.store.get_binding_record(msg.channel_name, msg.chat_id)
        if binding is None or not binding.active_run_id:
            await self._reply(msg, bus, "当前无运行中的任务")
            return

        owner_uid = binding.active_run_owner_uid or ""
        if not owner_uid:
            await self._reply(msg, bus, "⚠️ 无法确定运行发起者,取消失败")
            return

        try:
            async with self._session_factory() as db:
                await request_cancel_agent_run(
                    run_id=binding.active_run_id,
                    current_uid=owner_uid,
                    db=db,
                    cascade_children=False,
                )
            await self.store.clear_active_run(msg.channel_name, msg.chat_id)
            await self._reply(msg, bus, "已取消当前运行")
        except Exception:
            logger.exception("[cmd] cancel failed")
            await self._reply(msg, bus, "⚠️ 取消失败,你可能无权取消他人的运行")

    async def _reply(self, msg: InboundMessage, bus: MessageBus, text: str) -> None:
        await bus.publish_outbound(OutboundMessage(
            channel_name=msg.channel_name, chat_id=msg.chat_id,
            thread_id="", text=text, phase="final", is_final=True,
            thread_ts=msg.thread_ts,
        ))
