"""ChannelManager - 消费 inbound,调 service 层提交 AgentRun,发布 outbound。

核心职责:
1. 从 MessageBus 消费 InboundMessage
2. 解析 IM 用户 + 会话绑定(get_binding / insert_binding)
3. 立即推送 pending,调 create_agent_invocation_run_view 提交 run(经 Redis 队列由 arq worker 执行),
   stream_agent_run_events 消费 SSE 推送 streaming,终态 load_agent_run_result 推送 final
4. 错误矩阵:UserResolveError/HTTPException/RunTimeoutError -> error phase
5. chat_id 串行锁避免 thread busy

"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from pathlib import Path

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from yuxi.im_channels.artifacts import resolve_artifacts
from yuxi.im_channels.message_bus import InboundMessage, InboundMessageType, MessageBus, OutboundMessage
from yuxi.services.agent_invocation_service import create_agent_invocation_run_view
from yuxi.services.agent_run_service import load_agent_run_result, stream_agent_run_events
from yuxi.services.input_message_service import AgentRunInputMessage, build_chat_input_message
from yuxi.storage.postgres.models_business import User

logger = logging.getLogger(__name__)

# streaming 阶段推送节流间隔(秒),避免高频刷屏 IM 平台
DEFAULT_THROTTLE = 0.5
# run 总超时(秒),SSE 中断后 load_agent_run_result 轮询的上限
DEFAULT_RUN_TOTAL_TIMEOUT = 600
# load_agent_run_result 轮询间隔(秒)
DEFAULT_SSE_RECONNECT_INTERVAL = 5
# run 终态集合(load_agent_run_result 返回的 status)
TERMINAL_STATUSES = ("succeeded", "failed", "cancelled", "error", "completed", "interrupted")


class ChannelManager:
    """消息分发器:消费 inbound -> 调 service 层提交 run -> 发布 outbound。

    并发模型:
    - semaphore 限制全局并发(max_concurrency)
    - chat_lock 保证同一 chat_id 串行(避免 thread busy 409)
    """

    def __init__(
        self,
        *,
        bus: MessageBus,
        store,
        session_factory: async_sessionmaker[AsyncSession],
        default_agent_slug: str,
        max_concurrency: int = 5,
        stream_throttle_seconds: float = DEFAULT_THROTTLE,
        run_total_timeout_seconds: int = DEFAULT_RUN_TOTAL_TIMEOUT,
        sse_reconnect_interval: int = DEFAULT_SSE_RECONNECT_INTERVAL,
        outputs_host_path_template: str = "/app/saves/threads/{thread_id}/user-data/outputs",
    ) -> None:
        self.bus = bus
        self.store = store
        self._session_factory = session_factory
        self._default_agent_slug = default_agent_slug
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._chat_locks: dict[str, asyncio.Lock] = {}
        self._chat_locks_guard = asyncio.Lock()
        self._stream_throttle = stream_throttle_seconds
        self._run_total_timeout = run_total_timeout_seconds
        self._sse_reconnect_interval = sse_reconnect_interval
        self._outputs_host_path_template = outputs_host_path_template
        self._running = False
        self._task: asyncio.Task | None = None

    async def _get_chat_lock(self, chat_key: str) -> asyncio.Lock:
        """获取 chat_id 级别的串行锁,同一 chat_key 共享一把锁。

        用 guard lock 保护 _chat_locks dict 的懒初始化,避免并发首消息重复创建。
        """
        async with self._chat_locks_guard:
            if chat_key not in self._chat_locks:
                self._chat_locks[chat_key] = asyncio.Lock()
            return self._chat_locks[chat_key]

    async def start(self) -> None:
        """启动 dispatch loop,后台消费 inbound 队列。"""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._dispatch_loop())
        logger.info("[Manager] started")

    async def stop(self) -> None:
        """停止 dispatch loop,取消未完成任务。"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _dispatch_loop(self) -> None:
        """后台循环:从 bus 取 inbound,为每条消息创建独立 task。"""
        while self._running:
            try:
                msg = await asyncio.wait_for(self.bus.get_inbound(), timeout=1.0)
            except TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            task = asyncio.create_task(self._handle_message(msg))
            task.add_done_callback(self._log_task_error)

    @staticmethod
    def _log_task_error(task: asyncio.Task) -> None:
        """任务异常回调,避免静默吞错。"""
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            logger.error("[Manager] unhandled error: %s", exc, exc_info=exc)

    async def _handle_message(self, msg: InboundMessage) -> None:
        """单条消息处理入口:semaphore + chat_lock 双重并发控制。"""
        async with self._semaphore:
            chat_lock = await self._get_chat_lock(f"{msg.channel_name}:{msg.chat_id}")
            async with chat_lock:
                try:
                    if msg.msg_type == InboundMessageType.COMMAND:
                        await self._handle_command(msg)
                    else:
                        await self._handle_chat(msg)
                except Exception:
                    logger.exception("[Manager] handle_message error: channel=%s chat=%s",
                                     msg.channel_name, msg.chat_id)
                    await self._publish_error(msg, "⚠️ 服务暂时不可用,请稍后重试")

    async def _handle_chat(self, msg: InboundMessage) -> None:
        """CHAT/IMAGE 路径:解析身份 -> 查 binding -> pending -> submit run -> 回填 binding -> stream -> final。

        路径 A:thread 由 create_agent_invocation_run_view 在 run 创建时一并建(conversation + run 同步),
        store 只负责记录 binding(thread_id 从 run_response 回填)。

        新用户首次发言不自动创建 Yuxi 用户,先引导输入账号与已有用户匹配;
        匹配不到则自动创建。
        """
        # 1. 解析 IM 用户
        try:
            yuxi_uid, _ = await self.store.get_or_create_user(
                msg.channel_name, msg.user_id, msg.user_name,
            )
        except Exception:
            logger.exception("[Manager] resolve user failed")
            await self._publish_error(msg, "⚠️ 无法验证身份,请稍后重试或联系管理员")
            return

        # 2. 新用户引导:首次发言无 yuxi_uid,引导输入账号匹配
        if not yuxi_uid:
            if self.store.is_pending(msg.channel_name, msg.user_id):
                await self._handle_account_linking(msg)
                return
            self.store.mark_pending(msg.channel_name, msg.user_id)
            await self._publish(
                msg, phase="pending", thread_id="",
                text="请提供你的账号名,以便关联你的 Yuxi 账户。如果没有账号,直接回复任意内容即可自动创建。",
            )
            return

        # 3. 查 existing binding:命中复用 thread_id,未命中占位空串(service 自动生成)
        existing = await self.store.get_binding(msg.channel_name, msg.chat_id)
        if existing is not None:
            thread_id, agent_slug = existing
        else:
            thread_id, agent_slug = "", self._default_agent_slug

        # 4. 推送 pending,告知用户已收到
        await self._publish(msg, phase="pending", text="正在思考...", thread_id=thread_id)

        # 5. 加载 User + 调 service 层提交 run
        try:
            user = await self._load_user(yuxi_uid)
            if user is None:
                await self._publish_error(msg, "⚠️ 用户不存在,请联系管理员", thread_id=thread_id)
                return
            logger.info("[Manager] submit run: agent_slug=%s, user_uid=%s, user_id=%s, thread_id=%s",
                        agent_slug, user.uid, user.id, thread_id)
            async with self._session_factory() as db:
                run_resp = await create_agent_invocation_run_view(
                    agent_slug=agent_slug,
                    input_message=_build_im_input_message(msg.text, msg.image_content_url),
                    invocation_metadata={
                        "source": "im_channel",
                        "im_channel": msg.channel_name,
                        "chat_id": msg.chat_id,
                    },
                    requested_thread_id=thread_id,
                    request_id=str(uuid.uuid4()),
                    model_spec=None,
                    current_user=user,
                    db=db,
                    conversation_title=f"{msg.channel_name} 对话",
                )
        except HTTPException as exc:
            await self._publish_error(msg, self._http_error_text(exc), thread_id=thread_id)
            return

        run_id = run_resp["run_id"]
        real_thread_id = run_resp["thread_id"]

        # 5. 未命中 binding 时回填(用 run_response 返回的真实 thread_id)
        if existing is None:
            await self.store.insert_binding(
                msg.channel_name, msg.chat_id, msg.chat_type, yuxi_uid,
                real_thread_id, agent_slug,
            )

        await self.store.set_active_run(msg.channel_name, msg.chat_id, run_id, owner_uid=yuxi_uid)

        # 6. SSE 流式 + 终态;finally 确保清理 active_run
        try:
            final_text, artifacts = await self._stream_and_collect(msg, yuxi_uid, run_id, real_thread_id)
        finally:
            await self.store.clear_active_run(msg.channel_name, msg.chat_id)

        # 7. 推送 final(挂载附件)
        await self._publish_final(msg, final_text, artifacts, real_thread_id)

    async def _handle_account_linking(self, msg: InboundMessage) -> None:
        """处理待匹配用户的账号回复:用消息文本匹配已有 Yuxi 用户,命中则绑定,未命中自动创建。"""
        account = msg.text.strip()
        if not account:
            await self._publish(msg, phase="pending", text="请提供你的账号名。", thread_id="")
            return

        # 尝试匹配已有 Yuxi 用户
        matched = None
        try:
            async with self._session_factory() as session:
                from yuxi.im_channels import user_service as us

                matched = await us.match_user_by_account(session, account)
        except Exception:
            logger.exception("[Manager] match user failed")

        try:
            if matched is not None:
                # 命中:关联到已有用户
                api_key = await self.store.link_to_existing_user(
                    msg.channel_name, msg.user_id, msg.user_name, matched.uid,
                )
                yuxi_uid = matched.uid
                text = f"已关联账号 {matched.username},可以开始对话了"
            else:
                # 未命中:自动创建新用户
                yuxi_uid, api_key = await self.store.create_user(
                    msg.channel_name, msg.user_id, msg.user_name,
                )
                text = "已创建新账号,可以开始对话了"
        except Exception:
            logger.exception("[Manager] account linking failed")
            await self._publish_error(msg, "⚠️ 账号匹配失败,请稍后重试或联系管理员")
            return

        self.store.clear_pending(msg.channel_name, msg.user_id)
        await self._publish(msg, phase="final", text=text, thread_id="")

    async def _load_user(self, uid: str) -> User | None:
        """按 uid 加载 Yuxi User 对象(service 层 create_agent_invocation_run_view 需要)。"""
        async with self._session_factory() as session:
            result = await session.execute(select(User).where(User.uid == uid))
            return result.scalar_one_or_none()

    async def _stream_and_collect(
        self, msg: InboundMessage, yuxi_uid: str, run_id: str, thread_id: str,
    ) -> tuple[str, list]:
        """订阅 stream_agent_run_events,节流推送 streaming;SSE 中断切 load_agent_run_result 轮询到总超时。

        SSE 事件中 message_delta 携带增量 content,这里累积成全量后推送。
        SSE 断开或异常时,不放弃,切到 load_agent_run_result 轮询直到拿到终态或总超时。
        返回 (final_text, artifacts)。
        """
        accumulated_text = ""
        last_published = ""
        last_publish_at = 0.0
        deadline = time.monotonic() + self._run_total_timeout

        # 阶段一:SSE 流式(stream_agent_run_events yield SSE 文本行)
        try:
            async for sse_line in stream_agent_run_events(
                run_id=run_id, after_seq="0-0", current_uid=yuxi_uid, verbose=False,
            ):
                event = _parse_sse_line(sse_line)
                if event is None:
                    continue
                delta = self._extract_text(event)
                if delta:
                    accumulated_text += delta
                    # 节流:首次立即推,后续按 throttle 间隔推
                    now = time.monotonic()
                    if accumulated_text != last_published:
                        if last_published and now - last_publish_at < self._stream_throttle:
                            continue
                        await self._publish(msg, phase="streaming", text=accumulated_text, thread_id=thread_id)
                        last_published = accumulated_text
                        last_publish_at = now

                if event.get("event") == "end":
                    break
        except Exception:
            logger.exception("[Manager] SSE interrupted, fallback to load_agent_run_result polling")

        # 阶段二:load_agent_run_result 轮询兜底(直到总超时)
        while time.monotonic() < deadline:
            try:
                result = await load_agent_run_result(run_id=run_id, current_uid=yuxi_uid)
                if str(result.get("status") or "") in TERMINAL_STATUSES:
                    final_output = result.get("output") or accumulated_text
                    return final_output, _extract_artifacts(result)
            except Exception:
                logger.exception("[Manager] load_agent_run_result polling failed")
            await asyncio.sleep(self._sse_reconnect_interval)

        # 超时:推 error 并抛异常,触发上层 finally 清理
        await self._publish_error(msg, "⚠️ 处理超时,请用 /cancel 取消或稍后重试", thread_id=thread_id)
        raise TimeoutError("run total timeout")

    def _extract_text(self, event: dict) -> str:
        """从 SSE event 提取增量文本(message_delta.content)。

        SSE 事件结构(yield 的 dict):
        - event: 事件类型(messages/end/error/metadata/custom...)
        - data:  envelope,含 payload.items 或 payload.chunk

        messages 事件的 payload.items/chunk 中,stream_event.type == "message_delta"
        时携带 content 字段(增量文本)。reasoning_content 是推理过程,不推给 IM 用户。
        """
        if event.get("event") != "messages":
            return ""
        data = event.get("data") or {}
        payload = data.get("payload") or {}

        chunks: list[dict] = []
        if isinstance(payload.get("chunk"), dict):
            chunks.append(payload["chunk"])
        if isinstance(payload.get("items"), list):
            chunks.extend(c for c in payload["items"] if isinstance(c, dict))

        parts: list[str] = []
        for chunk in chunks:
            stream_event = chunk.get("stream_event") or {}
            if stream_event.get("type") == "message_delta":
                content = stream_event.get("content")
                if isinstance(content, str) and content:
                    parts.append(content)
        return "".join(parts)

    async def _publish_final(self, msg: InboundMessage, text: str, artifacts: list, thread_id: str) -> None:
        """推送 final 消息,解析 artifact 路径挂载附件。"""
        outputs_dir = Path(self._outputs_host_path_template.format(thread_id=thread_id))
        attachments = resolve_artifacts(artifacts or [], outputs_dir=outputs_dir)

        await self.bus.publish_outbound(OutboundMessage(
            channel_name=msg.channel_name, chat_id=msg.chat_id, thread_id=thread_id,
            text=text or "(无回复)", phase="final", is_final=True,
            thread_ts=msg.thread_ts, attachments=attachments,
        ))

    async def _publish(self, msg: InboundMessage, *, phase: str, text: str, thread_id: str) -> None:
        """推送中间状态(pending/streaming)消息。"""
        await self.bus.publish_outbound(OutboundMessage(
            channel_name=msg.channel_name, chat_id=msg.chat_id, thread_id=thread_id,
            text=text, phase=phase, is_final=(phase == "final"), thread_ts=msg.thread_ts,
        ))

    async def _publish_error(self, msg: InboundMessage, text: str, *, thread_id: str = "") -> None:
        """推送 error 消息(终态)。thread_id 可空(身份解析失败时还没 thread_id)。"""
        await self.bus.publish_outbound(OutboundMessage(
            channel_name=msg.channel_name, chat_id=msg.chat_id, thread_id=thread_id,
            text=text, phase="error", is_final=True, thread_ts=msg.thread_ts,
        ))

    @staticmethod
    def _http_error_text(exc: HTTPException) -> str:
        """按 HTTPException.status_code 返回中文提示,文案与 spec 7.1 错误矩阵一致。

        service 层(create_agent_invocation_run_view 等)抛 HTTPException,
        status_code 与原 AgentCallError 矩阵对齐:404 agent 不存在、403 无权访问、
        409 thread busy、429 限流、5xx 服务不可用。
        """
        status = exc.status_code
        if status == 404:
            return "⚠️ Agent 不可用,请用 /agent list 查看可切换的 Agent"
        if status == 403:
            return "⚠️ 你无权访问该 Agent,请用 /agent list 查看"
        if status == 409:
            return "⚠️ 当前会话正在处理上一条消息,请稍后"
        if status == 429:
            return "⚠️ 系统繁忙,请稍后重试"
        return "⚠️ 服务暂时不可用,请稍后重试"

    async def _handle_command(self, msg: InboundMessage) -> None:
        """命令路由,延迟 import CommandParser 避免 CHAT 路径未用到时也加载命令模块。"""
        from yuxi.im_channels.commands import CommandParser

        parser = CommandParser(
            store=self.store,
            session_factory=self._session_factory,
            default_agent_slug=self._default_agent_slug,
        )
        await parser.handle(msg, bus=self.bus)


# ---------- 模块级辅助函数 ----------

def _parse_sse_line(line: str) -> dict | None:
    """解析 SSE 文本行为 {event, data} dict。

    stream_agent_run_events yield 的格式:
        event: messages\\n
        data: {...json...}\\n
        \\n
    一个 SSE 事件跨多行,以空行分隔。本函数处理单行:遇 event:/data: 前缀提取,
    data 行 json.loads。调用方按空行聚合(本简化版逐行处理:data 行即触发解析,
    event 行作为最近 event 上下文)。

    返回 None 表示非 event/data 行(如心跳注释行)。
    """
    line = line.strip()
    if not line or line.startswith(":"):
        return None
    if line.startswith("event:"):
        return {"event": line[len("event:"):].strip(), "data": {}}
    if line.startswith("data:"):
        payload = line[len("data:"):].strip()
        try:
            data = json.loads(payload) if payload else {}
        except (json.JSONDecodeError, ValueError):
            return None
        return {"event": "messages", "data": data}
    return None


def _extract_artifacts(result: dict) -> list:
    """从 load_agent_run_result 返回的 result 提取 artifacts 列表。

    load_agent_run_result 返回键名是 output(非 final_output),artifacts 暂从
    output_message 的 extra_metadata 取;首版若无则返回空列表,final 消息仅含文本。
    """
    # result 结构见 agent_run_service.get_agent_run_result,当前未直接暴露 artifacts
    # 后续可从 output_message.extra_metadata.artifacts 取,首版返回空
    return []


def _build_im_input_message(text: str, image_content_url: str | None) -> AgentRunInputMessage:
    """把 IM 入站 text + image_content_url(data URL)构造成 AgentRunInputMessage。

    build_chat_input_message 的 image_content 期望纯 base64(不含 data: 前缀),
    这里剥掉 'data:image/...;base64,' 前缀后传入。无图片时只传文本。
    """
    if not image_content_url:
        return build_chat_input_message(text)
    # 剥 data:image/png;base64, 前缀
    prefix_marker = "base64,"
    idx = image_content_url.find(prefix_marker)
    base64_content = image_content_url[idx + len(prefix_marker):] if idx >= 0 else image_content_url
    return build_chat_input_message(text, image_content=base64_content)
