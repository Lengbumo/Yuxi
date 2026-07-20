"""ChannelService - im-worker 生命周期管理。

从 IMConfig 读取配置,启动所有 enabled 的 Channel 与 ChannelManager。
ChannelManager 消费 MessageBus 的 inbound,Channel 负责与 IM 平台收发。

im-worker 不通过 HTTP 调 api,ChannelManager 直接复用 api/worker 共享的 service
层函数(create_agent_invocation_run_view 等)提交 AgentRun,经 Redis 队列由 arq
worker 执行。ChannelService 持有 session_factory 供 manager/commands 开 session。
"""
from __future__ import annotations

import logging
from typing import Any

from yuxi.im_channels.config import IMConfig
from yuxi.im_channels.manager import ChannelManager
from yuxi.im_channels.message_bus import MessageBus
from yuxi.im_channels.store import ChannelStore

logger = logging.getLogger(__name__)


class ChannelService:
    """im-worker 进程内的顶层编排器。

    持有 MessageBus / ChannelStore / ChannelManager,并按 IMConfig 启动
    feishu/dingtalk channel(延迟 import,未启用不触发)。
    """

    def __init__(self, config: IMConfig, *, session_factory, resolve_fn) -> None:
        self.bus = MessageBus()
        self.store = ChannelStore(session_factory=session_factory, resolve_fn=resolve_fn)
        self.manager = ChannelManager(
            bus=self.bus,
            store=self.store,
            session_factory=session_factory,
            default_agent_slug=config.default_agent_slug,
            max_concurrency=config.max_concurrency,
            stream_throttle_seconds=config.stream_throttle_seconds,
            run_total_timeout_seconds=config.run_total_timeout_seconds,
            sse_reconnect_interval=config.sse_reconnect_interval,
            outputs_host_path_template=config.outputs_host_path_template,
        )
        self._channels: dict[str, Any] = {}
        self._config = config

    async def start(self) -> None:
        """启动 manager 与所有 enabled 的 channel。

        channel 子类(FeishuChannel/DingtalkChannel)延迟 import,
        未启用的渠道不会触发 import,避免未启用渠道的依赖影响启动。
        """
        await self.manager.start()
        if self._config.feishu.enabled:
            from yuxi.im_channels.channels.feishu import FeishuChannel

            ch = FeishuChannel(name="feishu", bus=self.bus, config=self._config.feishu)
            await ch.start()
            self.bus.subscribe_outbound(ch._on_outbound)
            self._channels["feishu"] = ch
        if self._config.dingtalk.enabled:
            from yuxi.im_channels.channels.dingtalk import DingtalkChannel

            ch = DingtalkChannel(name="dingtalk", bus=self.bus, config=self._config.dingtalk)
            await ch.start()
            self.bus.subscribe_outbound(ch._on_outbound)
            self._channels["dingtalk"] = ch
        logger.info("[ChannelService] started: %s", list(self._channels.keys()))

    async def stop(self) -> None:
        """停止所有 channel + manager。

        channel stop 失败不阻塞其他 channel 清理,异常记录后继续。
        """
        for ch in self._channels.values():
            try:
                self.bus.unsubscribe_outbound(ch._on_outbound)
                await ch.stop()
            except Exception:
                logger.exception("[ChannelService] stop channel failed")
        self._channels.clear()
        await self.manager.stop()

    def get_status(self) -> dict[str, Any]:
        """返回当前运行状态,供健康检查/调试使用。"""
        return {
            "running": bool(self._channels),
            "channels": {name: ch.is_running for name, ch in self._channels.items()},
        }
