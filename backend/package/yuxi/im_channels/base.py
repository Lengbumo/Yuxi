"""Channel 抽象基类。

每个 Channel 实现:
1. start/stop 生命周期(连接/断开 IM 平台)
2. send 发送文本消息(根据 OutboundMessage.phase 决定新建/更新/回复)
3. send_file 上传文件附件(可选,默认不支持)

_on_outbound 是注册到 MessageBus 的回调,只转发目标 channel 匹配的消息。
文本发送失败时不尝试上传文件,避免部分投递。
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

from yuxi.im_channels.message_bus import (
    InboundMessage, InboundMessageType, MessageBus, OutboundMessage, ResolvedAttachment,
)

logger = logging.getLogger(__name__)


class Channel(ABC):
    def __init__(self, name: str, bus: MessageBus, config: dict[str, Any]) -> None:
        self.name = name
        self.bus = bus
        self.config = config
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running

    @abstractmethod
    async def start(self) -> None: ...

    @abstractmethod
    async def stop(self) -> None: ...

    @abstractmethod
    async def send(self, msg: OutboundMessage) -> None: ...

    async def send_file(self, msg: OutboundMessage, attachment: ResolvedAttachment) -> bool:
        """上传单个文件附件,返回是否成功。默认不支持,子类覆盖。"""
        return False

    def _make_inbound(
        self,
        chat_id: str,
        user_id: str,
        text: str,
        *,
        chat_type: str,
        user_name: str = "",
        msg_type: InboundMessageType = InboundMessageType.CHAT,
        thread_ts: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> InboundMessage:
        return InboundMessage(
            channel_name=self.name,
            chat_id=chat_id,
            chat_type=chat_type,
            user_id=user_id,
            user_name=user_name,
            text=text,
            msg_type=msg_type,
            thread_ts=thread_ts,
            metadata=metadata or {},
        )

    async def _on_outbound(self, msg: OutboundMessage) -> None:
        """MessageBus 出站回调,只转发目标 channel 匹配的消息。

        文本发送失败时不尝试上传文件,避免部分投递。
        """
        if msg.channel_name != self.name:
            return
        try:
            await self.send(msg)
        except Exception:
            logger.exception("[%s] send failed, skipping file uploads", self.name)
            return

        for attachment in msg.attachments:
            try:
                success = await self.send_file(msg, attachment)
                if not success:
                    logger.warning("[%s] file upload skipped: %s", self.name, attachment.filename)
            except Exception:
                logger.exception("[%s] file upload failed: %s", self.name, attachment.filename)
