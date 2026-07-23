"""MessageBus - 异步 pub/sub hub,解耦 channel 与 dispatcher。

Channel 发布 inbound,dispatcher 消费;dispatcher 发布 outbound,channel 订阅。
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class InboundMessageType(StrEnum):
    CHAT = "chat"
    COMMAND = "command"
    IMAGE = "image"


@dataclass
class InboundMessage:
    """IM 平台 -> dispatcher 的入站消息。"""
    channel_name: str
    chat_id: str
    chat_type: str               # 'p2p' / 'group'
    user_id: str
    user_name: str
    text: str
    msg_type: InboundMessageType = InboundMessageType.CHAT
    thread_ts: str | None = None
    image_content_url: str | None = None  # base64 data URL
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)


@dataclass
class ResolvedAttachment:
    """已解析到宿主文件系统的附件,准备上传到 IM 平台。"""
    virtual_path: str
    actual_path: Path
    filename: str
    mime_type: str
    size: int
    is_image: bool


@dataclass
class OutboundMessage:
    """dispatcher -> IM 平台的出站消息。"""
    channel_name: str
    chat_id: str
    thread_id: str
    text: str
    attachments: list[ResolvedAttachment] = field(default_factory=list)
    is_final: bool = True
    phase: str = "final"         # 'pending' / 'streaming' / 'final' / 'error'
    thread_ts: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)


OutboundCallback = Callable[[OutboundMessage], Coroutine[Any, Any, None]]


class MessageBus:
    def __init__(self) -> None:
        self._inbound_queue: asyncio.Queue[InboundMessage] = asyncio.Queue()
        self._outbound_listeners: list[OutboundCallback] = []

    async def publish_inbound(self, msg: InboundMessage) -> None:
        await self._inbound_queue.put(msg)
        logger.info("[Bus] inbound enqueued: channel=%s chat_id=%s type=%s qsize=%d",
                    msg.channel_name, msg.chat_id, msg.msg_type.value, self._inbound_queue.qsize())

    async def get_inbound(self) -> InboundMessage:
        return await self._inbound_queue.get()

    @property
    def inbound_queue(self) -> asyncio.Queue[InboundMessage]:
        return self._inbound_queue

    def subscribe_outbound(self, callback: OutboundCallback) -> None:
        self._outbound_listeners.append(callback)

    def unsubscribe_outbound(self, callback: OutboundCallback) -> None:
        self._outbound_listeners = [cb for cb in self._outbound_listeners if cb != callback]

    async def publish_outbound(self, msg: OutboundMessage) -> None:
        for callback in self._outbound_listeners:
            try:
                await callback(msg)
            except Exception:
                logger.exception("[Bus] outbound listener error: channel=%s", msg.channel_name)
