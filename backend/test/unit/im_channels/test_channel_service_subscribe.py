"""ChannelService 出站订阅挂载测试。

验证 service.start() 会把每个 channel 的 _on_outbound 注册到 MessageBus,
service.stop() 会注销。这是钉钉+飞书共用的出站消息投递关键链路:
若不订阅,manager 的 publish_outbound 无监听者,出站消息全部静默丢失。
"""
from __future__ import annotations

import sys
import types

import pytest

from yuxi.im_channels.base import Channel
from yuxi.im_channels.config import FeishuConfig, IMConfig
from yuxi.im_channels.message_bus import OutboundMessage
from yuxi.im_channels.service import ChannelService


class _RecordingChannel(Channel):
    """记录 send 调用的假 channel,用于验证订阅投递。"""

    def __init__(self, name, bus, config):
        super().__init__(name, bus, config)
        self.sent: list[OutboundMessage] = []

    async def start(self) -> None:
        self._running = True

    async def stop(self) -> None:
        self._running = False

    async def send(self, msg: OutboundMessage) -> None:
        self.sent.append(msg)


def _install_fake_feishu_module(monkeypatch) -> type:
    """向 sys.modules 注入假的 yuxi.im_channels.channels.feishu 模块,
    其 FeishuChannel 指向 _RecordingChannel,避免 service.start() 触发真实 lark import。
    返回 _RecordingChannel 供测试断言使用。
    """
    fake_module = types.ModuleType("yuxi.im_channels.channels.feishu")
    fake_module.FeishuChannel = _RecordingChannel
    monkeypatch.setitem(sys.modules, "yuxi.im_channels.channels.feishu", fake_module)
    return _RecordingChannel


def _make_service(*, feishu_enabled: bool) -> ChannelService:
    config = IMConfig(
        enabled=True,
        feishu=FeishuConfig(
            enabled=feishu_enabled,
            app_id="cli_fake" if feishu_enabled else "",
            app_secret="secret_fake" if feishu_enabled else "",
        ),
    )
    return ChannelService(
        config=config,
        session_factory=None,
        resolve_fn=lambda *_a, **_kw: ("", ""),
    )


@pytest.mark.asyncio
async def test_start_subscribes_channel_on_outbound(monkeypatch):
    """service.start() 后,channel._on_outbound 必须在 bus 监听者列表里。"""
    _install_fake_feishu_module(monkeypatch)
    service = _make_service(feishu_enabled=True)

    await service.start()
    try:
        ch = service._channels["feishu"]
        assert ch._on_outbound in service.bus._outbound_listeners
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_outbound_message_actually_reaches_channel(monkeypatch):
    """端到端行为验证:publish_outbound 后 channel.send 真的收到消息。

    这是订阅链路的核心价值——不是验证 mock 被调用,而是验证消息真的投递到了 channel。
    """
    _install_fake_feishu_module(monkeypatch)
    service = _make_service(feishu_enabled=True)

    await service.start()
    try:
        ch = service._channels["feishu"]
        await service.bus.publish_outbound(OutboundMessage(
            channel_name="feishu", chat_id="c1", thread_id="t1", text="hello",
        ))
        assert len(ch.sent) == 1
        assert ch.sent[0].text == "hello"
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_stop_unsubscribes_channel_on_outbound(monkeypatch):
    """service.stop() 后,_on_outbound 必须从监听者列表移除,避免泄漏。"""
    _install_fake_feishu_module(monkeypatch)
    service = _make_service(feishu_enabled=True)

    await service.start()
    ch = service._channels["feishu"]
    assert ch._on_outbound in service.bus._outbound_listeners

    await service.stop()
    assert ch._on_outbound not in service.bus._outbound_listeners


@pytest.mark.asyncio
async def test_other_channel_outbound_ignored(monkeypatch):
    """订阅挂载后,发给其他 channel 的消息不应被本 channel 处理(过滤逻辑在 base._on_outbound)。"""
    _install_fake_feishu_module(monkeypatch)
    service = _make_service(feishu_enabled=True)

    await service.start()
    try:
        ch = service._channels["feishu"]
        await service.bus.publish_outbound(OutboundMessage(
            channel_name="dingtalk", chat_id="c1", thread_id="t1", text="hi",
        ))
        assert ch.sent == []
    finally:
        await service.stop()
