"""IM 渠道实现(钉钉/飞书等)。

每个 Channel 子类对接一个 IM 平台,继承 base.Channel,实现 start/stop/send/send_file。
ChannelService 按配置启用对应渠道,延迟 import 避免未启用渠道的依赖被加载。
"""
