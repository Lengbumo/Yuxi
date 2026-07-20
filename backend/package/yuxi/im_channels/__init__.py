"""IM 渠道接入(钉钉/飞书)。

提供 IM 平台长连接、消息分发、Yuxi AgentCall HTTP 调用与用户/会话绑定。
独立 im-worker 进程通过本包对接 IM 平台,api-dev 通过本包的 models 与 router
提供用户解析与管理接口。
"""
