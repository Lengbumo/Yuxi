"""im-worker 入口。

启动 ChannelService,运行到收到 SIGTERM/SIGINT。
不注册 FastAPI 路由,纯后台进程。

启动流程:
1. 加载 IMConfig,若 IM_WORKER_ENABLED=false 直接退出
2. 初始化 pg_manager,取 async_sessionmaker 作为 session_factory
3. 构造 ChannelService(resolve_fn 闭包调 user_service.resolve_im_user)
4. 注册 SIGTERM/SIGINT handler,await stop_event
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys

# Windows 适配:与 server/worker_main.py 一致,用 selector event loop policy
# (asyncpg/lark ws 在默认 Proactor loop 下可能有问题),并把根目录加入 sys.path,
# 支持本地 `python -m server.im_worker_main` 跑法。
if sys.platform == "win32":
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from yuxi.im_channels import user_service
from yuxi.im_channels.config import load_im_config
from yuxi.im_channels.service import ChannelService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    config = load_im_config()
    if not config.enabled:
        logger.info("[im-worker] IM_WORKER_ENABLED=false, exit")
        return

    # 调用后返回新 AsyncSession,与 ChannelStore._session() 期望一致
    from yuxi.storage.postgres.manager import pg_manager

    pg_manager.initialize()
    session_factory = pg_manager.AsyncSession

    # resolve_fn:首次解析 IM 用户时调 user_service.resolve_im_user 直接建 Yuxi 用户 + API Key,
    async def resolve_fn(channel: str, im_user_id: str, im_user_name: str) -> tuple[str, str]:
        async with session_factory() as session:
            return await user_service.resolve_im_user(
                session,
                im_channel=channel,
                im_user_id=im_user_id,
                im_user_name=im_user_name,
                rate_limit=config.user_resolve_rate_limit,
            )

    service = ChannelService(config=config, session_factory=session_factory, resolve_fn=resolve_fn)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    # Linux/Mac 用 loop.add_signal_handler 优雅退出;Windows 不支持该方法
    # (NotImplementedError,SIGTERM 信号概念也不存在),退化用 signal.signal
    # 注册 SIGINT(Ctrl+C)兜底,回调里 thread-safe 设置 stop_event。
    if sys.platform == "win32":
        def _win_stop(*_args):
            loop.call_soon_threadsafe(stop_event.set)
        signal.signal(signal.SIGINT, _win_stop)
    else:
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop_event.set)

    await service.start()
    try:
        await stop_event.wait()
    finally:
        await service.stop()
        logger.info("[im-worker] stopped")


if __name__ == "__main__":
    asyncio.run(main())
