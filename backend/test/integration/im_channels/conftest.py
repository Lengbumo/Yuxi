"""IM 渠道集成测试共享 fixture。"""

from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete, event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from yuxi.storage.postgres.models_business import Base
from yuxi.utils.auth_utils import AuthUtils


@pytest_asyncio.fixture()
async def async_db_session() -> AsyncGenerator:
    """提供 sqlite 内存数据库会话,用于 IM 渠道启动逻辑的离线集成测试。"""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")

    @event.listens_for(engine.sync_engine, "connect")
    def _enable_fk(dbapi_conn, _):  # noqa: ANN001
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        yield session
    await engine.dispose()


@pytest_asyncio.fixture()
async def test_engine() -> AsyncGenerator:
    """sqlite 内存数据库引擎,启用外键约束,供 app_client 与 db_cleanup 共享。"""
    # 显式导入 IM 渠道模型,确保 IMChannelUser/IMChannelBinding 注册到 Base.metadata
    from yuxi.im_channels import models as _im_models  # noqa: F401

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")

    @event.listens_for(engine.sync_engine, "connect")
    def _enable_fk(dbapi_conn, _):  # noqa: ANN001
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture
def system_api_key(monkeypatch) -> str:
    """生成测试用 IM 系统级 API Key 并写入 env(供 router fallback 读取)。"""
    full_key, _, _ = AuthUtils.generate_api_key()
    monkeypatch.setenv("IM_SYSTEM_API_KEY", full_key)
    return full_key


@pytest_asyncio.fixture()
async def app_client(test_engine, system_api_key) -> AsyncGenerator[AsyncClient, None]:
    """启动 ASGI app(仅 im_router),用 sqlite 内存数据库,不走完整 lifespan。

    - override get_db 用 sqlite 会话
    - 手动设置 app.state.im_system_api_key,避免每次请求 fallback 读 env
    - 预先创建 IM 默认部门,模拟 lifespan 的 ensure_im_default_department
    """
    factory = async_sessionmaker(test_engine, expire_on_commit=False)

    # 预先创建 IM 默认部门
    from yuxi.storage.postgres.models_business import Department

    async with factory() as session:
        dept = Department(name="IM用户", description="IM 渠道自动创建用户默认部门")
        session.add(dept)
        await session.commit()

    async def get_test_db() -> AsyncGenerator[AsyncSession, None]:
        async with factory() as session:
            yield session

    from fastapi import FastAPI

    from server.routers.im_router import router as im_router
    from server.utils.auth_middleware import get_db

    app = FastAPI()
    app.include_router(im_router, prefix="/api")
    app.dependency_overrides[get_db] = get_test_db
    app.state.im_system_api_key = system_api_key

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


@pytest_asyncio.fixture()
async def db_cleanup(test_engine) -> AsyncGenerator[None, None]:
    """每个测试后清理 IM 渠道相关数据,避免测试间互相污染。

    保留 IM 默认部门,仅清理 IMChannelUser、IMChannelBinding、APIKey 与 IM 创建的 Yuxi 用户。
    同时清空 user_service 模块级速率限制计数,避免跨测试累积触发 429。
    需在测试签名中显式引用以触发 teardown。
    """
    yield
    from yuxi.im_channels.models import IMChannelBinding, IMChannelUser
    from yuxi.im_channels.user_service import _resolve_timestamps
    from yuxi.storage.postgres.models_business import APIKey, User

    factory = async_sessionmaker(test_engine, expire_on_commit=False)
    async with factory() as session:
        await session.execute(delete(IMChannelUser))
        await session.execute(delete(IMChannelBinding))
        await session.execute(delete(APIKey))
        # IM 用户 uid 形如 '{channel}:{im_user_id}',通过 LIKE 匹配冒号清理
        await session.execute(delete(User).where(User.uid.like("%:%")))
        await session.commit()
    # 清空速率限制计数,避免前序测试的请求影响后续测试
    _resolve_timestamps.clear()
