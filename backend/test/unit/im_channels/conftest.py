"""IM 渠道模型测试共享 fixture。"""
from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest_asyncio
from sqlalchemy import event
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from yuxi.storage.postgres.models_business import Base


@pytest_asyncio.fixture()
async def async_db_session() -> AsyncGenerator:
    """提供 sqlite 内存数据库会话。

    sqlite 默认不启用外键约束,这里通过 connect 事件显式开启 PRAGMA foreign_keys=ON,
    以验证 IMChannelUser.api_key_id 的 ON DELETE SET NULL 行为。
    """
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
