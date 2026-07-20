"""IM 默认部门启动 ensure 测试。"""
from __future__ import annotations

import pytest
from sqlalchemy import select

from yuxi.im_channels.user_service import ensure_im_default_department
from yuxi.storage.postgres.models_business import Department

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_ensure_im_default_department_creates_if_absent(async_db_session, monkeypatch):
    """部门不存在时自动创建。"""
    monkeypatch.setenv("IM_DEFAULT_DEPARTMENT", "测试IM部门")
    dept_id = await ensure_im_default_department(async_db_session)
    assert dept_id is not None

    result = await async_db_session.execute(
        select(Department).where(Department.name == "测试IM部门")
    )
    dept = result.scalar_one_or_none()
    assert dept is not None
    assert dept.id == dept_id


@pytest.mark.asyncio
async def test_ensure_im_default_department_idempotent(async_db_session, monkeypatch):
    """已存在时不重复创建,返回同一 id。"""
    monkeypatch.setenv("IM_DEFAULT_DEPARTMENT", "测试IM部门")
    id1 = await ensure_im_default_department(async_db_session)
    id2 = await ensure_im_default_department(async_db_session)
    assert id1 == id2

    result = await async_db_session.execute(
        select(Department).where(Department.name == "测试IM部门")
    )
    departments = list(result.scalars().all())
    assert len(departments) == 1
