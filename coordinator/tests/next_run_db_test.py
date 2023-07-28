from contextlib import suppress
from datetime import timedelta, datetime
from typing import AsyncIterator, List

import pytest
from pytest import approx

from collect_coordinator.db.database import DbEngine, EntityDb, DbEntity
from collect_coordinator.db.nextrundb import NextRun, running_for_too_long, runs_to_start
from collect_coordinator.util import utc


@pytest.fixture
async def db_engine() -> AsyncIterator[DbEngine]:
    async with DbEngine("mysql+aiomysql://root:@127.0.0.1:3306/test") as engine:
        yield engine


@pytest.fixture
async def next_runs() -> List[NextRun]:
    now = utc()
    now_plus_2h = now + timedelta(hours=2)
    now_minus_2h = now - timedelta(hours=2)
    now_minus_4h = now - timedelta(hours=4)
    return [
        NextRun(tenant_id="t1", name="t1", next_run=now_plus_2h),
        NextRun(tenant_id="t2", name="t2", next_run=now),
        NextRun(tenant_id="t3", name="t3", next_run=now_minus_2h, in_progress=True, started_at=now),
        NextRun(tenant_id="t4", name="t4", next_run=now_minus_4h, in_progress=True, started_at=now_minus_2h),
    ]


@pytest.fixture
async def next_db(db_engine: DbEngine, next_runs: List[NextRun]) -> EntityDb[str, NextRun]:
    db = db_engine.db(str, NextRun)
    async with db.engine.connect() as conn:
        await conn.run_sync(DbEntity.metadata.drop_all)
        await conn.run_sync(DbEntity.metadata.create_all)
    async with db.session() as session:
        for next_run in next_runs:
            session.add(next_run)
    return db


@pytest.mark.asyncio
async def test_next_db(next_db: EntityDb[str, NextRun]) -> None:
    async with next_db.session() as session:
        # all: 4 elements
        assert len([s async for s in session.all()]) == 4
        # running for longer than 30 minutes: 1
        assert len([a async for a in session.query(running_for_too_long(timedelta(minutes=30)))]) == 1
        # running for longer than 3 hours: 0
        assert len([a async for a in session.query(running_for_too_long(timedelta(hours=3)))]) == 0
        # runs to start: 1
        assert len([a async for a in session.query(runs_to_start())]) == 1


@pytest.mark.asyncio
async def test_update_entries(next_db: EntityDb[str, NextRun]) -> None:
    in_1_min = datetime.now() + timedelta(minutes=1)
    # updating properties of loaded elements, will be persisted
    async with next_db.session() as session:
        async for next_run in session.all():
            next_run.in_progress = True
            next_run.started_at = in_1_min

    # check that the changes are persisted
    async with next_db.session() as session:
        async for next_run in session.all():
            assert next_run.in_progress is True
            assert next_run.started_at.timestamp() == approx(in_1_min.timestamp(), abs=1)


@pytest.mark.asyncio
async def test_rollback_on_exception(next_db: EntityDb[str, NextRun]) -> None:
    in_1_min = datetime.now() + timedelta(minutes=1)
    # on exception no change is persisted
    with suppress(Exception):
        async with next_db.session() as session:
            async for next_run in session.all():
                next_run.started_at = in_1_min
            raise Exception("boom")

    # check that the changes are persisted
    async with next_db.session() as session:
        async for next_run in session.all():
            assert next_run.started_at is None or next_run.started_at.timestamp() != approx(in_1_min.timestamp(), abs=1)
