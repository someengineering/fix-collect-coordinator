import asyncio
from contextlib import suppress
from typing import Dict, Any

import pytest
from arq import create_pool
from arq.connections import RedisSettings, ArqRedis
from arq.worker import Worker, Function
from pytest import fixture

from redis_stream_test import ExampleData


@fixture
async def arq_redis() -> ArqRedis:
    return await create_pool(RedisSettings(host="localhost", port=6379, database=5))


@pytest.mark.skip(reason="only for ingesting test data")
async def test_enqueue_job(arq_redis: ArqRedis) -> None:
    arg = dict(
        tenant_id="test",
        graphdb_server="http://db-0.dbs.fix.svc.cluster.local:8529",
        graphdb_database="db3",
        graphdb_username="resoto",
        graphdb_password="",
        worker_config={"resotoworker": {"collector": ["aws"]}},
        env={
            "AWS_ACCESS_KEY_ID": "",
            "AWS_SECRET_ACCESS_KEY": "",
            "AWS_SESSION_TOKEN": "",
        },
        account_len_hint=2,
    )
    job = await arq_redis.enqueue_job("collect", arg)
    assert job is not None
    try:
        result = await job.result()
        print(result)
    except Exception as e:
        print(e)


@pytest.mark.asyncio
async def test_queue(arq_redis: ArqRedis) -> None:
    # This is the worker function that is called by arq.
    async def the_task(ctx: Dict[str, Any], *args: Any, **kwargs: Any) -> str:
        url, num, data = args
        assert isinstance(url, str)
        assert isinstance(num, int)
        assert isinstance(data, ExampleData)
        await asyncio.sleep(1)
        return "yes"

    # Enqueue 3 jobs.
    jobs = [
        await arq_redis.enqueue_job("the_task", url, 123, ExampleData(1, "foo", [1, 2, 3]))
        for url in ("https://facebook.com", "https://microsoft.com", "https://github.com")
    ]

    # Create a worker and process the jobs.
    worker = Worker(
        functions=[Function("the_task", the_task, None, None, None, None)],
        redis_pool=arq_redis,
        max_jobs=10,
        poll_delay=0.1,
        keep_result=3,
    )
    task = asyncio.create_task(worker.async_run())

    # Wait for the jobs to finish.
    for job in jobs:
        assert job is not None
        result = await job.result()
        assert result == "yes"

    # Stop the worker.
    await worker.close()
    with suppress(Exception):
        await task
