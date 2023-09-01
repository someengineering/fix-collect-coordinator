# fix-collect-coordinator
# Copyright (C) 2023  Some Engineering
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import asyncio
import os
from contextlib import suppress
from typing import Dict, Any, List

import pytest
from attr import define
from arq import create_pool
from arq.connections import RedisSettings, ArqRedis
from arq.worker import Worker, Function
from pytest import fixture


@define
class ExampleData:
    foo: int
    bar: str
    bla: List[int]


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
            "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
            "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
            "AWS_SESSION_TOKEN": os.environ["AWS_SESSION_TOKEN"],
        },
        account_len_hint=2,
    )
    job = await arq_redis.enqueue_job("collect", arg)
    print("Job enqueued. Waiting for result...")
    assert job is not None
    try:
        result = await job.result()
        print(result)
    except Exception as e:
        print(e)


@pytest.mark.asyncio
@pytest.mark.skipif(os.environ.get("REDIS_RUNNING") is None, reason="Redis is not running")
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
