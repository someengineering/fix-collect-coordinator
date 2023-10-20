#  Copyright (c) 2023. Some Engineering
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Affero General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Affero General Public License for more details.
#
#  You should have received a copy of the GNU Affero General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Affero General Public License for more details.
#
#  You should have received a copy of the GNU Affero General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.
from asyncio import Future
from typing import Dict, Tuple, AsyncIterator, Any

from arq import create_pool
from arq.connections import RedisSettings, ArqRedis
from pytest import fixture

from collect_coordinator.job_coordinator import JobCoordinator, JobDefinition
from collect_coordinator.worker_queue import WorkerQueue


class LazyJobCoordinator(JobCoordinator):
    def __init__(self) -> None:
        self.running_jobs: Dict[str, Tuple[JobDefinition, Future[bool]]] = {}

    @property
    def max_parallel(self) -> int:
        return 100

    async def start_job(self, definition: JobDefinition) -> Future[bool]:
        result: Future[bool] = Future()
        self.running_jobs[definition.id] = (definition, result)
        return result

    async def mark_all_jobs_done(self) -> None:
        for job_id, (definition, result) in self.running_jobs.items():
            result.set_result(True)


@fixture
async def arq_redis() -> AsyncIterator[ArqRedis]:
    pool = await create_pool(RedisSettings(host="localhost", port=6379, database=5))
    await pool.flushdb()
    yield pool


@fixture
def credentials() -> Dict[str, Any]:
    return dict(
        aws=dict(
            aws_access_key_id="some_access",
            aws_secret_access_key="some_secret",
        ),
        redis_password="test",
        graph_db_root_password="test",
    )


@fixture
def versions() -> Dict[str, str]:
    return dict(fix_collect_single="0.0.1")


@fixture
def coordinator() -> LazyJobCoordinator:
    return LazyJobCoordinator()


@fixture
async def worker_queue(
    arq_redis: ArqRedis,
    coordinator: LazyJobCoordinator,
    credentials: Dict[str, Dict[str, str]],
    versions: Dict[str, str],
) -> WorkerQueue:
    return WorkerQueue(arq_redis, coordinator, credentials, versions, "redis://localhost:6379/0")
