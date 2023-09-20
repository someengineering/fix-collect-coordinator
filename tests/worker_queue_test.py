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

import pytest
from fixcloudutils.types import Json
from pytest import fixture

from collect_coordinator.worker_queue import WorkerQueue
from tests.conftest import LazyJobCoordinator, worker_queue


@fixture
def example_definition() -> Json:
    return {
        "job_id": "uid",
        "tenant_id": "a",
        "graphdb_server": "b",
        "graphdb_database": "c",
        "graphdb_username": "d",
        "graphdb_password": "e",
        "account": {
            "kind": "aws_account_information",
            "aws_account_id": "123456789012",
            "aws_account_name": "test",
            "aws_role_arn": "arn:aws:iam::123456789012:role/test",
            "external_id": "test",
        },
        "env": {"test": "test"},
    }


def test_read_job_definition(worker_queue: WorkerQueue, example_definition: Json) -> None:
    job_def = worker_queue.parse_collect_definition_json(example_definition)
    assert job_def.name.startswith("collect")
    assert job_def.args == [
        "--write",
        "resoto.worker.yaml=WORKER_CONFIG",
        "--job-id",
        "uid",
        "--tenant-id",
        "a",
        "--redis-url",
        "redis://redis-master.fix.svc.cluster.local:6379/0",
        "--write",
        ".aws/credentials=AWS_CREDENTIALS",
        "---",
        "--graphdb-bootstrap-do-not-secure",
        "--graphdb-server",
        "b",
        "--graphdb-database",
        "c",
        "--graphdb-username",
        "d",
        "--graphdb-password",
        "e",
        "--override-path",
        "/home/resoto/resoto.worker.yaml",
        "---",
    ]
    assert job_def.env == {
        "AWS_CREDENTIALS": "[default]\n"
        "aws_access_key_id = \n"
        "aws_secret_access_key = \n\n"
        "[test]\n"
        "role_arn = arn:aws:iam::123456789012:role/test\n"
        "source_profile = default\n"
        "external_id = test\n",
        "RESOTO_LOG_TEXT": "true",
        "WORKER_CONFIG": '{"resotoworker": {"collector": ["aws"], "aws": {"account": '
        '["123456789012"], "profiles": ["test"], '
        '"prefer_profile_as_account_name": true}}}',
        "test": "test",
    }


@pytest.mark.asyncio
async def test_enqueue_jobs(
    worker_queue: WorkerQueue, coordinator: LazyJobCoordinator, example_definition: Json
) -> None:
    async with worker_queue:
        await worker_queue.redis.enqueue_job("collect", example_definition)
        await worker_queue.redis.enqueue_job("collect", example_definition)
        await worker_queue.redis.enqueue_job("collect", example_definition)
        ping = await worker_queue.redis.enqueue_job("ping")
        assert ping is not None

        async def assert_job_in_queue() -> None:
            while len(coordinator.running_jobs) != 3:
                await asyncio.sleep(0.01)
            await coordinator.mark_all_jobs_done()

        await asyncio.wait_for(assert_job_in_queue(), timeout=5)
        assert await ping.result() == "pong"
