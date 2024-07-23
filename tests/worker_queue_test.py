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
import os

import pytest
from arq.connections import ArqRedis
from fixcloudutils.types import Json
from pytest import fixture

from collect_coordinator.worker_queue import WorkerQueue
from tests.conftest import LazyJobCoordinator, worker_queue


@fixture
def example_aws_collect_definition() -> Json:
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


@fixture
def example_azure_collect_definition() -> Json:
    return {
        "job_id": "uid",
        "tenant_id": "a",
        "graphdb_server": "b",
        "graphdb_database": "c",
        "graphdb_username": "d",
        "graphdb_password": "e",
        "account": {
            "kind": "azure_subscription_information",
            "azure_subscription_id": "123",
            "tenant_id": "123",
            "client_id": "234",
            "client_secret": "bombproof",
            "collect_microsoft_graph": False,
        },
        "env": {"test": "test"},
    }


@fixture
def example_post_collect_definition() -> Json:
    return {
        "job_id": "uid",
        "tenant_id": "a",
        "graphdb_server": "b",
        "graphdb_database": "c",
        "graphdb_username": "d",
        "graphdb_password": "e",
        "accounts_collected": [
            {"cloud": "aws", "account_id": "aws_123456789012", "task_id": "t1-987654321"},
            {"cloud": "gcp", "account_id": "gcp_123456789012", "task_id": "t2-987654321"},
            {"cloud": "azure", "account_id": "ms_123456789012", "task_id": "t3-987654321"},
        ],
        "env": {"test": "test"},
    }


@pytest.mark.skipif(os.environ.get("REDIS_RUNNING", "false") != "true", reason="Redis not running")
def test_read_azure_job_definition(worker_queue: WorkerQueue, example_azure_collect_definition: Json) -> None:
    job_def = worker_queue.parse_collect_definition_json(example_azure_collect_definition)
    assert (
        job_def.env
        and job_def.env["WORKER_CONFIG"]
        == '{"azure": {"accounts": {"default": {"subscriptions": ["123"], "collect_microsoft_graph": false, '
        '"client_secret": {"tenant_id": "123", "client_id": "234", "client_secret": "bombproof"}}}}, '
        '"fixworker": {"collector": ["azure"]}}'
    )


@pytest.mark.skipif(os.environ.get("REDIS_RUNNING", "false") != "true", reason="Redis not running")
def test_read_aws_job_definition(worker_queue: WorkerQueue, example_aws_collect_definition: Json) -> None:
    job_def = worker_queue.parse_collect_definition_json(example_aws_collect_definition)
    assert job_def.name.startswith("collect")
    assert job_def.image == "someengineering/fix-collect-single:0.0.1"
    # fmt: off
    assert job_def.args == [
        "collect",
        "--write", "fix.worker.yaml=WORKER_CONFIG",
        "--job-id", "uid",
        "--tenant-id", "a",
        "--redis-url", "redis://localhost:6379/0",
        "--ca-cert", "/etc/ssl/certs/ca.crt",
        "--push-gateway-url", "http://pushgateway-prometheus-pushgateway.monitoring.svc.cluster.local:9091",
        "--write", ".aws/credentials=AWS_CREDENTIALS",
        "--cloud", "aws",
        "--account-id", "123456789012",
        "---",
        "--graphdb-bootstrap-do-not-secure",
        "--graphdb-server", "b",
        "--graphdb-database", "c",
        "--graphdb-username", "d",
        "--graphdb-password", "e",
        "--override-path", "/home/fixinventory/fix.worker.yaml",
        "--ca-cert", "/etc/ssl/certs/ca.crt",
        "---",
        "--idle-timeout", "120",
    ]
    # fmt: on
    assert job_def.env == {
        "AWS_CREDENTIALS": "[default]\n"
        "aws_access_key_id = some_access\n"
        "aws_secret_access_key = some_secret\n\n"
        "[test]\n"
        "role_arn = arn:aws:iam::123456789012:role/test\n"
        "source_profile = default\n"
        "external_id = test\n",
        "WORKER_CONFIG": '{"aws": {"account": '
        '["123456789012"], "profiles": ["test"], '
        '"prefer_profile_as_account_name": true}, '
        '"fixworker": {"collector": ["aws"]}}',
        "test": "test",
        "REDIS_PASSWORD": "test",
        "FIXCORE_GRAPHDB_ROOT_PASSWORD": "test",
    }


@pytest.mark.skipif(os.environ.get("REDIS_RUNNING", "false") != "true", reason="Redis not running")
def test_read_post_job_definition(worker_queue: WorkerQueue, example_post_collect_definition: Json) -> None:
    job_def = worker_queue.parse_post_collect_definition_json(example_post_collect_definition)
    assert job_def.name.startswith("post-collect")
    assert job_def.image == "someengineering/fix-collect-single:0.0.1"
    # fmt:off
    assert job_def.args == [
        "post-collect",
        "--job-id", "uid",
        "--tenant-id", "a",
        "--redis-url", "redis://localhost:6379/0",
        "--ca-cert", "/etc/ssl/certs/ca.crt",
        "--push-gateway-url", "http://pushgateway-prometheus-pushgateway.monitoring.svc.cluster.local:9091",
        "--accounts-collected", '[{"cloud": "aws", "account_id": "aws_123456789012", "task_id": "t1-987654321"}, {"cloud": "gcp", "account_id": "gcp_123456789012", "task_id": "t2-987654321"}, {"cloud": "azure", "account_id": "ms_123456789012", "task_id": "t3-987654321"}]', # noqa
        "---",
        "--graphdb-bootstrap-do-not-secure",
        "--graphdb-server", "b",
        "--graphdb-database", "c",
        "--graphdb-username", "d",
        "--graphdb-password", "e",
        "--ca-cert", "/etc/ssl/certs/ca.crt",
    ]
    # fmt:on


@pytest.mark.asyncio
@pytest.mark.skipif(os.environ.get("REDIS_RUNNING", "false") != "true", reason="Redis not running")
async def test_enqueue_jobs(
    arq_redis: ArqRedis,
    worker_queue: WorkerQueue,
    coordinator: LazyJobCoordinator,
    example_aws_collect_definition: Json,
) -> None:
    async with worker_queue:
        await arq_redis.enqueue_job("collect", example_aws_collect_definition)
        await arq_redis.enqueue_job("collect", example_aws_collect_definition)
        await arq_redis.enqueue_job("collect", example_aws_collect_definition)
        ping = await arq_redis.enqueue_job("ping")
        assert ping is not None

        async def assert_job_in_queue() -> None:
            while len(coordinator.running_jobs) != 3:
                await asyncio.sleep(0.01)
            await coordinator.mark_all_jobs_done()

        await asyncio.wait_for(assert_job_in_queue(), timeout=5)
        assert await ping.result() == "pong"
