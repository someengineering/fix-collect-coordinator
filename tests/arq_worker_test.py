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

from typing import List

import pytest
from arq.connections import ArqRedis
from attr import define


@define
class ExampleData:
    foo: int
    bar: str
    bla: List[int]


@pytest.mark.skip(reason="only for ingesting test data")
async def test_ping(arq_redis: ArqRedis) -> None:
    job = await arq_redis.enqueue_job("ping")
    assert job is not None
    print("Got result: ", await job.result())


@pytest.mark.skip(reason="only for ingesting test data")
async def test_enqueue_job(arq_redis: ArqRedis) -> None:
    arg = dict(
        tenant_id="test",
        graphdb_server="http://db-0.dbs.fix.svc.cluster.local:8529",
        graphdb_database="db3",
        graphdb_username="fix",
        graphdb_password="",
        account=dict(
            kind="aws_account_information",
            aws_account_id="123456789012",
            aws_account_name="test",
            aws_role_arn="arn:aws:iam::123456789012:role/test",
            external_id="test",
        ),
        env={"test": "test"},
    )
    job = await arq_redis.enqueue_job("collect", arg)
    print("Job enqueued. Waiting for result...")
    assert job is not None
    try:
        result = await job.result()
        print(result)
    except Exception as e:
        print(e)
