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

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Dict, Any, Optional, List, Set

from arq.connections import ArqRedis
from arq.worker import Worker, Function
from bitmath import MiB, GiB
from fixcloudutils.asyncio import stop_running_task
from fixcloudutils.service import Service
from fixcloudutils.types import Json

from collect_coordinator.job_coordinator import JobDefinition, ComputeResources, JobCoordinator

log = logging.getLogger("collect.coordinator")


class WorkerQueue(Service):
    def __init__(
        self,
        redis: ArqRedis,
        coordinator: JobCoordinator,
        credentials: Dict[str, Dict[str, str]],
        versions: Dict[str, str],
        redis_event_url: str,
    ) -> None:
        self.redis = redis
        self.coordinator = coordinator
        self.worker: Optional[Worker] = None
        self.redis_worker_task: Optional[asyncio.Task[Any]] = None
        self.credentials = credentials
        self.versions = versions
        self.redis_event_url = redis_event_url

    async def collect(self, ctx: Dict[Any, Any], *args: Any, **kwargs: Any) -> bool:
        log.debug(f"Collect function called with ctx: {ctx}, args: {args}, kwargs: {kwargs}")
        job_id: str = ctx["job_id"]
        if len(args) == 1 and isinstance(args[0], dict):
            data = args[0]
            data["job_id"] = job_id
            try:
                jd = self.parse_collect_definition_json(data)
            except Exception as e:
                message = f"Failed to parse collect definition json: {e}"
                log.error(message, exc_info=True)
                raise ValueError(message) from e
            log.info(f"Received collect job {jd.name}")
            future = await self.coordinator.start_job(jd)
            log.info(f"Collect job {jd.name} started. Wait for the job to finish.")
            return await future
        else:
            message = f"Invalid arguments for collect function. Got {args}. Expect one arg of type Json."
            log.error(message)
            raise ValueError(message)

    async def ping(self, ctx: Dict[Any, Any], *args: Any, **kwargs: Any) -> str:
        log.info(f"Got Ping request with following context: {ctx}, args: {args}, kwargs: {kwargs}")
        return "pong"

    async def start(self) -> Any:
        worker = Worker(
            functions=[
                Function(
                    name="ping",
                    coroutine=self.ping,
                    timeout_s=3,
                    keep_result_s=180,
                    keep_result_forever=False,
                    max_tries=1,
                ),
                Function(
                    name="collect",
                    coroutine=self.collect,
                    timeout_s=3600,
                    keep_result_s=180,
                    keep_result_forever=False,
                    max_tries=1,
                ),
            ],
            job_timeout=3600,  # 1 hour
            redis_pool=self.redis,
            max_jobs=self.coordinator.max_parallel,
            keep_result=180,
            retry_jobs=False,
            handle_signals=False,
            log_results=False,
        )
        self.worker = worker
        self.redis_worker_task = asyncio.create_task(worker.async_run())

    async def stop(self) -> Any:
        if self.worker:
            # worker close stops with errors
            for task in self.worker.tasks.values():
                await stop_running_task(task)
            for task in self.worker.job_tasks.values():
                await stop_running_task(task)
            await stop_running_task(self.worker.main_task)
            await self.worker.pool.delete(self.worker.health_check_key)
        await stop_running_task(self.redis_worker_task)

    def parse_collect_definition_json(self, js: Json) -> JobDefinition:
        job_id = js["job_id"]  # str
        tenant_id = js["tenant_id"]  # str
        graphdb_server = js["graphdb_server"]  # str
        graphdb_database = js["graphdb_database"]  # str
        graphdb_username = js["graphdb_username"]  # str
        graphdb_password = js["graphdb_password"]  # str
        account = js["account"]
        env = js.get("env") or {}  # Optional[Dict[str, str]]
        account_len_hint = js.get("account_len_hint", 1)  # Optional[int]
        if account_len_hint == 1:
            requires = ComputeResources(cores=1, memory=MiB(512))
            limits = ComputeResources(cores=1, memory=GiB(2))
        elif account_len_hint < 10:
            requires = ComputeResources(cores=2, memory=GiB(3))
            limits = ComputeResources(cores=4, memory=GiB(10))
        else:
            requires = ComputeResources(cores=4, memory=GiB(5))
            limits = None

        coordinator_args = [
            "--write",
            "resoto.worker.yaml=WORKER_CONFIG",
            "--job-id",
            job_id,
            "--tenant-id",
            tenant_id,
            "--redis-url",
            self.redis_event_url,
            "--ca-cert",
            "/etc/ssl/certs/ca.crt",
        ]
        core_args = [
            "--graphdb-bootstrap-do-not-secure",  # root password comes via the environment
            "--graphdb-server",
            graphdb_server,
            "--graphdb-database",
            graphdb_database,
            "--graphdb-username",
            graphdb_username,
            "--graphdb-password",
            graphdb_password,
            "--override-path",
            "/home/resoto/resoto.worker.yaml",
            "--ca-cert",  # make the ca available to core
            "/etc/ssl/certs/ca.crt",
        ]
        worker_args: List[str] = []
        worker_config: Json = {}
        collectors: Set[str] = set()
        # make the root password available via env
        if graph_db_root_password := self.credentials.get("graph_db_root_password"):
            env["RESOTOCORE_GRAPHDB_ROOT_PASSWORD"] = graph_db_root_password
        if redis_password := self.credentials.get("redis_password"):
            env["REDIS_PASSWORD"] = redis_password

        def handle_aws_account() -> None:
            account_id = account["aws_account_id"]
            account_name = account.get("aws_account_name")  # optional
            role_arn = account["aws_role_arn"]
            external_id = account["external_id"]
            profile = account_name or account_id
            env["AWS_CREDENTIALS"] = (
                "[default]\n"
                f"aws_access_key_id = {self.credentials['aws']['aws_access_key_id']}\n"
                f"aws_secret_access_key = {self.credentials['aws']['aws_secret_access_key']}\n\n"
                f"[{profile}]\n"
                f"role_arn = {role_arn}\n"
                "source_profile = default\n"
                f"external_id = {external_id}\n"
            )
            coordinator_args.extend(["--write", ".aws/credentials=AWS_CREDENTIALS"])
            collectors.add("aws")
            worker_config["aws"] = {
                "account": [account_id],
                "profiles": [profile],
                "prefer_profile_as_account_name": account_name is not None,
            }

        if account["kind"] == "aws_account_information":
            handle_aws_account()
        else:
            raise ValueError("Don't know how to collect account kind: {account['kind']}")

        worker_config["resotoworker"] = {"collector": list(collectors)}
        env["WORKER_CONFIG"] = json.dumps(worker_config)
        return JobDefinition(
            id=job_id,
            name="collect-" + str(uuid.uuid1()),
            image="someengineering/fix-collect-single:" + self.versions.get("fix_collect_single", "edge"),
            args=[*coordinator_args, "---", *core_args, "---", *worker_args],
            requires=requires,
            limits=limits,
            env=env,
        )
