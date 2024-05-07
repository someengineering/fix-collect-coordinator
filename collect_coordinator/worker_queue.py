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

from __future__ import annotations

import json
import logging
import uuid
from typing import Dict, Any, List, Set

from arq.connections import RedisSettings
from arq.constants import default_queue_name
from arq.worker import func
from bitmath import GiB
from fixcloudutils.asyncio.timed import timed
from fixcloudutils.redis.worker_queue import WorkerInstance
from fixcloudutils.service import Service
from fixcloudutils.types import Json

from collect_coordinator.job_coordinator import JobDefinition, ComputeResources, JobCoordinator

log = logging.getLogger("collect.coordinator")

# Home directory in the fix docker image
ImageHome = "/home/fixinventory"


class WorkerQueue(Service):
    def __init__(
        self,
        redis_settings: RedisSettings,
        coordinator: JobCoordinator,
        credentials: Dict[str, Dict[str, str]],
        versions: Dict[str, str],
        redis_event_url: str,
    ) -> None:
        self.redis_settings = redis_settings
        self.coordinator = coordinator
        self.credentials = credentials
        self.versions = versions
        self.redis_event_url = redis_event_url
        self.worker_instance = WorkerInstance(
            redis_settings,
            default_queue_name,  # TODO: change to a defined name
            functions=[
                func(coroutine=self.ping, name="ping", timeout=3, max_tries=1),
                func(coroutine=self.collect, name="collect", timeout=3600, max_tries=1),
            ],
            max_jobs=self.coordinator.max_parallel,
        )

    @timed(module="collect_coordinator", name="collect")
    async def collect(self, ctx: Dict[Any, Any], *args: Any, **kwargs: Any) -> bool:
        log.debug(f"Collect function called with ctx: {ctx}, args: {args}, kwargs: {kwargs}")
        job_id: str = ctx["job_id"]
        if len(args) == 1 and isinstance(args[0], dict):
            data = args[0]
            data["job_id"] = job_id
            try:
                jd = self.parse_collect_definition_json(data)
                log.info(f"Collect function called: id={jd.id}, name={jd.name}")
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
        await self.worker_instance.start()

    async def stop(self) -> Any:
        await self.worker_instance.stop()

    def parse_collect_definition_json(self, js: Json) -> JobDefinition:
        job_id = js["job_id"]  # str
        tenant_id = js["tenant_id"]  # str
        graphdb_server = js["graphdb_server"]  # str
        graphdb_database = js["graphdb_database"]  # str
        graphdb_username = js["graphdb_username"]  # str
        graphdb_password = js["graphdb_password"]  # str
        account = js["account"]
        debug = js.get("debug", False)  # Optional[bool]
        retry_failed = js.get("retry_failed_for_seconds")  # Optional[float]
        # each job run is one account
        requires = ComputeResources(cores=1, memory=GiB(4))
        limits = ComputeResources(cores=4, memory=GiB(16))

        # environment variables to pass to process
        env = js.get("env") or {}  # Optional[Dict[str, str]]
        # this defined the worker override configuration
        worker_config: Json = {}
        # the set of collectors to run
        collectors: Set[str] = set()
        # all coordinator arguments
        coordinator_args = [
            "--write",
            "fix.worker.yaml=WORKER_CONFIG",
            "--job-id",
            job_id,
            "--tenant-id",
            tenant_id,
            "--redis-url",
            self.redis_event_url,
            "--ca-cert",
            "/etc/ssl/certs/ca.crt",
            "--push-gateway-url",
            "http://pushgateway-prometheus-pushgateway.monitoring.svc.cluster.local:9091",
        ]
        if retry_failed:
            coordinator_args.extend(["--retry-failed-for", str(retry_failed)])
        # all fixcore arguments
        core_args = [
            "--graphdb-bootstrap-do-not-secure",
            "--graphdb-server",
            graphdb_server,
            "--graphdb-database",
            graphdb_database,
            "--graphdb-username",
            graphdb_username,
            "--graphdb-password",
            graphdb_password,
            "--override-path",
            f"{ImageHome}/fix.worker.yaml",
            "--ca-cert",
            "/etc/ssl/certs/ca.crt",
        ]
        # all fixworker arguments
        worker_args: List[str] = [
            # A collect message should arrive within 2 minutes. If not, fail the process.
            "--idle-timeout",
            "120",
        ]
        # make the root password available via env
        if graph_db_root_password := self.credentials.get("graph_db_root_password"):
            env["FIXCORE_GRAPHDB_ROOT_PASSWORD"] = graph_db_root_password
        if redis_password := self.credentials.get("redis_password"):
            env["REDIS_PASSWORD"] = redis_password
        if debug:
            worker_args.append("--verbose")
            # core_args.append("--debug")

        def handle_azure_subscription() -> None:
            az_subscription_id = account["azure_subscription_id"]
            az_tenant_id = account["tenant_id"]
            az_client_id = account["client_id"]
            az_client_secret = account["client_secret"]
            coordinator_args.extend(
                [
                    "--cloud",
                    "azure",
                    "--account-id",
                    az_subscription_id,
                ]
            )
            collectors.add("azure")
            worker_config["azure"] = {
                "accounts": {
                    "default": {
                        "subscriptions": [az_subscription_id],
                        "client_secret": {
                            "tenant_id": az_tenant_id,
                            "client_id": az_client_id,
                            "client_secret": az_client_secret,
                        },
                    }
                },
            }

        def handle_gcp_project() -> None:
            gcp_project_id = account["gcp_project_id"]
            gcp_credentials = account["google_application_credentials"]
            env["GCP_CREDENTIALS"] = gcp_credentials
            filename = f"{ImageHome}/.gcp/credentials"
            coordinator_args.extend(
                [
                    "--write",
                    f"{filename}=GCP_CREDENTIALS",
                    "--cloud",
                    "gcp",
                    "--account-id",
                    gcp_project_id,
                ]
            )
            collectors.add("gcp")
            worker_config["gcp"] = {"service_account": [filename], "project": [gcp_project_id]}

        def handle_aws_account() -> None:
            aws_account_id = account["aws_account_id"]
            aws_account_name = account.get("aws_account_name")  # optional
            aws_role_arn = account["aws_role_arn"]
            aws_external_id = account["external_id"]
            aws_profile = aws_account_name or aws_account_id
            env["AWS_CREDENTIALS"] = (
                "[default]\n"
                f"aws_access_key_id = {self.credentials['aws']['aws_access_key_id']}\n"
                f"aws_secret_access_key = {self.credentials['aws']['aws_secret_access_key']}\n\n"
                f"[{aws_profile}]\n"
                f"role_arn = {aws_role_arn}\n"
                "source_profile = default\n"
                f"external_id = {aws_external_id}\n"
            )
            coordinator_args.extend(
                [
                    "--write",
                    ".aws/credentials=AWS_CREDENTIALS",
                    "--cloud",
                    "aws",
                    "--account-id",
                    aws_account_id,
                ]
            )
            collectors.add("aws")
            worker_config["aws"] = {
                "account": [aws_account_id],
                "profiles": [aws_profile],
                "prefer_profile_as_account_name": aws_account_name is not None,
            }

        if account["kind"] == "aws_account_information":
            handle_aws_account()
        elif account["kind"] == "gcp_project_information":
            handle_gcp_project()
        elif account["kind"] == "azure_subscription_information":
            handle_azure_subscription()
        else:
            raise ValueError("Don't know how to collect account kind: {account['kind']}")

        worker_config["fixworker"] = {"collector": list(collectors)}
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
