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
import logging
import os
import socket
from argparse import ArgumentParser, Namespace
from contextlib import suppress
from typing import AsyncIterator, TypeVar

from aiohttp import web
from aiohttp.web_app import Application
from arq import create_pool
from arq.connections import RedisSettings
from kubernetes_asyncio import config
from kubernetes_asyncio.client import ApiClient

from collect_coordinator.api import Api
from collect_coordinator.worker_queue import WorkerQueue
from collect_coordinator.job_coordinator import KubernetesJobCoordinator
from collect_coordinator.util import setup_process, CollectDependencies

log = logging.getLogger("collect.coordinator")
T = TypeVar("T")


def start(args: Namespace) -> None:
    hostname = socket.gethostname()
    deps = CollectDependencies(args=args)
    app = web.Application()
    credentials = dict(
        aws=dict(aws_access_key_id=args.aws_access_key_id, aws_secret_access_key=args.aws_secret_access_key)
    )
    versions = dict(fix_collect_single=(args.fix_collect_single_version or "edge"))
    log.info(f"Start collect coordinator hostname={hostname}, versions={versions}.")

    async def load_kube_config() -> None:
        loaded = False
        try:
            await config.load_kube_config(config_file=args.kube_config)
            log.info("Loaded kube config from file.")
            loaded = True
        except Exception as e:
            log.info(f"Failed to load kube config: {e}")
        try:
            config.incluster_config.load_incluster_config()
            log.info("Loaded kube config from incluster config.")
            loaded = True
        except Exception as e:
            log.info(f"Failed to load incluster config: {e}")
        if not loaded:
            raise RuntimeError("Failed to load kubernetes access configuration.")

    async def on_start() -> None:
        await load_kube_config()
        arq_redis = await create_pool(RedisSettings.from_dsn(deps.redis_worker_url))
        api_client = deps.add("api_client", ApiClient(pool_threads=10))
        coordinator = deps.add(
            "job_coordinator",
            KubernetesJobCoordinator(hostname, arq_redis, api_client, args.namespace, args.max_parallel_jobs),
        )
        deps.add("worker_queue", WorkerQueue(arq_redis, coordinator, credentials, versions))
        deps.add("api", Api(app, coordinator))
        await deps.start()

    async def on_stop() -> None:
        await deps.stop()

    async def async_initializer() -> Application:
        async def clean_all_tasks() -> None:
            log.info("Clean up all running tasks.")
            for task in asyncio.all_tasks():
                with suppress(asyncio.CancelledError):
                    if not task.done() or not task.cancelled():
                        task.cancel()
                    log.debug(f"Wait for task: {task}")
                    await task

        async def on_start_stop(_: Application) -> AsyncIterator[None]:
            await on_start()
            log.info("Initialization done. Starting API.")
            yield
            log.info("Shutdown initiated. Stop all tasks.")
            await on_stop()
            await clean_all_tasks()

        app.cleanup_ctx.append(on_start_stop)
        return app

    web.run_app(async_initializer(), host="0.0.0.0", port=8080)


def main() -> None:
    ap = ArgumentParser()
    ap.add_argument("--namespace", help="The namespace to start the jobs in.", required=True)
    ap.add_argument("--debug", action="store_true", help="Enable debug logging")
    ap.add_argument("--kube-config", help="Optional path to kube config file.")
    ap.add_argument(
        "--redis-url-nodb", help="Redis host. Default: redis://localhost:6379", default="redis://localhost:6379"
    )
    ap.add_argument("--redis-event-db", type=int, help="Redis worker db. Default to 0", default=0)
    ap.add_argument("--redis-worker-db", type=int, help="Redis worker db. Default to 5", default=5)
    ap.add_argument("--max-parallel-jobs", type=int, default=100, help="Jobs to spawn in parallel. Defaults to 100.")
    ap.add_argument("--aws-access-key-id", help="AWS access key id.", default=os.environ.get("AWS_ACCESS_KEY_ID"))
    ap.add_argument("--aws-secret-access-key", help="AWS secret.", default=os.environ.get("AWS_SECRET_ACCESS_KEY"))
    ap.add_argument(
        "--fix-collect-single-version",
        help="Image version for collect single.",
        default=os.environ.get("FIX_COLLECT_SINGLE_VERSION"),
    )

    args = ap.parse_args()
    setup_process(args)
    start(args)


if __name__ == "__main__":
    main()
