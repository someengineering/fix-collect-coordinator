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
import socket
from argparse import ArgumentParser, Namespace
from contextlib import suppress
from typing import AsyncIterator, TypeVar

from aiohttp import web
from aiohttp.web_app import Application
from arq import create_pool
from arq.connections import RedisSettings
from fixcloudutils.redis.event_stream import RedisStreamPublisher
from kubernetes_asyncio import config
from kubernetes_asyncio.client import ApiClient
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff

from collect_coordinator.api import Api
from collect_coordinator.job_coordinator import JobCoordinator
from collect_coordinator.util import setup_process, CollectDependencies

log = logging.getLogger("collect.coordinator")
T = TypeVar("T")


def start(args: Namespace) -> None:
    hostname = socket.gethostname()
    deps = CollectDependencies(args=args)
    app = web.Application()

    async def on_start() -> None:
        await config.load_kube_config(config_file=args.kube_config)
        redis = Redis.from_url(deps.redis_event_url, decode_responses=True, retry=Retry(ExponentialBackoff(), 10))  # type: ignore # noqa
        arq_redis = await create_pool(RedisSettings.from_dsn(deps.redis_worker_url))
        publisher = deps.add(
            "publisher", RedisStreamPublisher(redis, "collect-coordinator-events", "collect-coordinator")
        )
        api_client = deps.add("api_client", ApiClient(pool_threads=10))
        coordinator = deps.add(
            "job_coordinator",
            JobCoordinator(hostname, arq_redis, publisher, api_client, args.namespace, args.max_parallel_jobs),
        )
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
    args = ap.parse_args()

    setup_process(args)
    start(args)


if __name__ == "__main__":
    main()
