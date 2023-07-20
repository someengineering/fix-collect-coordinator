import asyncio
import logging
from contextlib import suppress
from typing import AsyncIterator, TypeVar

from aiohttp import web
from aiohttp.web_app import Application
from kubernetes_asyncio import config
from kubernetes_asyncio.client import ApiClient

from collect_coordinator.api import Api
from collect_coordinator.job_coordinator import JobCoordinator
from collect_coordinator.service import Dependencies

log = logging.getLogger("collect.coordinator")
T = TypeVar("T")


def start() -> None:
    services = Dependencies()
    api = Api(services)

    async def on_start() -> None:
        await config.load_kube_config()
        api = services.add("api_client", ApiClient())
        services.add("job_coordinator", JobCoordinator(api, "test"))
        await services.start()

    async def on_stop() -> None:
        await services.stop()

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

        api.app.cleanup_ctx.append(on_start_stop)
        return api.app

    web.run_app(async_initializer(), host="0.0.0.0", port=8080)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    start()


if __name__ == "__main__":
    main()
