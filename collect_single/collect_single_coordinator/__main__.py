import asyncio
import logging
import sys
from asyncio import Future, streams
from asyncio.subprocess import Process
from contextlib import suppress
from itertools import takewhile
from pathlib import Path
from signal import SIGKILL
from typing import List, Optional, Any

from resotoclient.async_client import ResotoClient

from resotolib.proc import kill_children
from resotolib.args import ArgumentParser

log = logging.getLogger("resoto.coordinator")


class ProcessWrapper:
    def __init__(self, cmd: List[str]) -> None:
        self.cmd = cmd
        self.process: Optional[Process] = None
        self.reader: Optional[Future[Any]] = None

    async def read_stream(self, stream: streams.StreamReader) -> None:
        while True:
            line = await stream.readline()
            if line:
                print(line.decode("utf-8").strip())
            else:
                await asyncio.sleep(0.1)

    async def run(self) -> None:
        process = await asyncio.create_subprocess_exec(
            *self.cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        self.process = process
        self.reader = asyncio.gather(
            asyncio.create_task(self.read_stream(process.stdout)),
            asyncio.create_task(self.read_stream(process.stderr)),
        )

    async def stop(self, wait_seconds: int = 0) -> None:
        if self.reader:
            self.reader.cancel()
            with suppress(asyncio.CancelledError):
                await self.reader
        if self.process:
            self.process.terminate()
            await asyncio.sleep(wait_seconds)
            kill_children(SIGKILL, process_pid=self.process.pid)

    async def __aenter__(self):
        await self.run()

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()


class CollectAndSync:
    def __init__(self, core_args: List[str], worker_args: List[str], core_url: str = "http://localhost:8980") -> None:
        self.core_args = ["resotocore", "--no-scheduling", "--ignore-interrupted-tasks"] + core_args
        self.worker_args = ["resotoworker"] + worker_args
        self.core_url = core_url
        self.task_id: Optional[str] = None

    async def core_client(self) -> ResotoClient:
        client = ResotoClient(self.core_url)
        while True:
            try:
                await client.ping()
                return client
            except Exception:
                await asyncio.sleep(1)

    async def wait_for_collect_done(self, client: ResotoClient) -> bool:
        async for event in client.events({"task_end"}):
            if self.task_id and event.get("data", {}).get("task_id", "") == self.task_id:
                log.info(f"Received event. Exit.  {event}")
                return True
            else:
                log.info(f"Received event. Waiting for task {self.task_id}. Ignore: {event}")

    async def wait_for_worker_connected(self, client: ResotoClient) -> None:
        while True:
            res = await client.subscribers_for_event("collect")
            if len(res) > 0:
                return
            log.info("Wait for worker to connect.")
            await asyncio.sleep(1)

    async def wait_for_collect_tasks_to_finish(self, client: ResotoClient) -> None:
        while True:
            running = [
                entry async for entry in client.cli_execute("workflows running") if entry.get("workflow") != "collect"
            ]
            if len(running) == 0:
                return
            else:
                log.info(f"Wait for running workflows to finish. Running: {running}")
                await asyncio.sleep(5)

    async def start_collect(self, client: ResotoClient) -> None:
        running = [
            entry async for entry in client.cli_execute("workflows running") if entry.get("workflow") == "collect"
        ]
        if not running:
            log.info("No collect workflow running. Start one.")
            # start a workflow
            async for result in client.cli_execute("workflow run collect"):
                pass
            running = [
                entry async for entry in client.cli_execute("workflows running") if entry.get("workflow") == "collect"
            ]
        log.info(f"All running collect workflows: {running}")
        if running:
            self.task_id = running[0]["task-id"]
        else:
            raise Exception("Could not start collect workflow")

    async def sync(self) -> None:
        async with ProcessWrapper(self.core_args):
            log.info("Core started.")
            async with await asyncio.wait_for(self.core_client(), timeout=60) as client:
                log.info("Core client connected")
                # wait up to 5 minutes for all running workflows to finish
                await asyncio.wait_for(self.wait_for_collect_tasks_to_finish(client), timeout=300)
                log.info("All collect workflows finished")
                async with ProcessWrapper(self.worker_args):
                    log.info("Worker started")
                    try:
                        # wait for worker to be connected
                        event_listener = asyncio.create_task(self.wait_for_collect_done(client))
                        # wait for worker to be connected
                        await asyncio.wait_for(self.wait_for_worker_connected(client), timeout=60)
                        log.info("Worker connected")
                        await self.start_collect(client)
                        log.info("Collect started. wait for the collect to finish")
                        await asyncio.wait_for(event_listener, 3600)  # wait up to 1 hour
                        log.info("Event listener done")
                    except Exception as ex:
                        log.info(f"Got exception {ex}. Giving up", exc_info=ex)
                        raise


def main() -> None:
    # 3 argument sets delimited by "---": <coordinator args> --- <core args> --- <worker args>
    # coordinator --main-arg1 --main-arg2 --- --core-arg1 --core-arg2 --- --worker-arg1 --worker-arg2
    args = iter(sys.argv[1:])
    coordinator_args = list(takewhile(lambda x: x != "---", args))
    core_args = list(takewhile(lambda x: x != "---", args))
    worker_args = list(args)
    # handle coordinator args
    parser = ArgumentParser()
    parser.add_argument("--worker-config", help="Worker config file")
    parsed = parser.parse_args(coordinator_args)
    if parsed.worker_config:
        log.info("Writing worker config")
        worker_config = Path.home() / "resoto.worker.yaml"
        with open(worker_config, "w+") as f:
            f.write(parsed.worker_config)
        worker_args.extend(["--override-path", str(worker_config)])

    log.info(f"Coordinator args:({coordinator_args}) Core args:({core_args}) Worker args:({worker_args})")
    asyncio.run(CollectAndSync(core_args, worker_args).sync())


if __name__ == "__main__":
    main()
