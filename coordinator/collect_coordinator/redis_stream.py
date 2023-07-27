import asyncio
import json
import logging
import random
import uuid
from asyncio import Task, CancelledError
from contextlib import suppress
from datetime import datetime, timezone
from functools import partial
from typing import Callable, Any, Union, Awaitable, Optional, TypeVar

from attrs import define
from redis.asyncio import Redis

from collect_coordinator.job_coordinator import Json
from collect_coordinator.service import Service

log = logging.getLogger("collect.coordinator")
UTC_Date_Format = "%Y-%m-%dT%H:%M:%SZ"
T = TypeVar("T")


@define(frozen=True, slots=True)
class Backoff:
    base_delay: float
    maximum_delay: float
    retries: int

    def wait_time(self, attempt: int) -> float:
        delay: float = self.base_delay * (2**attempt + random.uniform(0, 1))
        return min(delay, self.maximum_delay)

    async def with_backoff(self, fn: Callable[[], Awaitable[T]], attempt: int = 0) -> T:
        try:
            return await fn()
        except Exception as e:
            if attempt < self.retries:
                delay = self.wait_time(attempt)
                log.warning(f"Got Exception in attempt {attempt}. Retry after {delay} seconds: {e}")
                await asyncio.sleep(delay)
                return await self.with_backoff(fn, attempt + 1)
            else:
                raise


NoBackoff = Backoff(0, 0, 0)


class RedisStreamListener(Service):
    def __init__(
        self,
        redis: Redis,  # type: ignore
        stream: str,
        listener: str,
        message_processor: Callable[[Json], Union[Awaitable[Any], Any]],
        batch_size: int = 1000,
        wait_for_batch_ms: int = 1000,
        stop_on_fail: bool = False,
        backoff: Optional[Backoff] = Backoff(0.1, 10, 10),
    ) -> None:
        """
        Create a RedisStream client.
        :param redis: the redis client.
        :param stream: the name of the redis event stream.
        :param listener:  the name of this listener (used to store the last read event id).
        :param message_processor: the function to process the event message.
        :param batch_size: the number of events to read in one batch.
        :param wait_for_batch_ms: the time to wait for events in one batch.
        :param stop_on_fail: if True, the listener will stop if a failed event is retried too many times.
        :param backoff: the backoff strategy to use when retrying failed events.
        """
        self.redis = redis
        self.stream = stream
        self.listener = listener
        self.message_processor = message_processor
        self.batch_size = batch_size
        self.wait_for_batch_ms = wait_for_batch_ms
        self.stop_on_fail = stop_on_fail
        self.backoff = backoff or NoBackoff
        self.__should_run = True
        self.__listen_task: Optional[Task[Any]] = None

    async def listen(self) -> None:
        last = (await self.redis.hget(f"{self.stream}.listener", self.listener)) or 0
        while self.__should_run:
            try:
                # wait for either batch_size messages or wait_for_batch_ms time whatever comes first
                res = await self.redis.xread(
                    {self.stream: last},
                    count=self.batch_size,
                    block=self.wait_for_batch_ms,
                )
                if res:
                    [[_, content]] = res  # safe, since we are reading from one stream
                    for rid, data in content:
                        await self.handle_message(data)
                        # await self.handle_message(byte_dict_to_str(data))
                        last = rid
                    # acknowledge all messages by committing the last read id
                    await self.redis.hset(f"{self.stream}.listener", self.listener, last)
            except Exception as e:
                log.error(f"Failed to read from stream {self.stream}: {e}", exc_info=True)
                if self.stop_on_fail:
                    raise

    async def handle_message(self, message: Json) -> None:
        try:
            if "id" in message and "at" in message and "data" in message:
                mid = message["id"]
                at = message["at"]
                data = json.loads(message["data"])
                log.debug(f"Received message {self.listener}: message {mid}, from {at}, data: {data}")
                await self.backoff.with_backoff(partial(self.message_processor, data))
            else:
                log.warning(f"Invalid message format: {message}. Ignore.")
        except Exception as e:
            if self.stop_on_fail:
                raise e
            else:
                log.error(f"Failed to process message {self.listener}: {message}. Error: {e}")
                # write the failed message to the dlq
                await self.redis.xadd(
                    f"{self.stream}.dlq", {"listener": self.listener, "error": str(e), "message": json.dumps(message)}
                )

    async def start(self) -> Any:
        self.__should_run = True
        self.__listen_task = asyncio.create_task(self.listen())

    async def stop(self) -> Any:
        self.__should_run = False
        if self.__listen_task:
            self.__listen_task.cancel()
            with suppress(CancelledError):
                await self.__listen_task


class RedisStreamPublisher(Service):
    """
    Publish messages to a redis stream.
    :param redis: the redis client.
    :param stream: the name of the redis event stream.
    """

    def __init__(self, redis: Redis, stream: str) -> None:  # type: ignore
        self.redis = redis
        self.stream = stream

    async def publish(self, message: Json) -> None:
        now_str = datetime.now(timezone.utc).strftime(UTC_Date_Format)
        message = {"id": str(uuid.uuid1()), "at": now_str, "data": json.dumps(message)}
        await self.redis.xadd(self.stream, message)
