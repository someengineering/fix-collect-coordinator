from __future__ import annotations

import asyncio
import json
import logging
from asyncio import Future
from contextlib import suppress
from enum import Enum
from typing import Dict, Any, Optional, List, Tuple

from attr import evolve
from arq.connections import ArqRedis
from arq.worker import Worker, Function
from attrs import define
from bitmath import Byte, MiB, GiB
from kubernetes_asyncio import client as k8s
from kubernetes_asyncio.client import (
    V1Job,
    V1JobStatus,
    ApiException,
    V1ObjectMeta,
    V1JobSpec,
    V1PodTemplateSpec,
    V1PodSpec,
    V1Container,
    V1ResourceRequirements,
    V1EnvVar,
)
from kubernetes_asyncio.client.api_client import ApiClient
from kubernetes_asyncio.watch import Watch

from fixcloudutils.redis.event_stream import RedisStreamPublisher
from fixcloudutils.service import Service

log = logging.getLogger("collect.coordinator")
Json = Dict[str, Any]


@define(eq=True, order=True, repr=True, frozen=True)
class ComputeResources:
    cores: Optional[float] = None
    memory: Optional[Byte] = None

    def pod_spec(self) -> Json:
        limits = {}
        if self.cores is not None:
            limits["cpu"] = str(self.cores)
        if self.memory is not None:
            limits["memory"] = f"{int(self.memory.MiB.value)}Mi"
        return limits


@define(eq=True, order=True, repr=True, frozen=True)
class JobDefinition:
    id: str
    name: str
    image: str
    args: List[str]
    restart_policy: str = "Never"
    requires: Optional[ComputeResources] = None
    limits: Optional[ComputeResources] = None
    env: Optional[Dict[str, str]] = None

    @staticmethod
    def from_job(job: V1Job) -> JobDefinition:
        return JobDefinition(
            id=job.metadata.labels.get("job-id", "n/a"),
            name=job.metadata.name,
            image=job.spec.template.spec.containers[0].image,
            args=job.spec.template.spec.containers[0].args,
            restart_policy=job.spec.template.spec.restart_policy,
            env={e.name: e.value for e in job.spec.template.spec.containers[0].env},
        )

    @classmethod
    def collect_definition_json(cls, js: Json) -> JobDefinition:
        return cls.collect_definition(
            job_id=js["job_id"],  # str
            tenant_id=js["tenant_id"],  # str
            graphdb_server=js["graphdb_server"],  # str
            graphdb_database=js["graphdb_database"],  # str
            graphdb_username=js["graphdb_username"],  # str
            graphdb_password=js["graphdb_password"],  # str
            worker_config=json.dumps(js["worker_config"]),  # Json
            env=js.get("env"),  # Optional[Dict[str, str]]
            account_len_hint=js.get("account_len_hint"),  # Optional[int]
        )

    @classmethod
    def collect_definition(
        cls,
        job_id: str,
        tenant_id: str,
        graphdb_server: str,
        graphdb_database: str,
        graphdb_username: str,
        graphdb_password: str,
        worker_config: str,
        env: Optional[Dict[str, str]] = None,
        account_len_hint: Optional[int] = None,
    ) -> JobDefinition:
        if account_len_hint is None:
            # account len size unknown: make sure we can collect and are reasonable fast
            requires = ComputeResources(cores=4, memory=GiB(5))
            limits = ComputeResources(cores=4, memory=GiB(20))
        elif account_len_hint == 1:
            requires = ComputeResources(cores=1, memory=MiB(512))
            limits = ComputeResources(cores=1, memory=GiB(2))
        elif account_len_hint < 10:
            requires = ComputeResources(cores=2, memory=GiB(3))
            limits = ComputeResources(cores=4, memory=GiB(10))
        else:
            requires = ComputeResources(cores=4, memory=GiB(5))
            limits = None

        coordinator_args = ["--write", "resoto.worker.yaml=WORKER_CONFIG"]
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
            "/home/resoto/resoto.worker.yaml",
        ]
        worker_args: List[str] = []

        return JobDefinition(
            id=job_id,
            name=f"collect-single-{tenant_id}",
            image="someengineering/fix-collect-single:test3",
            args=[*coordinator_args, "---", *core_args, "---", *worker_args],
            requires=requires,
            limits=limits,
            env={"WORKER_CONFIG": worker_config, **(env or {})},
        )


class JobStatus(Enum):
    pending = "pending"
    running = "running"
    wait_for_status = "wait_for_status"
    succeeded = "succeeded"
    failed = "failed"

    def is_done(self) -> bool:
        return self in (JobStatus.succeeded, JobStatus.failed)


@define(eq=True, order=True, repr=True, frozen=True)
class JobReference:
    name: str
    status: JobStatus

    @staticmethod
    def from_job(job: V1Job) -> JobReference:
        js: V1JobStatus = job.status
        if js.start_time is None:
            status = JobStatus.pending
        elif isinstance(js.active, int) and js.active > 0:
            status = JobStatus.running
        elif isinstance(js.succeeded, int) and js.succeeded > 0:
            status = JobStatus.succeeded
        elif isinstance(js.failed, int) and js.failed > 0:
            status = JobStatus.failed
        else:
            status = JobStatus.wait_for_status

        return JobReference(name=job.metadata.name, status=status)


@define(eq=True, order=True, repr=True)
class RunningJob:
    definition: JobDefinition
    ref: JobReference
    future: Future[bool]


class JobCoordinator(Service):
    def __init__(
        self,
        coordinator_id: str,
        redis: ArqRedis,
        publisher: RedisStreamPublisher,
        api_client: ApiClient,
        namespace: str,
        max_parallel: int,
    ) -> None:
        self.coordinator_id = coordinator_id
        self.redis = redis
        self.api_client = api_client
        self.publisher = publisher
        self.batch = k8s.BatchV1Api(api_client)
        self.namespace = namespace
        self.max_parallel = max_parallel
        self.running_jobs: Dict[str, RunningJob] = {}
        self.running_jobs_lock = asyncio.Lock()
        self.job_queue_lock = asyncio.Lock()
        self.job_queue: List[Tuple[JobDefinition, Future[bool]]] = []
        self.worker: Optional[Worker] = None
        self.redis_worker_task: Optional[asyncio.Task[Any]] = None
        self.watcher: Optional[asyncio.Task[Any]] = None

    async def start_job(self, definition: JobDefinition) -> Future[bool]:
        async with self.job_queue_lock:
            result: Future[bool] = Future()
            self.job_queue.append((definition, result))
        await self.__schedule_next()
        return result

    def __running_job_unsafe(self, v: V1Job) -> RunningJob:
        # note: no lock used here on purpose: caller should acquire the lock
        ref = JobReference.from_job(v)
        if (rj := self.running_jobs.get(ref.name)) is not None:
            return rj
        else:
            return RunningJob(definition=JobDefinition.from_job(v), ref=ref, future=Future())

    async def __schedule_next(self) -> None:
        async with self.job_queue_lock:
            async with self.running_jobs_lock:
                split_at = max(self.max_parallel - len(self.running_jobs), 0)
                to_schedule = self.job_queue[:split_at]
                self.job_queue = self.job_queue[split_at:]
                for job, future in to_schedule:
                    log.info(f"Scheduling job {job.name}")
                    await self.__schedule_job_unsafe(job, future)

    async def __schedule_job_unsafe(self, definition: JobDefinition, result: Future[bool]) -> JobReference:
        # note: no lock used here on purpose: caller should acquire the lock
        uname = definition.name + "-" + definition.id
        pod_template = V1PodTemplateSpec(
            spec=V1PodSpec(
                restart_policy=definition.restart_policy,
                containers=[
                    V1Container(
                        name=definition.name,
                        env=[V1EnvVar(name=k, value=v) for k, v in (definition.env or {}).items()],
                        image=definition.image,
                        image_pull_policy="Always",
                        args=definition.args,
                        resources=V1ResourceRequirements(
                            requests=definition.requires.pod_spec() if definition.requires else None,
                            limits=definition.limits.pod_spec() if definition.limits else None,
                        ),
                    )
                ],
            ),
        )
        job = V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=V1ObjectMeta(
                name=uname,
                labels={"app": "collect-coordinator", "coordinator-id": self.coordinator_id, "job-id": definition.id},
            ),
            spec=V1JobSpec(
                backoff_limit=0,
                template=pod_template,
                ttl_seconds_after_finished=600,  # Safeguard. We will delete the job manually.
            ),
        )
        await self.batch.create_namespaced_job(namespace=self.namespace, body=job)
        ref = JobReference(name=uname, status=JobStatus.pending)
        self.running_jobs[uname] = RunningJob(definition=definition, ref=ref, future=result)
        return ref

    async def __mark_job_done(self, job: RunningJob, error_message: Optional[str] = None) -> None:
        # note: no lock used here on purpose: caller should acquire the lock
        if not job.future.done():
            success = error_message is None
            message = dict(job_id=job.definition.id, success=success)
            if success:
                job.future.set_result(True)
            else:
                job.future.set_exception(RuntimeError("Job failed!"))
                message["error"] = error_message
            await self.publisher.publish("job_done", message)

    async def __reconcile(self) -> None:
        res = await self.batch.list_namespaced_job(
            namespace=self.namespace, label_selector=f"app=collect-coordinator,coordinator-id={self.coordinator_id}"
        )
        running_jobs = {}
        for item in res.items:
            definition = JobDefinition.from_job(item)
            ref = JobReference.from_job(item)
            running_jobs[ref.name] = RunningJob(definition=definition, ref=ref, future=Future())
        async with self.running_jobs_lock:
            self.running_jobs = running_jobs

    async def __clean_done_jobs(self) -> None:
        # Select all jobs in namespace and delete succeeded ones
        res = await self.batch.list_namespaced_job(
            namespace=self.namespace, label_selector=f"app=collect-coordinator,coordinator-id={self.coordinator_id}"
        )
        for item in res.items:
            ref = JobReference.from_job(item)
            if ref.status.is_done():
                await self.__delete_job(ref)
                self.running_jobs.pop(ref.name, None)

    async def __watch_jobs(self) -> None:
        watch = Watch()
        async for event in watch.stream(
            self.batch.list_namespaced_job,
            namespace=self.namespace,
            label_selector=f"app=collect-coordinator,coordinator-id={self.coordinator_id}",
        ):
            try:
                change_type = event["type"]  # ADDED, MODIFIED, DELETED
                job = event["object"]
                name = job.metadata.name
                ref = JobReference.from_job(job)
                async with self.running_jobs_lock:
                    does_not_exist = name not in self.running_jobs
                    running_job = self.__running_job_unsafe(job)
                    changed = does_not_exist or running_job.ref != ref
                    if change_type in ("ADDED", "MODIFIED"):
                        self.running_jobs[name] = evolve(running_job, ref=ref)
                    elif change_type == "DELETED":
                        log.info(f"Job marked as deleted. Remove {name}.")
                        # if we come here and the future is not done, mark it as failed
                        await self.__mark_job_done(running_job, "Job failed!")
                        # remove all traces from the running job
                        self.running_jobs.pop(name, None)
                    else:
                        raise ValueError(f"Unknown change type: {change_type}")
                if changed:
                    log.info(f"Job {name} change_type: {change_type} changed: {ref}")
                    # we delete the job as soon as it succeeds, do not wait until ttlSecondsAfterFinished
                    if change_type == "MODIFIED" and ref.status.is_done():
                        log.info(f"Job {name} is done. Deleting it.")
                        error = f"Job {name} failed!" if ref.status == JobStatus.failed else None
                        await self.__mark_job_done(running_job, error)
                        with suppress(ApiException):
                            # Job succeeded, delete it. Can throw an error in case it is already deleted
                            await self.__delete_job(ref)
                # if one job is deleted, check if we can schedule a new one
                if change_type == "DELETED":
                    await self.__schedule_next()
            except Exception as ex:
                log.exception(f"Error while processing job event: {ex}")

    async def __delete_job(self, ref: JobReference) -> None:
        await self.batch.delete_namespaced_job(name=ref.name, namespace=self.namespace, propagation_policy="Foreground")

    async def start(self) -> Any:
        await self.__clean_done_jobs()
        await self.__reconcile()
        await self.publisher.start()
        self.watcher = asyncio.create_task(self.__watch_jobs())

        async def collect(ctx: Dict[Any, Any], *args: Any, **kwargs: Any) -> bool:
            log.debug(f"Collect function called with ctx: {ctx}, args: {args}, kwargs: {kwargs}")
            job_id: str = ctx["job_id"]
            # TODO: value will be encrypted. We would expect a string: decrypt and parse json.
            if len(args) == 1 and isinstance(args[0], dict):
                data = args[0]
                data["job_id"] = job_id
                jd = JobDefinition.collect_definition_json(data)
                log.info(f"Received collect job {jd.name}")
                future = await self.start_job(jd)
                log.info(f"Collect job {jd.name} started. Wait for the job to finish.")
                return await future
            else:
                message = f"Invalid arguments for collect function. Got {args}. Expect one arg of type Dict[str, Any]."
                log.error(message)
                raise ValueError(message)

        worker = Worker(
            functions=[
                Function(
                    name="collect",
                    coroutine=collect,
                    timeout_s=3600,
                    keep_result_s=180,
                    keep_result_forever=False,
                    max_tries=1,
                )
            ],
            job_timeout=3600,  # 1 hour
            redis_pool=self.redis,
            max_jobs=self.max_parallel,
            keep_result=180,
            retry_jobs=False,
            handle_signals=False,
            log_results=False,
        )
        self.worker = worker
        self.redis_worker_task = asyncio.create_task(worker.async_run())

    async def stop(self) -> None:
        if self.worker:
            await self.worker.close()
            self.worker = None
        if self.watcher and not self.watcher.done():
            self.watcher.cancel()
            with suppress(asyncio.CancelledError):
                await self.watcher
            self.watcher = None
        if self.redis_worker_task and not self.redis_worker_task.done():
            self.redis_worker_task.cancel()
            with suppress(asyncio.CancelledError):
                await self.redis_worker_task
            self.redis_worker_task = None
        for name, running_job in self.running_jobs.items():
            # The process stops and the job is still running.
            # Choices:
            # a) Stop insertion of jobs and wait for all jobs to be done.
            #    -> Could take a long time until the service can be restarted.
            #    -> Does not help in case the pod is killed the hard way.
            # B) Persist the in-progress state somehow and continue on restart.
            #    -> Needs maintaining in-progress state.
            #    -> Unclear how to interface with arq, since the API is call-based.
            # C) Mark the job as failed.
            #    -> Even if the job might be successful - we do not know.
            #    -> Let the caller create a new job, so we collect the data again.
            # For simplicity, we choose c) for now.
            log.info(f"Tear down. Job is still running. Mark job {name} as failed.")
            await self.__mark_job_done(running_job, "Coordinator stopped. Job is still running.")
        await self.publisher.stop()
