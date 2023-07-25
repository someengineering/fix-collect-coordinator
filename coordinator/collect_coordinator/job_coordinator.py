from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from enum import Enum
from typing import Dict, Any, Optional, List

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

from collect_coordinator.service import Service

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
    name: str
    image: str
    args: List[str]
    restart_policy: str = "Never"
    requires: Optional[ComputeResources] = None
    limits: Optional[ComputeResources] = None
    env: Optional[Dict[str, str]] = None

    @classmethod
    def collect_definition(
        cls,
        tenant: str,
        graphdb_server: str,
        graphdb_database: str,
        worker_config: str,
        env: Dict[str, str],
        account_len_hint: Optional[int],
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
            "--override-path",
            "/home/resoto/resoto.worker.yaml",
        ]
        worker_args: List[str] = []

        return JobDefinition(
            name=f"collect-single-{tenant}",
            image="someengineering/fix-collect-single:test3",
            args=[*coordinator_args, "---", *core_args, "---", *worker_args],
            requires=requires,
            limits=limits,
            env={"WORKER_CONFIG": worker_config, **env},
        )


class JobStatus(Enum):
    pending = "pending"
    running = "running"
    wait_for_status = "wait_for_status"
    succeeded = "succeeded"
    failed = "failed"


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


class JobCoordinator(Service):
    def __init__(self, api_client: ApiClient, namespace: str, max_parallel: Optional[int] = None) -> None:
        self.api_client = api_client
        self.batch = k8s.BatchV1Api(api_client)
        self.namespace = namespace
        self.max_parallel = max_parallel
        self.running_jobs: Dict[str, JobReference] = {}
        self.running_jobs_lock = asyncio.Lock()
        self.job_queue_lock = asyncio.Lock()
        self.job_queue: List[JobDefinition] = []
        self.watcher: Optional[asyncio.Task[Any]] = None

    async def start_job(self, definition: JobDefinition) -> None:
        async with self.job_queue_lock:
            self.job_queue.append(definition)
        await self.__schedule_next()

    async def __schedule_next(self) -> None:
        async with self.job_queue_lock:
            async with self.running_jobs_lock:
                to_schedule: Dict[str, JobDefinition] = {}
                for job in self.job_queue:
                    if job.name not in self.running_jobs and job.name not in to_schedule:
                        # job could be schedule, make sure we don't exceed max_parallel
                        if self.max_parallel is not None and (
                            len(self.running_jobs) + len(to_schedule) >= self.max_parallel
                        ):
                            break
                        else:
                            to_schedule[job.name] = job
                for job in to_schedule.values():
                    log.info(f"Scheduling job {job.name}")
                    await self.__schedule_job(job)
                    self.job_queue.remove(job)

    async def __schedule_job(self, definition: JobDefinition) -> JobReference:
        # note: no lock used here on purpose: caller should acquire the lock
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
            metadata=V1ObjectMeta(name=definition.name),
            spec=V1JobSpec(
                template=pod_template,
                ttl_seconds_after_finished=600,  # safeguard. We will delete the job manually.
            ),
        )
        await self.batch.create_namespaced_job(namespace=self.namespace, body=job)
        ref = JobReference(name=definition.name, status=JobStatus.pending)
        self.running_jobs[ref.name] = ref
        return ref

    async def __reconcile(self) -> None:
        res = await self.batch.list_namespaced_job(namespace=self.namespace)
        running_jobs = {}
        for item in res.items:
            ref = JobReference.from_job(item)
            running_jobs[ref.name] = ref
        async with self.running_jobs_lock:
            self.running_jobs = running_jobs

    async def __clean_done_jobs(self) -> None:
        # Select all jobs in namespace and delete succeeded ones
        res = await self.batch.list_namespaced_job(namespace=self.namespace)
        for item in res.items:
            ref = JobReference.from_job(item)
            if ref.status == JobStatus.succeeded:
                await self.__delete_job(ref)
                self.running_jobs.pop(ref.name, None)

    async def __watch_jobs(self) -> None:
        watch = Watch()
        async for event in watch.stream(self.batch.list_namespaced_job, namespace=self.namespace):
            try:
                change_type = event["type"]  # ADDED, MODIFIED, DELETED
                job = event["object"]
                ref = JobReference.from_job(job)
                async with self.running_jobs_lock:
                    existing = self.running_jobs.get(ref.name)
                    changed = not existing or existing != ref
                    if change_type in ("ADDED", "MODIFIED"):
                        self.running_jobs[ref.name] = ref
                    elif change_type == "DELETED":
                        log.info(f"Job marked as deleted. Remove {ref.name}.")
                        self.running_jobs.pop(ref.name, None)
                    else:
                        raise ValueError(f"Unknown change type: {change_type}")
                if changed:
                    log.info(f"Job {ref.name} change_type: {change_type} changed: {ref}")
                # we delete the job as soon as it succeeds, ndo not wait until ttlSecondsAfterFinished
                if change_type == "MODIFIED" and changed and ref.status == JobStatus.succeeded:
                    log.info(f"Job {ref.name} is done. Deleting it.")
                    with suppress(ApiException):
                        # job succeeded, delete it. Can throw an error in case it is already deleted
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
        self.watcher = asyncio.create_task(self.__watch_jobs())

    async def stop(self) -> None:
        if self.watcher:
            self.watcher.cancel()
            with suppress(asyncio.CancelledError):
                await self.watcher
            self.watcher = None
