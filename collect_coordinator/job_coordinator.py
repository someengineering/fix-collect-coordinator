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

from __future__ import annotations

import asyncio
import logging
from abc import abstractmethod, ABC
from asyncio import Future
from contextlib import suppress
from enum import Enum
from typing import Dict, Any, Optional, List, Tuple

from arq.connections import ArqRedis
from attr import evolve
from attrs import define
from bitmath import Byte
from fixcloudutils.asyncio import stop_running_task
from fixcloudutils.asyncio.timed import timed
from fixcloudutils.redis.event_stream import RedisStreamPublisher
from fixcloudutils.service import Service
from fixcloudutils.types import Json
from kubernetes_asyncio import client as k8s
from kubernetes_asyncio.client import (
    V1Volume,
    V1Job,
    V1JobStatus,
    ApiException,
    V1ObjectMeta,
    V1JobSpec,
    V1PodTemplateSpec,
    V1PodSpec,
    V1Container,
    V1ResourceRequirements,
    V1SecretVolumeSource,
    V1EnvVar,
    V1Toleration,
    V1VolumeMount,
)
from kubernetes_asyncio.client.api_client import ApiClient
from kubernetes_asyncio.watch import Watch
from prometheus_client import Counter
from redis.asyncio import Redis

log = logging.getLogger("collect.coordinator")

JobRuns = Counter("coordinator_job_runs", "Number of job runs", ["coordinator_id", "image", "success"])


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
            id=job.metadata.annotations.get("job-id", "n/a"),
            name=job.metadata.name,
            image=job.spec.template.spec.containers[0].image,
            args=job.spec.template.spec.containers[0].args,
            restart_policy=job.spec.template.spec.restart_policy,
            env={e.name: e.value for e in (job.spec.template.spec.containers[0].env or [])},
        )


@define(eq=True, order=True, repr=True)
class RunningJob:
    definition: JobDefinition
    ref: JobReference
    future: Future[bool]


class JobCoordinator(Service, ABC):
    @abstractmethod
    async def start_job(self, definition: JobDefinition) -> Future[bool]:
        pass

    @property
    @abstractmethod
    def max_parallel(self) -> int:
        pass


class KubernetesJobCoordinator(JobCoordinator):
    def __init__(
        self,
        coordinator_id: str,
        redis: Redis,
        arq_redis: ArqRedis,
        api_client: ApiClient,
        namespace: str,
        max_parallel: int,
        env: Dict[str, str],
    ) -> None:
        self.coordinator_id = coordinator_id
        self.arq_redis = arq_redis
        self.api_client = api_client
        self.batch = k8s.BatchV1Api(api_client)
        self.namespace = namespace
        self.__max_parallel = max_parallel
        self.env = env
        self.running_jobs: Dict[str, RunningJob] = {}
        self.running_jobs_lock = asyncio.Lock()
        self.job_queue_lock = asyncio.Lock()
        self.job_queue: List[Tuple[JobDefinition, Future[bool]]] = []
        self.watcher: Optional[asyncio.Task[Any]] = None
        self.collect_done_publisher = RedisStreamPublisher(redis, "collect-events", "collect-coordinator")

    async def start_job(self, definition: JobDefinition) -> Future[bool]:
        async with self.job_queue_lock:
            result: Future[bool] = Future()
            self.job_queue.append((definition, result))
        await self.__schedule_next()
        return result

    @property
    def max_parallel(self) -> int:
        return self.__max_parallel

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
                log.info(f"scheduled={len(to_schedule)} running={len(self.running_jobs)} queued={len(self.job_queue)}")

    @timed("collect_coordinator", "schedule_job")
    async def __schedule_job_unsafe(self, definition: JobDefinition, result: Future[bool]) -> JobReference:
        # note: no lock used here on purpose: caller should acquire the lock
        uname = definition.name
        # uname = definition.name + "-" + definition.id
        env = self.env | (definition.env or {})
        pod_template = V1PodTemplateSpec(
            spec=V1PodSpec(
                restart_policy=definition.restart_policy,
                containers=[
                    V1Container(
                        name=definition.name,
                        env=[V1EnvVar(name=k, value=v) for k, v in env.items()],
                        image=definition.image,
                        image_pull_policy="Always",
                        args=definition.args,
                        resources=V1ResourceRequirements(
                            requests=definition.requires.pod_spec() if definition.requires else None,
                            limits=definition.limits.pod_spec() if definition.limits else None,
                        ),
                        # Mount certificates under /etc/ssl/certs
                        volume_mounts=[V1VolumeMount(name="cert-secret", mount_path="/etc/ssl/certs", read_only=True)],
                    )
                ],
                # We want to run on nodes of the jobs pool
                node_selector={"node-role.fixcloud.io": "jobs"},
                # All nodes in that pool are tainted, so no other pod is scheduled. Tolerate this taint.
                tolerations=[
                    V1Toleration(
                        effect="NoSchedule", key="node-role.fixcloud.io/dedicated", operator="Equal", value="jobs"
                    )
                ],
                # Make ssl certificates available to the pod
                volumes=[
                    V1Volume(
                        name="cert-secret", secret=V1SecretVolumeSource(secret_name="fix-collect-coordinator-jobs-cert")
                    )
                ],
            ),
        )
        job = V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=V1ObjectMeta(
                name=uname,
                labels={"app": "collect-coordinator", "type": "job", "coordinator-id": self.coordinator_id},
                annotations={"job-id": definition.id},
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

    async def __mark_future_done(self, job: RunningJob, error_message: Optional[str] = None) -> None:
        # note: no lock used here on purpose: caller should acquire the lock
        if not job.future.done():
            success = error_message is None
            if success:
                job.future.set_result(True)
            else:
                job.future.set_exception(RuntimeError("Job failed!"))
            # increment prometheus counter
            succ_str = "success" if success else "failed"
            JobRuns.labels(coordinator_id=self.coordinator_id, image=job.definition.image, success=succ_str).inc()
            # emit a message
            await self.__done_event(success, job.definition.id, error_message)

    async def __done_event(self, success: bool, job_id: str, error: Optional[str] = None) -> None:
        kind = "job-finished" if success else "job-failed"
        await self.collect_done_publisher.publish(kind, {"job_id": job_id, "error": error})

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
            # all jobs not in the running list are done
            for k, v in self.running_jobs.items():
                if k not in running_jobs:
                    await self.__mark_future_done(v)
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
                job_def = JobDefinition.from_job(item)
                await self.__done_event(ref.status == JobStatus.succeeded, job_def.id)

    async def __watch_jobs_continuously(self) -> None:
        while True:
            try:
                await self.__reconcile()
                await self.__watch_jobs()
            except Exception as ex:
                log.exception(f"Error while watching jobs: {ex}")

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
                job_id = job.metadata.annotations.get("job-id", "n/a")
                log.info(f"Job changed: id={job_id}, name={name}, type={change_type}, status={ref.status}")
                async with self.running_jobs_lock:
                    does_not_exist = name not in self.running_jobs
                    running_job = self.__running_job_unsafe(job)
                    changed = does_not_exist or running_job.ref != ref
                    if change_type in ("ADDED", "MODIFIED"):
                        self.running_jobs[name] = evolve(running_job, ref=ref)
                    elif change_type == "DELETED":
                        log.info(f"Job marked as deleted. Remove {name}.")
                        # if we come here and the future is not done, mark it as failed
                        await self.__mark_future_done(running_job, "Job failed!")
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
                        await self.__mark_future_done(running_job, error)
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
        await self.collect_done_publisher.start()
        await self.__clean_done_jobs()
        await self.__reconcile()
        self.watcher = asyncio.create_task(self.__watch_jobs_continuously())

    async def stop(self) -> None:
        await stop_running_task(self.watcher)
        self.watcher = None
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
            await self.__mark_future_done(running_job, "Coordinator stopped. Job is still running.")
        await self.collect_done_publisher.stop()
