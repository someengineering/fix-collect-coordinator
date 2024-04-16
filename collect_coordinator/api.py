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
import prometheus_client
from aiohttp.web import Request, Response, Application, get, json_response
from cattr import unstructure
from fixcloudutils.service import Service
from kubernetes_asyncio.client.api_client import ApiClient
from kubernetes_asyncio import client as k8s
from redis.asyncio import Redis

from collect_coordinator.job_coordinator import KubernetesJobCoordinator


class Api(Service):
    def __init__(
        self, app: Application, coordinator: KubernetesJobCoordinator, redis: Redis, kubernetes_client: ApiClient
    ) -> None:
        app.add_routes(
            [
                get("/ping", self.ping),
                get("/status", self.status),
                get("/metrics", self.metrics),
                get("/health", self.health),
            ]
        )
        self.app = app
        self.coordinator = coordinator
        self.redis = redis
        self.api_client = kubernetes_client

    async def ping(self, _: Request) -> Response:
        return Response(text="pong")

    async def health(self, _: Request) -> Response:
        await self.redis.ping()  # throws exception in case of error
        await k8s.CoreV1Api(self.api_client).list_namespace(limit=1)  # throws exception in case of error
        return Response(text="ok")

    async def status(self, _: Request) -> Response:
        return json_response({"queue": unstructure(self.coordinator.job_queue)})

    async def metrics(self, _: Request) -> Response:
        resp = Response(body=prometheus_client.generate_latest())
        resp.content_type = prometheus_client.CONTENT_TYPE_LATEST
        return resp
