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

from aiohttp.web import Request, Response, Application, get, json_response
from cattr import unstructure
from fixcloudutils.service import Service
import prometheus_client

from collect_coordinator.job_coordinator import KubernetesJobCoordinator


class Api(Service):
    def __init__(self, app: Application, coordinator: KubernetesJobCoordinator) -> None:
        app.add_routes([get("/ping", self.ping), get("/status", self.status), get("/metrics", self.metrics)])
        self.app = app
        self.coordinator = coordinator

    async def ping(self, _: Request) -> Response:
        return Response(text="pong")

    async def status(self, _: Request) -> Response:
        return json_response({"queue": unstructure(self.coordinator.job_queue)})

    async def metrics(self, _: Request) -> Response:
        resp = Response(body=prometheus_client.generate_latest())
        resp.content_type = prometheus_client.CONTENT_TYPE_LATEST
        return resp
