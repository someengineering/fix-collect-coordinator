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

from uuid import uuid4

from aiohttp import web
from cattr import unstructure
from fixcloudutils.service import Service

from collect_coordinator.job_coordinator import JobCoordinator, JobDefinition


class Api(Service):
    def __init__(self, app: web.Application, coordinator: JobCoordinator) -> None:
        app.add_routes([web.get("/ping", self.ping), web.get("/run", self.run), web.get("/status", self.status)])
        self.app = app
        self.coordinator = coordinator

    async def ping(self, _: web.Request) -> web.Response:
        return web.Response(text="pong")

    async def run(self, request: web.Request) -> web.Response:
        name = request.query.get("name", "someengineering")

        definition = JobDefinition.collect_definition(
            str(uuid4()),
            name,
            "http://db-0.dbs.fix.svc.cluster.local:8529",
            "db3",
            "resoto",
            "",
            "resotoworker:\n  collector:\n     - 'aws'",
            {
                "AWS_ACCESS_KEY_ID": "",
                "AWS_SECRET_ACCESS_KEY": "",
                "AWS_SESSION_TOKEN": "",
            },
            1,
        )
        await self.coordinator.start_job(definition)
        return web.Response(text="ok")

    async def status(self, _: web.Request) -> web.Response:
        return web.json_response({"queue": unstructure(self.coordinator.job_queue)})
