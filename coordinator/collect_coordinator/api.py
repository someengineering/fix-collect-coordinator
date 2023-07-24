from aiohttp import web
from cattr import unstructure

from collect_coordinator.job_coordinator import JobCoordinator, JobDefinition
from collect_coordinator.service import Service, Dependencies


class Api(Service):
    def __init__(self, deps: Dependencies):
        self.deps = deps
        self.app = web.Application()
        self.app.add_routes([web.get("/ping", self.ping), web.get("/run", self.run), web.get("/status", self.status)])

    async def ping(self, request: web.Request) -> web.Response:
        return web.Response(text="pong")

    async def run(self, request: web.Request) -> web.Response:
        coordinator = self.deps.service("job_coordinator", JobCoordinator)
        name = request.query.get("name", "someengineering")

        definition = JobDefinition.collect_definition(
            name,
            "http://db-0.dbs.fix.svc.cluster.local:8529",
            "db3",
            "resotoworker:\n  collector:\n     - 'aws'",
            {
                "AWS_ACCESS_KEY_ID": "",
                "AWS_SECRET_ACCESS_KEY": "",
                "AWS_SESSION_TOKEN": "",
            },
            1,
        )
        await coordinator.start_job(definition)
        return web.Response(text="ok")

    async def status(self, request: web.Request) -> web.Response:
        coordinator = self.deps.service("job_coordinator", JobCoordinator)
        return web.json_response({"queue": unstructure(coordinator.job_queue)})
