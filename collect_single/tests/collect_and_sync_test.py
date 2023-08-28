import asyncio
from pytest import mark
from resotoclient.async_client import ResotoClient


async def core_client() -> ResotoClient:
    client = ResotoClient("http://localhost:8980")
    while True:
        try:
            await client.ping()
            return client
        except Exception:
            await asyncio.sleep(1)


@mark.asyncio
async def test_client() -> None:
    client = await core_client()
    configs = [
        cfg.split("resoto.report.benchmark.", maxsplit=1)[-1]
        async for cfg in client.cli_execute("configs list")
        if cfg.startswith("resoto.report.benchmark.")
    ]
    print(configs)
