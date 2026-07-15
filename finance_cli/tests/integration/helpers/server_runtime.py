from __future__ import annotations

import asyncio
from dataclasses import dataclass

import httpx
import uvicorn
from fastapi import FastAPI


@dataclass(slots=True)
class ServerRuntime:
    app: FastAPI
    base_url: str
    port: int
    server: uvicorn.Server
    task: asyncio.Task[None]


async def start_server(app: FastAPI, *, startup_timeout: float = 10.0) -> ServerRuntime:
    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=0,
        lifespan="on",
        log_level="warning",
    )
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())

    loop = asyncio.get_running_loop()
    deadline = loop.time() + startup_timeout
    while not server.started:
        if task.done():
            exc = task.exception()
            if exc is not None:
                raise RuntimeError("uvicorn task exited before startup") from exc
            raise RuntimeError("uvicorn task exited before startup")
        if loop.time() >= deadline:
            raise TimeoutError("Timed out waiting for uvicorn startup")
        await asyncio.sleep(0.05)

    if not server.servers or not server.servers[0].sockets:
        raise RuntimeError("uvicorn started without a bound socket")

    port = int(server.servers[0].sockets[0].getsockname()[1])
    base_url = f"http://127.0.0.1:{port}"

    async with httpx.AsyncClient() as client:
        response = await client.get(f"{base_url}/api/health")
        response.raise_for_status()

    return ServerRuntime(
        app=app,
        base_url=base_url,
        port=port,
        server=server,
        task=task,
    )


async def stop_server(runtime: ServerRuntime, *, shutdown_timeout: float = 10.0) -> None:
    runtime.server.should_exit = True
    await asyncio.wait_for(runtime.task, timeout=shutdown_timeout)
