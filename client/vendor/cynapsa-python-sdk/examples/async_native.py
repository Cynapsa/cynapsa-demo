"""Asynchronous native messaging."""

import asyncio
import os

import cynapsa


async def main() -> None:
    session = await cynapsa.connect_async(
        mesh_id=os.environ.get("CYNAPSA_MESH_ID", "mesh-one"),
        profile_id=os.environ.get("CYNAPSA_PROFILE_ID", "default"),
        enrollment_token=os.environ.get("CYNAPSA_ENROLLMENT_TOKEN"),
    )
    async with session:
        @session.on("/ping")
        async def ping(_: cynapsa.CynapsaRequest) -> object:
            return {"pong": True}

        result = await session.send("worker@example.test", b"ready", path="/ready")
        print(result.accepted_by_core)
        response = await session.request(
            "worker@example.test", "ping", path="/ping", ttl_ms=5_000
        )
        print(response.text())


asyncio.run(main())
