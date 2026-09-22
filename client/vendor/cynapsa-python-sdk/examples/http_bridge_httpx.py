"""Selective asynchronous HTTP Bridge use with HTTPX."""

import asyncio
import os

import httpx

import cynapsa


async def main() -> None:
    bridge = await cynapsa.login_async(
        mesh_id=os.environ.get("CYNAPSA_MESH_ID", "mesh-one"),
        profile_id=os.environ.get("CYNAPSA_PROFILE_ID", "default"),
        enrollment_token=os.environ.get("CYNAPSA_ENROLLMENT_TOKEN"),
        address_map={
            "https://orders.example.test": {
                "recipient": "orders@example.test",
                "mode": "rpc",
            }
        },
    )
    async with bridge:
        async with httpx.AsyncClient() as client:
            response = await client.get("https://orders.example.test/orders/42")
            response.raise_for_status()
            print(response.json())


asyncio.run(main())
