from __future__ import annotations

import asyncio

import cynapsa

from .common import connection_options, required
from .native_sync_client import http_request


async def read_message() -> str | None:
    try:
        text = await asyncio.to_thread(input, 'message (or "quit"): ')
    except (EOFError, KeyboardInterrupt):
        print()
        return None
    return None if text.strip().lower() == "quit" else text


async def run() -> None:
    session = await cynapsa.connect_async(**connection_options())
    async with session:
        while (text := await read_message()) is not None:
            targets = (
                ("native", required("CYNAPSA_NATIVE_SERVER_AGENT_ID"), text),
                ("fastapi", required("CYNAPSA_FASTAPI_SERVER_AGENT_ID"), http_request(text)),
            )
            for name, recipient, payload in targets:
                try:
                    if isinstance(payload, cynapsa.CynapsaRequest):
                        response = await session.request(recipient, payload)
                    else:
                        response = await session.request(recipient, payload, path="/reverse")
                    print(f"{name} response: {response.json()}", flush=True)
                except Exception as error:
                    print(f"{name} request failed: {error}", flush=True)


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
