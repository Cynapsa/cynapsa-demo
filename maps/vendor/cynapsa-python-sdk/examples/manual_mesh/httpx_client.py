from __future__ import annotations

import asyncio

import httpx


async def read_message() -> str | None:
    try:
        text = await asyncio.to_thread(input, 'message (or "quit"): ')
    except (EOFError, KeyboardInterrupt):
        print()
        return None
    return None if text.strip().lower() == "quit" else text


async def run() -> None:
    async with httpx.AsyncClient() as client:
        while (text := await read_message()) is not None:
            for name, origin in (
                ("native", "https://native-server.local"),
                ("fastapi", "https://fastapi-server.local"),
            ):
                try:
                    response = await client.post(
                        f"{origin}/reverse",
                        content=text.encode(),
                        headers={"content-type": "text/plain; charset=utf-8"},
                        timeout=30,
                    )
                    response.raise_for_status()
                    print(f"{name} response: {response.json()}", flush=True)
                except httpx.HTTPError as error:
                    print(f"{name} request failed: {error}", flush=True)


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
