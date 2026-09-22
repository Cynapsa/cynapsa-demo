from __future__ import annotations

import asyncio
import sys

import cynapsa

from .client_helpers import expect, native_http_request, result, run_async_target
from .settings import MONKEY_SERVER, NATIVE_SERVER, ROUTE, allow_mesh_traffic, auth


async def amain() -> int:
    passed = 0
    failures: list[str] = []
    session = await cynapsa.connect_async(**auth())
    async with session:
        allow_mesh_traffic(session._owner)
        for tag in ("native", "monkey"):

            async def check(text: str) -> None:
                if tag == "native":
                    response = await session.request(NATIVE_SERVER, text, path=ROUTE)
                else:
                    response = await session.request(MONKEY_SERVER, native_http_request(text))
                expect(response.json(), text, tag)

            passed = await run_async_target("native-async", tag, check, passed, failures)
    return result("native-async", passed, failures)


def main() -> int:
    return asyncio.run(amain())


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"E2E_FATAL={type(exc).__name__}:{exc}", file=sys.stderr, flush=True)
        raise
