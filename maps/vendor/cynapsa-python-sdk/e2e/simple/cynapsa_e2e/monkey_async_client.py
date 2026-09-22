from __future__ import annotations

import asyncio
import sys

import cynapsa
import httpx

from .client_helpers import address_map, expect, result, run_async_target
from .settings import (
    MONKEY_ORIGIN,
    MONKEY_SERVER,
    NATIVE_ORIGIN,
    NATIVE_SERVER,
    ROUTE,
    allow_mesh_traffic,
    auth,
    configured_rpc_timeout_ms,
    print_core_diagnostics,
)


async def amain() -> int:
    passed = 0
    failures: list[str] = []
    bridge = await cynapsa.login_async(address_map=address_map(), **auth())
    async with bridge:
        allow_mesh_traffic(bridge._bridge.owner)
        for tag in ("native", "monkey"):
            async with httpx.AsyncClient() as client:

                async def check(text: str) -> None:
                    origin = NATIVE_ORIGIN if tag == "native" else MONKEY_ORIGIN
                    response = await client.post(
                        origin + ROUTE,
                        content=text.encode("utf-8"),
                        headers={"content-type": "text/plain; charset=utf-8"},
                        timeout=configured_rpc_timeout_ms() / 1000,
                    )
                    if type(response) is not httpx.Response:
                        raise AssertionError(f"expected httpx.Response, got {type(response)!r}")
                    response.raise_for_status()
                    expect(response.json(), text, tag)

                peer = NATIVE_SERVER if tag == "native" else MONKEY_SERVER
                passed = await run_async_target(
                    "monkey-async",
                    tag,
                    check,
                    passed,
                    failures,
                    on_failure=lambda: print_core_diagnostics(
                        bridge._bridge.owner, peer
                    ),
                )
    return result("monkey-async", passed, failures)


def main() -> int:
    return asyncio.run(amain())


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"E2E_FATAL={type(exc).__name__}:{exc}", file=sys.stderr, flush=True)
        raise
