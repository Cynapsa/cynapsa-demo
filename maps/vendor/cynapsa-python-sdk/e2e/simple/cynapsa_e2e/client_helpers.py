from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable

import cynapsa

from .settings import (
    MONKEY_ORIGIN,
    MONKEY_SERVER,
    NATIVE_ORIGIN,
    NATIVE_SERVER,
    ROUTE,
    diagnostic,
)

MESSAGES = tuple(f"message-{index}" for index in range(10))


def native_http_request(text: str) -> cynapsa.CynapsaRequest:
    return cynapsa.CynapsaRequest(
        "POST",
        ROUTE,
        "",
        (("content-type", "text/plain; charset=utf-8"),),
        text.encode("utf-8"),
    )


def address_map() -> dict[str, dict[str, str]]:
    return {
        NATIVE_ORIGIN: {"recipient": NATIVE_SERVER, "mode": "rpc"},
        MONKEY_ORIGIN: {"recipient": MONKEY_SERVER, "mode": "rpc"},
    }


def expect(value: object, text: str, tag: str) -> None:
    expected = [text[::-1], tag]
    if value != expected:
        raise AssertionError(f"expected {expected!r}, received {value!r}")


def failure_fields(error: Exception) -> dict[str, object]:
    fields: dict[str, object] = {
        "error_type": type(error).__name__,
        "error": str(error),
    }
    if isinstance(error, cynapsa.NativeError):
        fields.update(
            status=error.status,
            code=error.code,
            details=dict(error.details),
        )
    return fields


def result(client: str, passed: int, failures: list[str]) -> int:
    report = {
        "client": client,
        "passed": passed,
        "failed": len(failures),
        "expected": 20,
        "failures": failures,
    }
    print("E2E_RESULT=" + json.dumps(report, sort_keys=True), flush=True)
    return 0 if passed == 20 and not failures else 1


def run_sync_target(
    client: str,
    tag: str,
    check: Callable[[str], None],
    passed: int,
    failures: list[str],
) -> int:
    for text in MESSAGES:
        started = time.monotonic()
        diagnostic("request-start", client=client, target=tag, message=text)
        try:
            check(text)
            passed += 1
            diagnostic(
                "request-pass",
                client=client,
                target=tag,
                message=text,
                elapsed_ms=round((time.monotonic() - started) * 1000),
            )
        except Exception as exc:
            failures.append(f"{tag}:{text}:{type(exc).__name__}:{exc}")
            diagnostic(
                "request-fail",
                client=client,
                target=tag,
                message=text,
                elapsed_ms=round((time.monotonic() - started) * 1000),
                **failure_fields(exc),
            )
    return passed

async def run_async_target(
    client: str,
    tag: str,
    check: Callable[[str], Awaitable[None]],
    passed: int,
    failures: list[str],
    on_failure: Callable[[], None] | None = None,
) -> int:
    for index, text in enumerate(MESSAGES):
        if index > 0:
            await asyncio.sleep(2.0)
        started = time.monotonic()
        diagnostic("request-start", client=client, target=tag, message=text)
        try:
            await check(text)
            passed += 1
            diagnostic(
                "request-pass",
                client=client,
                target=tag,
                message=text,
                elapsed_ms=round((time.monotonic() - started) * 1000),
            )
        except Exception as exc:
            failures.append(f"{tag}:{text}:{type(exc).__name__}:{exc}")
            diagnostic(
                "request-fail",
                client=client,
                target=tag,
                message=text,
                elapsed_ms=round((time.monotonic() - started) * 1000),
                **failure_fields(exc),
            )
            if on_failure is not None:
                on_failure()
                on_failure = None
    return passed
