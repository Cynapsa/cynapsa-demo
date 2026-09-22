from __future__ import annotations

import json
import sys
import time

import requests

MESSAGES = tuple(f"message-{index}" for index in range(10))
TARGETS = (
    ("native", "https://native-server.test"),
    ("monkey", "https://monkey-server.test"),
)


def diagnostic(event: str, **fields: object) -> None:
    print(
        "E2E_DIAGNOSTIC=" + json.dumps({"event": event, **fields}, sort_keys=True),
        flush=True,
    )


def main() -> int:
    passed = 0
    failures: list[str] = []
    for tag, origin in TARGETS:
        for text in MESSAGES:
            started = time.monotonic()
            diagnostic(
                "request-start", client="monkey-sync", target=tag, message=text
            )
            try:
                response = requests.post(
                    origin + "/reverse",
                    data=text.encode("utf-8"),
                    headers={"content-type": "text/plain; charset=utf-8"},
                    timeout=30,
                )
                if type(response) is not requests.Response:
                    raise AssertionError(
                        f"expected requests.Response, got {type(response)!r}"
                    )
                response.raise_for_status()
                expected = [text[::-1], tag]
                received = response.json()
                if received != expected:
                    raise AssertionError(
                        f"expected {expected!r}, received {received!r}"
                    )
                passed += 1
                diagnostic(
                    "request-pass",
                    client="monkey-sync",
                    target=tag,
                    message=text,
                    elapsed_ms=round((time.monotonic() - started) * 1000),
                )
            except Exception as exc:
                failures.append(f"{tag}:{text}:{type(exc).__name__}:{exc}")
                diagnostic(
                    "request-fail",
                    client="monkey-sync",
                    target=tag,
                    message=text,
                    elapsed_ms=round((time.monotonic() - started) * 1000),
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
    report = {
        "client": "monkey-sync",
        "passed": passed,
        "failed": len(failures),
        "expected": 20,
        "failures": failures,
    }
    print("E2E_RESULT=" + json.dumps(report, sort_keys=True), flush=True)
    return 0 if passed == 20 and not failures else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"E2E_FATAL={type(exc).__name__}:{exc}", file=sys.stderr, flush=True)
        raise
