from __future__ import annotations

import sys
import time

import cynapsa

from .client_helpers import expect, failure_fields, native_http_request
from .settings import (
    MONKEY_SERVER,
    NATIVE_SERVER,
    ROUTE,
    allow_mesh_traffic,
    auth,
    diagnostic,
    required,
)


def main() -> int:
    """Run a short, caller-selected native/monkey sequence with 10s bounds."""

    label = required("CYNAPSA_E2E_SEQUENCE_LABEL")
    targets = tuple(
        target.strip()
        for target in required("CYNAPSA_E2E_SEQUENCE").split(",")
        if target.strip()
    )
    if not targets or any(target not in {"native", "monkey"} for target in targets):
        raise RuntimeError("CYNAPSA_E2E_SEQUENCE must contain native/monkey targets")

    index = 0
    with cynapsa.connect(**auth(timeout_ms=10_000)) as session:
        allow_mesh_traffic(session._owner)
        for target in targets:
            text = f"{label}-{index}"
            started = time.monotonic()
            diagnostic(
                "sequence-request-start",
                label=label,
                index=index,
                target=target,
                message=text,
            )
            try:
                if target == "native":
                    response = session.request(NATIVE_SERVER, text, path=ROUTE, ttl_ms=10_000)
                else:
                    response = session.request(
                        MONKEY_SERVER,
                        native_http_request(text),
                        ttl_ms=10_000,
                    )
                expect(response.json(), text, target)
            except Exception as exc:
                diagnostic(
                    "sequence-request-fail",
                    label=label,
                    index=index,
                    target=target,
                    elapsed_ms=round((time.monotonic() - started) * 1000),
                    **failure_fields(exc),
                )
                raise
            diagnostic(
                "sequence-request-pass",
                label=label,
                index=index,
                target=target,
                elapsed_ms=round((time.monotonic() - started) * 1000),
            )
            index += 1
    diagnostic("sequence-pass", label=label, requests=len(targets))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"E2E_FATAL={type(exc).__name__}:{exc}", file=sys.stderr, flush=True)
        raise
