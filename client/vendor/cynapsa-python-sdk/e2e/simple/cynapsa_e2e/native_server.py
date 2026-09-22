from __future__ import annotations

import json
import sys

import cynapsa

from .settings import (
    ROUTE,
    allow_mesh_traffic,
    auth,
    diagnostic,
    mark_ready,
    print_local_diagnostics,
    wait_for_stop,
)


def _json_response(tag: str, text: str) -> cynapsa.CynapsaResponse:
    return cynapsa.CynapsaResponse(
        200,
        "OK",
        (("content-type", "application/json"),),
        json.dumps([text[::-1], tag], separators=(",", ":")).encode("utf-8"),
    )


def main() -> int:
    with cynapsa.connect(**auth()) as session:
        allow_mesh_traffic(session._owner)
        print_local_diagnostics(session)

        @session.on(ROUTE)
        def reverse(request: cynapsa.CynapsaRequest) -> object:
            diagnostic(
                "native-dispatch",
                payload_type=type(request).__name__,
                body_bytes=len(request.body),
            )
            return _json_response("native", request.text())

        mark_ready()
        wait_for_stop()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"E2E_FATAL={type(exc).__name__}:{exc}", file=sys.stderr, flush=True)
        raise
