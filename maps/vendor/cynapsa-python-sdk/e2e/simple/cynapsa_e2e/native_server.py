from __future__ import annotations

import json
import sys

import cynapsa

from .settings import (
    MONKEY_SERVER,
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

        def upstream(request: cynapsa.CynapsaRequest) -> cynapsa.CynapsaResponse:
            diagnostic("hop-intermediate", path=request.path)
            return session.request(
                MONKEY_SERVER,
                cynapsa.CynapsaRequest(
                    "POST", "/rate-limit", body=b"hop-probe",
                ),
            )

        @session.on("/forward")
        def forward(request: cynapsa.CynapsaRequest) -> cynapsa.CynapsaResponse:
            try:
                return upstream(request)
            except cynapsa.RemoteNativeError as error:
                return error.response

        @session.on("/remap")
        def remap(request: cynapsa.CynapsaRequest) -> cynapsa.CynapsaResponse:
            try:
                return upstream(request)
            except cynapsa.RemoteNativeError as error:
                raise cynapsa.RPCException(
                    503, code="upstream_limited", detail="Map service is busy",
                    headers=(("retry-after", "7"),),
                ) from error

        @session.on("/unhandled")
        def unhandled(request: cynapsa.CynapsaRequest) -> cynapsa.CynapsaResponse:
            return upstream(request)

        mark_ready()
        wait_for_stop()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"E2E_FATAL={type(exc).__name__}:{exc}", file=sys.stderr, flush=True)
        raise
