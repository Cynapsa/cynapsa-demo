from __future__ import annotations

import json
import os
import sys

import cynapsa

from .client_helpers import expect, native_http_request, result, run_sync_target
from .settings import MONKEY_SERVER, NATIVE_SERVER, ROUTE, allow_mesh_traffic, auth


def main() -> int:
    passed = 0
    failures: list[str] = []
    with cynapsa.connect(**auth()) as session:
        allow_mesh_traffic(session._owner)
        for tag in ("native", "monkey"):

            def check(text: str) -> None:
                if tag == "native":
                    response = session.request(NATIVE_SERVER, text, path=ROUTE)
                else:
                    response = session.request(MONKEY_SERVER, native_http_request(text))
                expect(response.json(), text, tag)

            passed = run_sync_target("native-sync", tag, check, passed, failures)
        if os.environ.get("E2E_HOP_CHECK") == "1":
            hop_checks: list[str] = []
            for path, expected_status in (
            ("/forward", 429), ("/remap", 503), ("/unhandled", 500)
            ):
                try:
                    session.request(NATIVE_SERVER, cynapsa.CynapsaRequest("GET", path))
                    raise AssertionError(f"{path} unexpectedly succeeded")
                except cynapsa.RemoteNativeError as error:
                    response = error.response
                    assert response.status_code == expected_status, (path, response)
                    if path == "/forward":
                        assert response.json() == {"error": "quota exceeded"}
                        assert ("retry-after", "7") in response.headers
                        assert ("x-upstream", "maps") in response.headers
                    elif path == "/remap":
                        assert error.code == "upstream_limited"
                        assert response.error is not None
                        assert response.error.detail == "Map service is busy"
                    else:
                        assert error.code == "handler_error"
                        assert b"quota exceeded" not in response.body
                    hop_checks.append(path)
            print(
                "E2E_HOP_RESULT=" + json.dumps({"client": "native-sync", "checks": hop_checks}),
                flush=True,
            )
    return result("native-sync", passed, failures)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"E2E_FATAL={type(exc).__name__}:{exc}", file=sys.stderr, flush=True)
        raise
