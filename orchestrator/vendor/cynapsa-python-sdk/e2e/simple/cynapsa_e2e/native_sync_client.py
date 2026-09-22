from __future__ import annotations

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
    return result("native-sync", passed, failures)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"E2E_FATAL={type(exc).__name__}:{exc}", file=sys.stderr, flush=True)
        raise
