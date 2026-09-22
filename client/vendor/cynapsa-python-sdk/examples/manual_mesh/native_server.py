from __future__ import annotations

import json
import threading

import cynapsa

from .common import connection_options


def response(text: str) -> cynapsa.CynapsaResponse:
    body = json.dumps([text[::-1], "native"], separators=(",", ":")).encode()
    return cynapsa.CynapsaResponse(
        status_code=200,
        reason="OK",
        headers=(("content-type", "application/json"),),
        body=body,
    )


def main() -> None:
    with cynapsa.connect(**connection_options()) as session:
        @session.on("/reverse")
        def reverse(request: cynapsa.CynapsaRequest) -> cynapsa.CynapsaResponse:
            text = request.text()
            print(f"native server received: {text!r}", flush=True)
            return response(text)

        print(f"native server ready as {session.agent_id}", flush=True)
        try:
            threading.Event().wait()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
