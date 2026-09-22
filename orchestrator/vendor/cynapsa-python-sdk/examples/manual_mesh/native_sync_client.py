from __future__ import annotations

import cynapsa

from .common import connection_options, required


def http_request(text: str) -> cynapsa.CynapsaRequest:
    return cynapsa.CynapsaRequest(
        method="POST",
        path="/reverse",
        query="",
        headers=(("content-type", "text/plain; charset=utf-8"),),
        body=text.encode(),
    )


def messages():
    while True:
        try:
            text = input('message (or "quit"): ')
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if text.strip().lower() == "quit":
            return
        yield text


def main() -> None:
    with cynapsa.connect(**connection_options()) as session:
        for text in messages():
            targets = (
                ("native", required("CYNAPSA_NATIVE_SERVER_AGENT_ID"), text),
                ("fastapi", required("CYNAPSA_FASTAPI_SERVER_AGENT_ID"), http_request(text)),
            )
            for name, recipient, payload in targets:
                try:
                    if isinstance(payload, cynapsa.CynapsaRequest):
                        response = session.request(recipient, payload)
                    else:
                        response = session.request(recipient, payload, path="/reverse")
                    print(f"{name} response: {response.json()}", flush=True)
                except Exception as error:
                    print(f"{name} request failed: {error}", flush=True)


if __name__ == "__main__":
    main()
