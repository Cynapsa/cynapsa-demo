from __future__ import annotations

import requests


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
    for text in messages():
        for name, origin in (
            ("native", "https://native-server.local"),
            ("fastapi", "https://fastapi-server.local"),
        ):
            try:
                response = requests.post(
                    f"{origin}/reverse",
                    data=text.encode(),
                    headers={"content-type": "text/plain; charset=utf-8"},
                    timeout=30,
                )
                response.raise_for_status()
                print(f"{name} response: {response.json()}", flush=True)
            except requests.RequestException as error:
                print(f"{name} request failed: {error}", flush=True)


if __name__ == "__main__":
    main()
