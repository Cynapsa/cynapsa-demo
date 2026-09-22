"""Minimal Cynapsa Python SDK flow."""

from __future__ import annotations

import sys

from cynapsa import CynapsaClient


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: python3 examples/basic.py /absolute/path/to/libcynapsacore")

    with CynapsaClient(sys.argv[1]) as client:
        client.start()
        admission = client.initialize()
        completion = client.next_completion(timeout_ms=5_000)
        status = client.status()

        print(f"accepted={admission.accepted} command_id={admission.command_id}")
        print(f"completion_ok={completion.ok if completion else False}")
        print(f"lifecycle={status.lifecycle}")


if __name__ == "__main__":
    main()
