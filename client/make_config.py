"""Write the client's non-secret Cynapsa CLI config for its HTTP mapping."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def main() -> None:
    destination = Path(sys.argv[1])
    config: dict[str, object] = {
        "schema_version": 1,
        "mesh_id": os.environ["DEMO_MESH_ID"],
        "profile_id": "demo-client",
        "rpc_timeout_ms": 140000,
        "map": [{
            "origin": os.environ["DEMO_ORCHESTRATOR_ORIGIN"],
            "recipient": os.environ["DEMO_ORCHESTRATOR_AGENT_ID"],
            "mode": "rpc",
        }],
    }
    token_path = destination.parent / "enrollment_token"
    if token_path.is_file():
        config["token_file"] = str(token_path)
    if os.environ.get("DEMO_FORCE_ENROLL") == "1":
        config["force_enroll"] = True
    descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump(config, stream)


if __name__ == "__main__":
    main()
