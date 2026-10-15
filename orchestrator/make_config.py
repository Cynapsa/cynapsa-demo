"""Write the non-secret Cynapsa CLI config for this Orchestrator installation."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def main() -> None:
    destination = Path(sys.argv[1])
    maps_origin = os.environ.get("DEMO_MAPS_ORIGIN") or "http://demo-maps.local"
    files_origin = os.environ.get("DEMO_FILES_ORIGIN") or "http://demo-files.local"
    mappings = [{
        "origin": maps_origin,
        "recipient": os.environ["DEMO_MAPS_AGENT_ID"],
        "mode": "rpc",
    }]
    if os.environ.get("DEMO_FILES_AGENT_ID"):
        mappings.append({
            "origin": files_origin,
            "recipient": os.environ["DEMO_FILES_AGENT_ID"],
            "mode": "rpc",
        })
        os.environ["DEMO_FILES_ORIGIN"] = files_origin
    else:
        os.environ.pop("DEMO_FILES_ORIGIN", None)
    config: dict[str, object] = {
        "schema_version": 1,
        "mesh_id": os.environ["DEMO_MESH_ID"],
        "profile_id": "demo-orchestrator",
        "rpc_timeout_ms": 110000,
        "map": mappings,
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
