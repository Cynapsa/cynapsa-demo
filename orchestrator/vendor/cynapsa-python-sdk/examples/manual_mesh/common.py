from __future__ import annotations

import os
import stat
from pathlib import Path


def required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def enrollment_token() -> str | None:
    direct = os.environ.get("CYNAPSA_ENROLLMENT_TOKEN")
    token_file = os.environ.get("CYNAPSA_ENROLLMENT_TOKEN_FILE")
    if direct and token_file:
        raise RuntimeError(
            "set only one of CYNAPSA_ENROLLMENT_TOKEN or "
            "CYNAPSA_ENROLLMENT_TOKEN_FILE"
        )
    if direct:
        return direct
    if not token_file:
        return None

    path = Path(token_file)
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise RuntimeError("CYNAPSA_ENROLLMENT_TOKEN_FILE must be a regular file")
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise RuntimeError(
            "CYNAPSA_ENROLLMENT_TOKEN_FILE must be accessible only by its owner"
        )
    token = path.read_text(encoding="utf-8").strip()
    if not token:
        raise RuntimeError("CYNAPSA_ENROLLMENT_TOKEN_FILE is empty")
    return token


def connection_options() -> dict[str, str]:
    options = {
        "mesh_id": required("CYNAPSA_MESH_ID"),
        "profile_id": os.environ.get("CYNAPSA_PROFILE_ID", "default"),
    }
    token = enrollment_token()
    if token is not None:
        options["enrollment_token"] = token
    return options
