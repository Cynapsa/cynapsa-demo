from __future__ import annotations

import os
import stat
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PRIVATE = Path(os.environ.get("DEMO_PRIVATE_DIRECTORY", ROOT / ".private"))
STATE = Path(os.environ.get("CYNAPSA_STATE_DIRECTORY", ROOT / ".state"))
CORE_LIBRARY = Path(os.environ.get("CYNAPSA_CORE_LIBRARY", ROOT / "libcynapsacore.so"))


def _secure_file(name: str) -> str:
    directory = PRIVATE.lstat()
    if not stat.S_ISDIR(directory.st_mode) or directory.st_uid != os.getuid():
        raise RuntimeError("private directory must be owned by the current user")
    if stat.S_IMODE(directory.st_mode) & 0o077:
        raise RuntimeError("private directory must have mode 0700")
    path = PRIVATE / name
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
        raise RuntimeError(f"{name} must be a regular file owned by the current user")
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise RuntimeError(f"{name} must have mode 0600")
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise RuntimeError(f"{name} is empty")
    return value


def _required_environment(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def prepare_runtime() -> None:
    info = STATE.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise RuntimeError("state directory must be owned by the current user")
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise RuntimeError("state directory must have mode 0700")
    if not CORE_LIBRARY.is_file():
        raise RuntimeError(f"Go Core shared library not found at {CORE_LIBRARY}")
    os.environ["CYNAPSA_CORE_LIBRARY"] = str(CORE_LIBRARY.resolve())
    os.environ["CYNAPSA_STATE_DIRECTORY"] = str(STATE.resolve())


def gemini_key() -> str:
    return _secure_file("gemini_api_key")


def google_maps_key() -> str:
    return _secure_file("google_maps_api_key")


def connection_options(*, enroll: bool) -> dict[str, str | int]:
    mesh_id = _required_environment("DEMO_MESH_ID")
    options: dict[str, str | int] = {
        "mesh_id": mesh_id,
        "profile_id": "demo-maps",
        "rpc_timeout_ms": 110_000,
    }
    if enroll:
        options["enrollment_token"] = _secure_file("enrollment_token")
    return options
