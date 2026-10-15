from __future__ import annotations

import os
import stat
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PRIVATE = Path(os.environ.get("DEMO_PRIVATE_DIRECTORY", ROOT / ".private"))


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


def litellm_key() -> str:
    return _secure_file("litellm_api_key")


def google_maps_key() -> str:
    return _secure_file("google_maps_api_key")
