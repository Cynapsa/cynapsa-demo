#!/usr/bin/env python3
"""Record an exact digest manifest for the Core source used by the E2E build."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import sys
from pathlib import Path


def _git(root: Path, *args: str) -> bytes:
    return subprocess.run(
        ["git", "-C", os.fspath(root), *args],
        check=True,
        stdout=subprocess.PIPE,
    ).stdout


def build_manifest(root: Path) -> dict[str, object]:
    root = root.resolve(strict=True)
    names = _git(
        root, "ls-files", "-z", "--cached", "--others", "--exclude-standard"
    ).split(b"\0")
    files: list[dict[str, object]] = []
    for encoded in sorted(name for name in names if name):
        relative = encoded.decode("utf-8", errors="surrogateescape")
        path = root / relative
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            files.append({"path": relative, "kind": "deleted"})
            continue
        if stat.S_ISLNK(metadata.st_mode):
            data = os.readlink(path).encode("utf-8", errors="surrogateescape")
            kind = "symlink"
        elif stat.S_ISREG(metadata.st_mode):
            data = path.read_bytes()
            kind = "file"
        else:
            raise RuntimeError(f"unsupported Core source entry: {relative}")
        files.append(
            {
                "path": relative,
                "kind": kind,
                "mode": stat.S_IMODE(metadata.st_mode),
                "bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            }
        )
    file_bytes = json.dumps(
        files, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return {
        "schema_version": 1,
        "repository": "https://github.com/Cynapsa/cynapsagocore",
        "head": _git(root, "rev-parse", "HEAD").decode().strip(),
        "branch": _git(root, "branch", "--show-current").decode().strip(),
        "status_porcelain_v1": _git(
            root, "status", "--porcelain=v1", "--untracked-files=all"
        ).decode("utf-8", errors="surrogateescape").splitlines(),
        "source_manifest_sha256": hashlib.sha256(file_bytes).hexdigest(),
        "files": files,
    }


def main() -> int:
    if len(sys.argv) != 3:
        raise SystemExit("usage: record_core_provenance.py CORE_CHECKOUT OUTPUT")
    manifest = build_manifest(Path(sys.argv[1]))
    Path(sys.argv[2]).write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
