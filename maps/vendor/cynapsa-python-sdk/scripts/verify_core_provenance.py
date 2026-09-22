#!/usr/bin/env python3
"""Verify vendored Go-core inputs and optionally compare a Go checkout."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

EXPECTED_PATHS = frozenset(
    {
        "cmd/cynapsacore-shared/cynapsacore_v1.h",
        "conformance/v1/README.md",
        "conformance/v1/abi.json",
        "conformance/v1/commands.json",
        "conformance/v1/events.json",
        "conformance/v1/operations.json",
        "conformance/v1/payloads.json",
        "conformance/v1/results.json",
    }
)
_SAFE_COMMITISH = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,255}\Z")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def load_source(project_root: Path) -> dict[str, Any]:
    source_path = project_root / "src" / "cynapsa" / "_core_source.json"
    source = json.loads(source_path.read_text(encoding="utf-8"))
    if (
        type(source) is not dict
        or source.get("schema_version") != 1
        or type(source.get("repository")) is not str
        or not source["repository"]
        or re.fullmatch(r"[0-9a-f]{40}", source.get("go_core_commit", "")) is None
        or type(source.get("sha256")) is not dict
        or set(source["sha256"]) != EXPECTED_PATHS
        or any(re.fullmatch(r"[0-9a-f]{64}", value) is None for value in source["sha256"].values())
    ):
        raise ValueError(f"invalid core provenance manifest: {source_path}")
    return source


def verify_vendor(project_root: Path) -> list[str]:
    source = load_source(project_root)
    vendor = project_root / "src" / "cynapsa" / "_vendor"
    errors: list[str] = []
    for relative, expected in sorted(source["sha256"].items()):
        path = vendor.joinpath(*relative.split("/"))
        if not path.is_file():
            errors.append(f"missing vendored input: {relative}")
            continue
        actual = _sha256(path.read_bytes())
        if actual != expected:
            errors.append(
                f"vendored hash mismatch for {relative}: expected {expected}, got {actual}"
            )
    return errors


def _git_bytes(checkout: Path, commit: str, relative: str) -> bytes:
    if commit == "WORKTREE":
        return checkout.joinpath(*relative.split("/")).read_bytes()
    process = subprocess.run(
        ["git", "show", f"{commit}:{relative}"],
        cwd=checkout,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if process.returncode:
        reason = process.stderr.decode("utf-8", errors="replace").strip()
        raise ValueError(f"cannot read {relative} at {commit}: {reason}")
    return process.stdout


def _snapshot_bytes(checkout: Path, relative: str) -> bytes:
    path = checkout.joinpath(*relative.split("/"))
    if not path.is_file():
        raise ValueError(f"cannot read {relative} from source snapshot: missing file")
    return path.read_bytes()


def compare_checkout(project_root: Path, checkout: Path, commit: str | None = None) -> list[str]:
    source = load_source(project_root)
    checkout = checkout.resolve()
    if not checkout.is_dir():
        return [f"Go core checkout is not a directory: {checkout}"]
    is_git_checkout = (checkout / ".git").exists()
    if not is_git_checkout and commit not in (None, "WORKTREE"):
        return [
            "cannot resolve an explicit commit in a Go core source snapshot "
            f"without git metadata: {checkout}"
        ]
    selected = source["go_core_commit"] if commit is None else commit
    errors: list[str] = []
    if is_git_checkout and selected != "WORKTREE":
        if (
            _SAFE_COMMITISH.fullmatch(selected) is None
            or ".." in selected
            or selected.endswith("/")
        ):
            return [f"unsafe Go core commit-ish: {selected!r}"]
        # ``selected`` has already passed the strict commit-ish allowlist above.
        # Use --verify for compatibility with Git versions where rev-parse
        # echoes an unrecognised --end-of-options token to stdout.
        process = subprocess.run(
            ["git", "rev-parse", "--verify", f"{selected}^{{commit}}"],
            cwd=checkout,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if process.returncode:
            return [f"cannot resolve Go core commit {selected}: {process.stderr.strip()}"]
        resolved = process.stdout.strip()
        if resolved != source["go_core_commit"]:
            errors.append(
                "Go core commit differs from provenance: "
                f"expected {source['go_core_commit']}, got {resolved}"
            )
        selected = resolved
    for relative, expected in sorted(source["sha256"].items()):
        try:
            if is_git_checkout:
                data = _git_bytes(checkout, selected, relative)
            else:
                data = _snapshot_bytes(checkout, relative)
            actual = _sha256(data)
        except (OSError, ValueError) as exc:
            errors.append(str(exc))
            continue
        if actual != expected:
            errors.append(
                f"Go core drift for {relative}: expected {expected}, got {actual}"
            )
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Python SDK repository root",
    )
    parser.add_argument("--core-checkout", type=Path, help="Go core git checkout")
    parser.add_argument(
        "--commit",
        help="Go commit-ish to compare; defaults to the pinned commit (or use WORKTREE)",
    )
    args = parser.parse_args(argv)
    errors = verify_vendor(args.project_root.resolve())
    if args.commit and not args.core_checkout:
        parser.error("--commit requires --core-checkout")
    if args.core_checkout:
        errors.extend(
            compare_checkout(args.project_root.resolve(), args.core_checkout, args.commit)
        )
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    source = load_source(args.project_root.resolve())
    print(f"verified vendored Go core inputs at {source['go_core_commit']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
