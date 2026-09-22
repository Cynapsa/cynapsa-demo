from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

from scripts.verify_core_provenance import compare_checkout, load_source, verify_vendor


ROOT = Path(__file__).resolve().parents[1]


def test_vendored_header_and_conformance_match_provenance() -> None:
    assert verify_vendor(ROOT) == []


def test_vendored_drift_is_reported(tmp_path: Path) -> None:
    project = tmp_path / "sdk"
    (project / "src" / "cynapsa").mkdir(parents=True)
    shutil.copytree(ROOT / "src" / "cynapsa" / "_vendor", project / "src" / "cynapsa" / "_vendor")
    shutil.copy2(ROOT / "src" / "cynapsa" / "_core_source.json", project / "src" / "cynapsa" / "_core_source.json")
    header = project / "src" / "cynapsa" / "_vendor" / "cmd" / "cynapsacore-shared" / "cynapsacore_v1.h"
    header.write_bytes(header.read_bytes() + b"\n/* drift */\n")
    errors = verify_vendor(project)
    assert len(errors) == 1
    assert "vendored hash mismatch" in errors[0]


@pytest.mark.skipif(
    not os.environ.get("CYNAPSA_GO_CORE_CHECKOUT"),
    reason="set CYNAPSA_GO_CORE_CHECKOUT to compare the pinned Go commit",
)
def test_supplied_go_checkout_matches_pinned_commit() -> None:
    checkout = Path(os.environ["CYNAPSA_GO_CORE_CHECKOUT"])
    assert compare_checkout(ROOT, checkout) == []


def test_source_snapshot_without_git_metadata_is_hash_verified(tmp_path: Path) -> None:
    source = load_source(ROOT)
    snapshot = tmp_path / "snapshot"
    vendor = ROOT / "src" / "cynapsa" / "_vendor"
    for relative in source["sha256"]:
        target = snapshot.joinpath(*relative.split("/"))
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(vendor.joinpath(*relative.split("/")).read_bytes())

    assert compare_checkout(ROOT, snapshot) == []
    assert compare_checkout(ROOT, snapshot, "HEAD") == [
        "cannot resolve an explicit commit in a Go core source snapshot "
        f"without git metadata: {snapshot.resolve()}"
    ]
    (snapshot / ".git").mkdir()
    assert compare_checkout(ROOT, snapshot, "--upload-pack=malicious") == [
        "unsafe Go core commit-ish: '--upload-pack=malicious'"
    ]
    (snapshot / ".git").rmdir()
    changed = snapshot / "conformance" / "v1" / "abi.json"
    changed.write_bytes(changed.read_bytes() + b"\n")
    errors = compare_checkout(ROOT, snapshot)
    assert len(errors) == 1
    assert errors[0].startswith("Go core drift for conformance/v1/abi.json:")
