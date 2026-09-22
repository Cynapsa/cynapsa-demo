from __future__ import annotations

import hashlib
import re
import shutil
import subprocess
from pathlib import Path


DOCUMENTS = (
    "AZTM_COMMAND_GATE.md",
    "AZTM_ENVELOPE_PAYLOAD_PATCHING.md",
    "AZTM_FUTURE_ISSUES.md",
    "AZTM_GO_CORE.md",
    "AZTM_NETWORKING.md",
    "AZTM_NETWORKING_SHORT.md",
    "AZTM_SDK.md",
    "AZTM_SDK_BOUNDARY.md",
)

OBSOLETE = re.compile(
    rb"ordered_delivery|delivery\.ordering_blocked|delivery\.unknown|"
    rb"(?:^|[^a-z0-9_])(?:ordered|unordered|ordering|seq|sequence|sequencer|"
    rb"generation|reorder|reordering|gap|head)(?:[^a-z0-9_]|$)",
    re.IGNORECASE | re.MULTILINE,
)


def test_aztm_root_document_manifest_and_vocabulary() -> None:
    repository = Path(__file__).resolve().parents[1]
    manifest = repository / "AZTM_DOCS_MANIFEST.sha256"
    entries: list[tuple[str, str]] = []
    for line in manifest.read_text(encoding="ascii").splitlines():
        digest, name = line.split("  ")
        assert len(digest) == hashlib.sha256().digest_size * 2
        entries.append((digest, name))

    assert tuple(name for _, name in entries) == DOCUMENTS
    assert tuple(sorted(path.name for path in repository.glob("AZTM_*.md"))) == DOCUMENTS
    for digest, name in entries:
        data = (repository / name).read_bytes()
        assert hashlib.sha256(data).hexdigest() == digest
        assert OBSOLETE.search(data) is None


def _stage_document_checkout(source: Path, destination: Path) -> None:
    destination.mkdir(parents=True)
    for name in (*DOCUMENTS, "AZTM_DOCS_MANIFEST.sha256"):
        shutil.copy2(source / name, destination / name)


def _stage_sdk_verifier(source: Path, destination: Path) -> None:
    _stage_document_checkout(source, destination)
    scripts = destination / "scripts"
    scripts.mkdir()
    shutil.copy2(source / "scripts/verify-aztm-docs.sh", scripts)


def test_aztm_verifier_resolves_supported_layouts_and_explicit_override(
    tmp_path: Path,
) -> None:
    source = Path(__file__).resolve().parents[1]
    layouts = (
        (
            tmp_path / "direct" / "cynapsa" / "cynapsa-python-sdk",
            tmp_path / "direct" / "cynapsa" / "cynapsagocore",
        ),
        (
            tmp_path / "nested" / "cynapsa-python-sdk",
            tmp_path / "nested" / "cynapsa" / "cynapsagocore",
        ),
    )
    for sdk, core in layouts:
        _stage_sdk_verifier(source, sdk)
        _stage_document_checkout(source, core)
        completed = subprocess.run(
            [str(sdk / "scripts/verify-aztm-docs.sh")],
            text=True,
            capture_output=True,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr + completed.stdout
        assert str(core.resolve()) in completed.stdout

    sdk = tmp_path / "override" / "sdk"
    core = tmp_path / "override" / "arbitrary-core"
    _stage_sdk_verifier(source, sdk)
    _stage_document_checkout(source, core)
    completed = subprocess.run(
        [str(sdk / "scripts/verify-aztm-docs.sh"), str(core)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr + completed.stdout
    assert str(core) in completed.stdout
