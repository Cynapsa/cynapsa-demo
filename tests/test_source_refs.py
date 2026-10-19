"""Each independent image must build from the paired main branches."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_all_entity_images_use_paired_main_branches() -> None:
    for role in ("client", "orchestrator", "maps", "files"):
        dockerfile = (ROOT / role / "Dockerfile").read_text(encoding="utf-8")
        entrypoint = (ROOT / role / "entrypoint.sh").read_text(encoding="utf-8")
        assert "ARG CORE_GIT_REF=main" in dockerfile, role
        assert "ARG SDK_GIT_REF=main" in dockerfile, role
        assert "source-revisions" in dockerfile, role
        assert "export CYNAPSA_CLI_NETWORK_EVENTS=1" in entrypoint, role
