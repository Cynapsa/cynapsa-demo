"""Credential-free regression checks: demo authorization belongs to the server."""

from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ROLES = ("client", "maps", "orchestrator")
LOCAL_POLICY = re.compile(r"--allow|\bpolicy\s*\.\s*(?:set|allow|deny)\b")


class NoLocalPolicyTests(unittest.TestCase):
    def assert_no_local_policy(self, path: Path) -> None:
        self.assertNotRegex(
            path.read_text(encoding="utf-8"),
            LOCAL_POLICY,
            f"{path.relative_to(ROOT)} must rely on server authorization",
        )

    def test_launch_scripts_have_no_local_policy(self) -> None:
        for role in ROLES:
            for name in ("run.sh", "entrypoint.sh", "bootstrap.py"):
                with self.subTest(role=role, name=name):
                    self.assert_no_local_policy(ROOT / role / name)

    def test_native_apps_and_runtime_have_no_local_policy(self) -> None:
        for role in ROLES:
            for name in ("app.py", "runtime.py"):
                with self.subTest(role=role, name=name):
                    self.assert_no_local_policy(ROOT / role / name)

    def test_demo_docs_have_no_local_policy(self) -> None:
        paths = [ROOT / "AGENTS.md", ROOT / "README.md"]
        for role in ROLES:
            paths.extend(sorted((ROOT / role).glob("*.md")))
        for path in paths:
            with self.subTest(path=path.relative_to(ROOT)):
                self.assert_no_local_policy(path)


if __name__ == "__main__":
    unittest.main()
