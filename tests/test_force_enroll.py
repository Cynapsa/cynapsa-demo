"""Local, credential-free checks for the demo force-enrollment wiring."""

from __future__ import annotations

import os
import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
ROLES = ("client", "maps", "orchestrator")


class ForceEnrollTests(unittest.TestCase):
    def test_runtime_passes_native_force_option(self) -> None:
        for role in ROLES:
            with self.subTest(role=role), tempfile.TemporaryDirectory(dir=ROOT) as scratch:
                spec = importlib.util.spec_from_file_location(
                    f"demo_{role}_runtime", ROOT / role / "runtime.py"
                )
                self.assertIsNotNone(spec)
                self.assertIsNotNone(spec.loader)
                runtime = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(runtime)
                private = Path(scratch) / "private"
                private.mkdir(mode=0o700)
                token_file = private / "enrollment_token"
                token_file.write_text("test-only-token", encoding="utf-8")
                token_file.chmod(0o600)
                with patch.object(runtime, "PRIVATE", private), patch.dict(
                    os.environ, {"DEMO_MESH_ID": "test-mesh"}
                ):
                    options = runtime.connection_options(enroll=True, force_enroll=True)
                    self.assertTrue(options["force_enroll"])
                    self.assertIn("enrollment_token", options)
                    self.assertNotIn("force_enroll", runtime.connection_options(enroll=False))
                    with self.assertRaisesRegex(RuntimeError, "requires an enrollment token"):
                        runtime.connection_options(enroll=False, force_enroll=True)

    def test_runner_keeps_selected_volume(self) -> None:
        for role in ROLES:
            with self.subTest(role=role), tempfile.TemporaryDirectory(dir=ROOT) as scratch:
                scratch_path = Path(scratch)
                fake_docker = scratch_path / "docker"
                log = scratch_path / "docker.log"
                fake_docker.write_text(
                    '#!/bin/sh\nprintf "%s\\n" "$*" >> "$DOCKER_LOG"\n'
                    'if [ "$1" = version ]; then printf "amd64\\n"; fi\n'
                    'if [ "$1 $2" = "image inspect" ] && '
                    '[ "${TEST_IMAGE_MISSING:-}" = 1 ]; then exit 1; fi\n'
                    'if [ "$1" = run ]; then\n'
                    '  while [ "$#" -gt 0 ]; do\n'
                    '    if [ "$1" = --env-file ]; then\n'
                    '      shift\n'
                    '      if grep -q "^CYNAPSA_GITHUB_TOKEN=" "$1"; then exit 72; fi\n'
                    '    fi\n'
                    '    shift\n'
                    '  done\n'
                    'fi\n',
                    encoding="utf-8",
                )
                fake_docker.chmod(0o755)
                env = os.environ.copy()
                env.pop("CYNAPSA_TOKEN", None)
                env.pop("CYNAPSA_GITHUB_TOKEN", None)
                demo_env = scratch_path / "runner.env"
                demo_env.write_text(
                    "CYNAPSA_GITHUB_TOKEN=test-only-github-token\n"
                    "DEMO_MESH_ID=test-mesh\n", encoding="utf-8"
                )
                env.update({
                    "PATH": f"{scratch}{os.pathsep}{env['PATH']}",
                    "DOCKER_LOG": str(log),
                    f"CYNAPSA_DEMO_{role.upper()}_ENV_FILE": str(demo_env),
                    f"CYNAPSA_DEMO_{role.upper()}_VOLUME": f"test-{role}-state",
                })
                runner = ["bash", str(ROOT / role / "run.sh")]
                normal = subprocess.run(runner, env=env, capture_output=True, text=True)
                self.assertEqual(normal.returncode, 0, normal.stderr)
                forced = subprocess.run(
                    runner + ["--force-enroll"], env=env, capture_output=True, text=True
                )
                self.assertEqual(forced.returncode, 0, forced.stderr)
                calls = log.read_text(encoding="utf-8").splitlines()
                runs = [call for call in calls if call.startswith("run ")]
                creates = [call for call in calls if call.startswith("volume create ")]
                self.assertEqual(len(runs), 2)
                self.assertEqual(creates, [f"volume create test-{role}-state"] * 2)
                self.assertTrue(all(f"src=test-{role}-state" in call for call in runs))
                self.assertNotIn("DEMO_FORCE_ENROLL", runs[0])
                self.assertIn("--env DEMO_FORCE_ENROLL=1", runs[1])
                self.assertFalse(any(call.startswith("buildx ") for call in calls))
                self.assertNotIn("DEMO_ENROLL_ONLY", "\n".join(calls))
                self.assertNotIn("test-only-github-token", "\n".join(calls))

                explicit = subprocess.run(
                    runner + ["--build"], env=env, capture_output=True, text=True
                )
                self.assertEqual(explicit.returncode, 0, explicit.stderr)
                self.assertEqual(
                    sum(call.startswith("buildx ") for call in log.read_text(encoding="utf-8").splitlines()),
                    1,
                )
                env["TEST_IMAGE_MISSING"] = "1"
                missing = subprocess.run(runner, env=env, capture_output=True, text=True)
                self.assertEqual(missing.returncode, 0, missing.stderr)
                self.assertEqual(
                    sum(call.startswith("buildx ") for call in log.read_text(encoding="utf-8").splitlines()),
                    2,
                )
                self.assertIn(
                    "--secret id=github_token,env=CYNAPSA_GITHUB_TOKEN",
                    log.read_text(encoding="utf-8"),
                )

    def test_entrypoint_runs_cli_force_enroll_before_native_app(self) -> None:
        for role in ROLES:
            with self.subTest(role=role), tempfile.TemporaryDirectory(dir=ROOT) as scratch:
                scratch_path = Path(scratch)
                state = scratch_path / "state"
                state.mkdir()
                (state / "profile-v2-test.state").write_text("placeholder", encoding="utf-8")
                fake_python = scratch_path / "python"
                log = scratch_path / "python.log"
                fake_cynapsa = scratch_path / "cynapsa"
                cli_log = scratch_path / "cynapsa.log"
                fake_python.write_text(
                    '#!/bin/sh\nprintf "%s\\n" "$*" >> "$PYTHON_LOG"\n',
                    encoding="utf-8",
                )
                fake_python.chmod(0o755)
                fake_cynapsa.write_text(
                    '#!/bin/sh\nprintf "%s\\n" "$*" >> "$CYNAPSA_LOG"\n'
                    '[ "${TEST_CLI_FAIL:-}" != 1 ] || exit 42\n',
                    encoding="utf-8",
                )
                fake_cynapsa.chmod(0o755)
                env = os.environ.copy()
                env.pop("CYNAPSA_TOKEN", None)
                env.update({
                    "PATH": f"{scratch}{os.pathsep}{env['PATH']}",
                    "CYNAPSA_STATE_DIRECTORY": str(state),
                    "DEMO_PRIVATE_DIRECTORY": str(scratch_path / "private"),
                    "DEMO_SECRET_SOURCE_DIRECTORY": str(scratch_path / "secrets"),
                    "PYTHON_LOG": str(log),
                    "CYNAPSA_LOG": str(cli_log),
                    "DEMO_MESH_ID": "test-mesh",
                    "DEMO_FORCE_ENROLL": "1",
                })
                if role != "client":
                    env["LITELLM_API_KEY"] = "test-only-key"
                if role == "maps":
                    env["GOOGLE_MAPS_API_KEY"] = "test-only-key"
                entrypoint = ["/bin/sh", str(ROOT / role / "entrypoint.sh")]
                missing = subprocess.run(entrypoint, env=env, capture_output=True, text=True)
                self.assertEqual(missing.returncode, 78)
                self.assertIn("an enrollment token is required", missing.stderr)
                self.assertFalse(log.exists())
                self.assertFalse(cli_log.exists())
                env["CYNAPSA_TOKEN"] = "test-only-token"
                forced = subprocess.run(entrypoint, env=env, capture_output=True, text=True)
                self.assertEqual(forced.returncode, 0, forced.stderr)
                self.assertEqual(log.read_text(encoding="utf-8").strip(), "/app/app.py")
                token_file = scratch_path / "private" / "enrollment_token"
                self.assertEqual(
                    cli_log.read_text(encoding="utf-8").strip(),
                    f"run --mesh-id test-mesh --profile-id demo-{role} "
                    f"--token-file {token_file} --force-enroll -- python /app/bootstrap.py",
                )
                self.assertFalse(token_file.exists())

                log.unlink()
                env["TEST_CLI_FAIL"] = "1"
                rejected = subprocess.run(entrypoint, env=env, capture_output=True, text=True)
                self.assertEqual(rejected.returncode, 42, rejected.stderr)
                self.assertFalse(log.exists(), "native app must not start after CLI failure")


if __name__ == "__main__":
    unittest.main()
