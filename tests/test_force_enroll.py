"""Local, credential-free checks for the demo force-enrollment wiring."""

from __future__ import annotations

import os
import json
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ROLES = ("client", "maps", "orchestrator", "files")
HTTP_SERVER_ROLES = ("maps", "orchestrator")


class ForceEnrollTests(unittest.TestCase):
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
                    "DEMO_FILES_DIRECTORY": scratch,
                    "DEMO_HTTP_ACCESS_KEY": "test-only-key",
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
                if role == "files":
                    self.assertTrue(all(
                        f"type=bind,src={scratch},dst=/data,readonly" in call for call in runs
                    ))
                    self.assertTrue(all("--env DEMO_FILES_DIRECTORY=/data" in call for call in runs))
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

    def test_entrypoint_runs_direct_asgi_inside_cli(self) -> None:
        for role in HTTP_SERVER_ROLES:
            with self.subTest(role=role), tempfile.TemporaryDirectory(dir=ROOT) as scratch:
                scratch_path = Path(scratch)
                state = scratch_path / "state"
                state.mkdir()
                (state / "profile-v2-test.state").write_text("placeholder", encoding="utf-8")
                fake_cynapsa = scratch_path / "cynapsa"
                cli_log = scratch_path / "cynapsa.log"
                fake_cynapsa.write_text(
                    '#!/bin/sh\nprintf "%s\\n" "$*" >> "$CYNAPSA_LOG"\n'
                    'printf "%s" "${CYNAPSA_CLI_NETWORK_EVENTS:-}" > "$NETWORK_LOG"\n'
                    '[ "${TEST_CLI_FAIL:-}" != 1 ] || exit 42\n',
                    encoding="utf-8",
                )
                fake_cynapsa.chmod(0o755)
                env = os.environ.copy()
                env.pop("CYNAPSA_TOKEN", None)
                env.pop("DEMO_RUN_COMMAND", None)
                env.update({
                    "PATH": f"{scratch}{os.pathsep}{env['PATH']}",
                    "CYNAPSA_STATE_DIRECTORY": str(state),
                    "DEMO_PRIVATE_DIRECTORY": str(scratch_path / "private"),
                    "DEMO_SECRET_SOURCE_DIRECTORY": str(scratch_path / "secrets"),
                    "CYNAPSA_LOG": str(cli_log),
                    "NETWORK_LOG": str(scratch_path / "network.log"),
                    "DEMO_MESH_ID": "test-mesh",
                    "DEMO_FORCE_ENROLL": "1",
                })
                if role == "orchestrator":
                    env["DEMO_MAPS_AGENT_ID"] = "maps@example.test"
                if role != "client":
                    env["LITELLM_API_KEY"] = "test-only-key"
                if role == "maps":
                    env["GOOGLE_MAPS_API_KEY"] = "test-only-key"
                entrypoint = ["/bin/sh", str(ROOT / role / "entrypoint.sh")]
                missing = subprocess.run(entrypoint, env=env, capture_output=True, text=True)
                self.assertEqual(missing.returncode, 78)
                self.assertIn("an enrollment token is required", missing.stderr)
                self.assertFalse(cli_log.exists())
                env["CYNAPSA_TOKEN"] = "test-only-token"
                forced = subprocess.run(entrypoint, env=env, capture_output=True, text=True)
                self.assertEqual(forced.returncode, 0, forced.stderr)
                token_file = scratch_path / "private" / "enrollment_token"
                config_file = scratch_path / "private" / "cynapsa.json"
                command = cli_log.read_text(encoding="utf-8").strip()
                self.assertEqual(command, f"run --config {config_file} -- app:app")
                self.assertEqual((scratch_path / "network.log").read_text(), "1")
                config = json.loads(config_file.read_text(encoding="utf-8"))
                self.assertEqual(config["token_file"], str(token_file))
                self.assertTrue(config["force_enroll"])
                self.assertEqual(config["mesh_id"], "test-mesh")
                if role == "orchestrator":
                    self.assertEqual(config["map"], [{
                        "origin": "http://demo-maps.local",
                        "recipient": "maps@example.test",
                        "mode": "rpc",
                    }])

                    env["DEMO_FILES_AGENT_ID"] = "files@example.test"
                    with_files = subprocess.run(entrypoint, env=env, capture_output=True, text=True)
                    self.assertEqual(with_files.returncode, 0, with_files.stderr)
                    self.assertEqual(len(json.loads(config_file.read_text())["map"]), 2)

                env["TEST_CLI_FAIL"] = "1"
                rejected = subprocess.run(entrypoint, env=env, capture_output=True, text=True)
                self.assertEqual(rejected.returncode, 42, rejected.stderr)

                # A trusted image-command override still works.
                direct = subprocess.run(entrypoint + ["true"], env=env, capture_output=True, text=True)
                self.assertEqual(direct.returncode, 0, direct.stderr)

                # Cloud environments select one of two visible, exact commands;
                # network ingress, not the bind address, controls reachability.
                uvicorn_log = scratch_path / "uvicorn.log"
                fake_uvicorn = scratch_path / "uvicorn"
                fake_uvicorn.write_text('#!/bin/sh\nprintf "%s\\n" "$*" > "$UVICORN_LOG"\n')
                fake_uvicorn.chmod(0o755)
                env["UVICORN_LOG"] = str(uvicorn_log)
                env["DEMO_RUN_COMMAND"] = (
                    "uvicorn app:app --host 0.0.0.0 --port 8000 --no-access-log"
                )
                env["DEMO_HTTP_ACCESS_KEY"] = "test-only-key"
                http = subprocess.run(entrypoint, env=env, capture_output=True, text=True)
                self.assertEqual(http.returncode, 0, http.stderr)
                self.assertEqual(
                    uvicorn_log.read_text().strip(),
                    "app:app --host 0.0.0.0 --port 8000 --no-access-log",
                )
                env["DEMO_RUN_COMMAND"] = "sh -c dangerous"
                unsupported = subprocess.run(entrypoint, env=env, capture_output=True, text=True)
                self.assertEqual(unsupported.returncode, 64)

    def test_files_entrypoint_runs_http_app_through_cli(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as scratch:
            root = Path(scratch)
            state = root / "state"
            state.mkdir()
            private = root / "private"
            fake_cli = root / "cynapsa"
            cli_log = root / "cli.log"
            fake_cli.write_text(
                '#!/bin/sh\nprintf "%s\\n" "$*" >> "$CYNAPSA_LOG"\n'
                '[ "${TEST_CLI_FAIL:-}" != 1 ] || exit 42\n', encoding="utf-8"
            )
            fake_cli.chmod(0o755)
            env = os.environ.copy()
            env.update({
                "PATH": f"{root}{os.pathsep}{env['PATH']}",
                "CYNAPSA_STATE_DIRECTORY": str(state),
                "DEMO_PRIVATE_DIRECTORY": str(private),
                "DEMO_SECRET_SOURCE_DIRECTORY": str(root / "secrets"),
                "CYNAPSA_LOG": str(cli_log),
                "DEMO_MESH_ID": "test-mesh",
                "LITELLM_API_KEY": "test-only-key",
            })
            command = ["/bin/sh", str(ROOT / "files/entrypoint.sh")]
            missing = subprocess.run(command, env=env, capture_output=True, text=True)
            self.assertEqual(missing.returncode, 78, missing.stderr)
            self.assertFalse(cli_log.exists())

            env["CYNAPSA_TOKEN"] = "test-only-token"
            enrolled = subprocess.run(command, env=env, capture_output=True, text=True)
            self.assertEqual(enrolled.returncode, 0, enrolled.stderr)
            self.assertIn(
                f"run --mesh-id test-mesh --profile-id demo-files --rpc-timeout-ms 110000 "
                f"--token-file {private / 'enrollment_token'} -- uvicorn app:app "
                "--host 0.0.0.0 --port 8000 --no-access-log",
                cli_log.read_text(),
            )

            (state / "profile-v2-test.state").write_text("placeholder")
            cached = subprocess.run(command, env=env, capture_output=True, text=True)
            self.assertEqual(cached.returncode, 0, cached.stderr)
            self.assertNotIn("--token-file", cli_log.read_text().splitlines()[-1])

            env["DEMO_FORCE_ENROLL"] = "1"
            forced = subprocess.run(command, env=env, capture_output=True, text=True)
            self.assertEqual(forced.returncode, 0, forced.stderr)
            self.assertIn("--token-file", cli_log.read_text().splitlines()[-1])
            self.assertIn("--force-enroll -- uvicorn", cli_log.read_text().splitlines()[-1])
            env["TEST_CLI_FAIL"] = "1"
            rejected = subprocess.run(command, env=env, capture_output=True, text=True)
            self.assertEqual(rejected.returncode, 42, rejected.stderr)


if __name__ == "__main__":
    unittest.main()
