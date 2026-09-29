"""Shell wiring tests use fake Docker/Python, never real profiles or keys."""
import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("enrollment", ["cached", "fresh", "force"])
def test_entrypoint_forwards_quiet_in_all_login_paths(tmp_path, enrollment):
    log = tmp_path / "calls"
    for command in ("python", "cynapsa"):
        script = tmp_path / command
        script.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$TEST_LOG"\n')
        script.chmod(0o755)
    state = tmp_path / "state"
    state.mkdir()
    if enrollment != "fresh":
        (state / "profile-v2-fake.state").write_text("not real credentials")
    env = {**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}",
           "TEST_LOG": str(log), "CYNAPSA_STATE_DIRECTORY": str(state),
           "DEMO_PRIVATE_DIRECTORY": str(tmp_path / "private"),
           "DEMO_SECRET_SOURCE_DIRECTORY": str(tmp_path / "secrets"),
           "DEMO_CLIENT_QUIET": "1", "CYNAPSA_TOKEN": "test-only-token",
           "DEMO_MESH_ID": "test-mesh", "DEMO_FORCE_ENROLL": "1" if enrollment == "force" else ""}
    result = subprocess.run(["/bin/sh", str(ROOT / "client/entrypoint.sh")], env=env, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    commands = log.read_text().splitlines()
    assert commands[-1] == "/app/app.py" + (" --enroll" if enrollment == "fresh" else "") + " --quiet"
    if enrollment == "force":
        assert commands[0].startswith("run --mesh-id test-mesh")
        assert "--force-enroll" in commands[0]


@pytest.mark.parametrize("build_failure", [False, True])
def test_quiet_runner_hides_successful_build_but_keeps_build_errors(tmp_path, build_failure):
    log = tmp_path / "docker.log"
    docker = tmp_path / "docker"
    docker.write_text(
        '#!/bin/sh\nprintf "%s\\n" "$*" >> "$TEST_LOG"\n'
        'if [ "$1" = version ]; then echo amd64; fi\n'
        'if [ "$1" = buildx ]; then\n'
        '  echo "Build progress"\n'
        '  if [ "$TEST_BUILD_FAILURE" = 1 ]; then echo "Build failed" >&2; exit 37; fi\n'
        'fi\n'
    )
    docker.chmod(0o755)
    envfile = tmp_path / "client.env"
    envfile.write_text("CYNAPSA_GITHUB_TOKEN=test-only-github-token\nDEMO_MESH_ID=test-mesh\n")
    env = {**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}", "TEST_LOG": str(log),
           "TEST_BUILD_FAILURE": "1" if build_failure else "0", "TMPDIR": str(tmp_path),
           "CYNAPSA_DEMO_CLIENT_ENV_FILE": str(envfile), "CYNAPSA_DEMO_CLIENT_VOLUME": "quiet-test-state"}
    env.pop("CYNAPSA_TOKEN", None)
    env.pop("CYNAPSA_GITHUB_TOKEN", None)
    result = subprocess.run(["bash", str(ROOT / "client/run.sh"), "--build", "--force-enroll", "--quiet"],
                            env=env, text=True, capture_output=True)
    assert result.stdout == ""
    calls = log.read_text()
    assert "test-only-github-token" not in calls
    assert not list(tmp_path.glob("cynapsa-demo-client-build.*"))
    assert not list(tmp_path.glob("cynapsa-demo-client-runtime.*"))
    if build_failure:
        assert result.returncode == 37
        assert "Build failed" in result.stderr
        assert "run --rm" not in calls
    else:
        assert result.returncode == 0, result.stderr
        assert result.stderr == ""
        assert "--env DEMO_CLIENT_QUIET=1" in calls
        assert "--env DEMO_FORCE_ENROLL=1" in calls
        assert "src=quiet-test-state" in calls
