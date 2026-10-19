"""HTTP client and launcher checks use fakes, never real profiles or keys."""
import ast
import json
import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_client_runner_has_one_visible_mode_switch() -> None:
    source = (ROOT / "client/run.sh").read_text()
    lines = source.splitlines()[:12]
    commands = [
        "DEMO_RUN_COMMAND='python /app/app.py'",
        "DEMO_RUN_COMMAND='cynapsa run --config /tmp/cynapsa-private/cynapsa.json -- python /app/app.py'",
    ]
    selected = [command for command in commands if command in lines]
    assert len(selected) == 1
    assert f"# {next(command for command in commands if command != selected[0])}" in lines
    assert not (ROOT / "client/run-http.sh").exists()
    result = subprocess.run(
        ["bash", "-c", source.split("ROOT=", 1)[0] + '\nprintf "%s" "$DEMO_RUN_COMMAND"'],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == selected[0].split("'", 2)[1]


def test_client_app_has_no_cynapsa_import_or_native_session() -> None:
    source = (ROOT / "client/app.py").read_text()
    tree = ast.parse(source)
    imports = {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imports.update(
        node.module.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    )
    assert "requests" in imports
    assert imports.isdisjoint({"cynapsa", "runtime", "connection_alerts", "rpc_logging"})
    assert "cynapsa.connect" not in source
    assert "requests.post" in source


def test_client_in_code_login_example_is_commented_and_valid_python() -> None:
    source = (ROOT / "client/app.py").read_text()
    example = source.split("    # import cynapsa\n", 1)[1].split("\n    main()", 1)[0]
    uncommented = "import cynapsa\n" + "\n".join(
        line[6:] if line.startswith("    # ") else ""
        for line in example.splitlines()
    )
    ast.parse(uncommented)
    assert "with cynapsa.login(" in uncommented
    assert 'mesh_id="cynapsa_demo_cynapsa_demo"' in uncommented
    assert '"recipient": "cynapsa-demo.agent-4dcbded5-8c32-435a-9e43-eeb37d903e98@connect.cynapsa.com"' in uncommented
    assert 'os.environ["DEMO_ORCHESTRATOR_ORIGIN"] = origin' in uncommented
    assert '"mode": "rpc"' in uncommented
    assert 'os.environ["DEMO_TRANSPORT"] = "cynapsa"' in uncommented


def test_client_entrypoint_can_run_the_same_app_as_plain_http(tmp_path):
    python = tmp_path / "python"
    log = tmp_path / "python.log"
    python.write_text('#!/bin/sh\nprintf "%s|%s|%s\n" "$*" "$DEMO_TRANSPORT" "${CYNAPSA_TOKEN-unset}" > "$TEST_LOG"\n')
    python.chmod(0o755)
    env = {**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}",
           "TEST_LOG": str(log), "DEMO_RUN_COMMAND": "python /app/app.py",
               "CYNAPSA_TOKEN": "test-only-token", "DEMO_CLIENT_QUIET": "1",
               "DEMO_HTTP_ACCESS_KEY": "test-only-key"}
    env.pop("DEMO_ORCHESTRATOR_AGENT_ID", None)
    result = subprocess.run(["/bin/sh", str(ROOT / "client/entrypoint.sh")],
                            env=env, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr
    assert log.read_text().strip() == "/app/app.py --quiet|http|unset"


def test_client_entrypoint_rejects_unknown_command(tmp_path):
    env = {**os.environ, "DEMO_RUN_COMMAND": "sh -c unexpected"}
    result = subprocess.run(["/bin/sh", str(ROOT / "client/entrypoint.sh")],
                            env=env, text=True, capture_output=True)
    assert result.returncode == 64
    assert "unsupported DEMO_RUN_COMMAND" in result.stderr


@pytest.mark.parametrize("enrollment", ["cached", "fresh", "force"])
@pytest.mark.parametrize("origin", [None, "https://orchestrator.example.test"])
def test_entrypoint_builds_cynapsa_config_in_all_login_paths(tmp_path, enrollment, origin):
    log = tmp_path / "calls"
    cli = tmp_path / "cynapsa"
    cli.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$TEST_LOG"\n')
    cli.chmod(0o755)
    state = tmp_path / "state"
    state.mkdir()
    if enrollment != "fresh":
        (state / "profile-v2-fake.state").write_text("not real credentials")
    env = {**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}",
           "TEST_LOG": str(log), "CYNAPSA_STATE_DIRECTORY": str(state),
           "DEMO_PRIVATE_DIRECTORY": str(tmp_path / "private"),
           "DEMO_SECRET_SOURCE_DIRECTORY": str(tmp_path / "secrets"),
           "DEMO_CLIENT_QUIET": "1", "CYNAPSA_TOKEN": "test-only-token",
           "DEMO_MESH_ID": "test-mesh", "DEMO_ORCHESTRATOR_AGENT_ID": "orchestrator@example.test",
           "DEMO_FORCE_ENROLL": "1" if enrollment == "force" else ""}
    env.pop("DEMO_ORCHESTRATOR_ORIGIN", None)
    if origin is not None:
        env["DEMO_ORCHESTRATOR_ORIGIN"] = origin
    result = subprocess.run(["/bin/sh", str(ROOT / "client/entrypoint.sh")], env=env, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    config_path = tmp_path / "private" / "cynapsa.json"
    assert log.read_text().splitlines() == [
        f"run --config {config_path} -- python /app/app.py --quiet"
    ]
    config = json.loads(config_path.read_text())
    assert config["schema_version"] == 1
    assert config["mesh_id"] == "test-mesh"
    assert config["profile_id"] == "demo-client"
    assert config["rpc_timeout_ms"] == 140000
    assert config["map"] == [{
        "origin": origin or "http://demo-orchestrator.local",
        "recipient": "orchestrator@example.test",
        "mode": "rpc",
    }]
    assert ("token_file" in config) is (enrollment != "cached")
    assert config.get("force_enroll", False) is (enrollment == "force")
    assert "test-only-token" not in config_path.read_text()
    assert config_path.stat().st_mode & 0o777 == 0o600


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
           "CYNAPSA_DEMO_CLIENT_ENV_FILE": str(envfile), "CYNAPSA_DEMO_CLIENT_VOLUME": "quiet-test-state",
           "DEMO_HTTP_ACCESS_KEY": "test-only-key"}
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
        assert "--env DEMO_RUN_COMMAND" in calls
        assert "--env DEMO_ORCHESTRATOR_ORIGIN" in calls
        if "DEMO_RUN_COMMAND='python /app/app.py'" in (ROOT / "client/run.sh").read_text().splitlines()[:12]:
            assert "--env DEMO_HTTP_ACCESS_KEY" in calls
        assert "src=quiet-test-state" in calls
