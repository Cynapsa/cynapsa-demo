from __future__ import annotations

import ast
import asyncio
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from e2e.simple.cynapsa_e2e import client_helpers, settings


ROOT = Path(__file__).resolve().parents[1]
E2E = ROOT / "e2e" / "simple"
DEDICATED_EJABBERD = Path(
    os.environ.get(
        "CYNAPSA_EJABBERD_PATH",
        ROOT.parent / "cynapsa" / "ejabberd-remove-snapshot",
    )
)


def dedicated_ejabberd_file(relative_path: str) -> str:
    path = DEDICATED_EJABBERD / relative_path
    if not path.is_file():
        pytest.skip(f"dedicated ejabberd checkout is unavailable: {path}")
    return path.read_text(encoding="utf-8")


def test_e2e_shell_scripts_parse() -> None:
    scripts = sorted(E2E.glob("*.sh"))
    assert scripts
    for script in scripts:
        subprocess.run(["bash", "-n", str(script)], check=True)


def test_monkey_fastapi_server_is_zero_source_change_http_code() -> None:
    source = (E2E / "cynapsa_e2e" / "monkey_fastapi_server.py").read_text(
        encoding="utf-8"
    )
    tree = ast.parse(source)
    fastapi_imports = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom) and node.module == "fastapi"
        for alias in node.names
    }
    assert "Request" in fastapi_imports
    assert not any(
        (
            isinstance(node, ast.Import)
            and any(
                alias.name == "cynapsa" or alias.name.startswith("cynapsa.")
                for alias in node.names
            )
        )
        or (
            isinstance(node, ast.ImportFrom)
            and node.module is not None
            and (node.module == "cynapsa" or node.module.startswith("cynapsa."))
        )
        for node in ast.walk(tree)
    )
    assert not any(
        isinstance(node, ast.ImportFrom) and node.level > 0 for node in ast.walk(tree)
    )
    assert "cynapsa" not in source.lower()
    assert 'handler.register' not in source
    assert "CYNAPSA_E2E_MONKEY_MODE" not in source


def test_monkey_sync_client_is_zero_source_change_requests_code() -> None:
    source = (E2E / "cynapsa_e2e" / "monkey_sync_client.py").read_text(
        encoding="utf-8"
    )
    tree = ast.parse(source)
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    assert "requests" in imports
    assert "cynapsa" not in source.lower()
    assert not any(
        isinstance(node, ast.ImportFrom) and node.level > 0 for node in ast.walk(tree)
    )


def test_compose_launches_zero_change_http_agents_through_cli() -> None:
    source = (E2E / "compose.yaml").read_text(encoding="utf-8")
    server = source[
        source.index("  monkey-server:") : source.index("  native-sync-client:")
    ]
    client = source[
        source.index("  monkey-sync-client:") : source.index("  monkey-async-client:")
    ]

    for service in (server, client):
        assert "      - cynapsa\n      - run\n" in service
        assert "      - --password-env\n      - CYNAPSA_PASSWORD\n" in service
        assert "      - --allow\n      - \"*\"\n      - \"*\"\n" in service

    assert "cynapsa_e2e.monkey_fastapi_server:app" in server
    assert "http://127.0.0.1:8000/openapi.json" in server
    assert "cynapsa_e2e.monkey_sync_client" in client
    assert client.count("      - --map\n") == 2
    assert "https://native-server.test" in client
    assert "native-server@mesh.test" in client
    assert "https://monkey-server.test" in client
    assert "monkey-server@mesh.test" in client


def test_sidecar_entrypoint_removes_untagged_ip_and_uses_vlan() -> None:
    source = (E2E / "sidecar-entrypoint.sh").read_text(encoding="utf-8")
    assert 'type vlan id "$CYNAPSA_VLAN_ID"' in source
    assert 'ip addr del "$trunk_address" dev eth0' in source
    assert "ip route flush dev eth0" in source
    assert "ip link set eth0 addrgenmode none" in source
    assert "type vxlan" not in source
    assert "iptables -t nat -F" in source
    assert "MASQUERADE|SNAT|DNAT" in source


def test_python_e2e_apps_do_not_contain_network_setup_or_probe_code() -> None:
    sources = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted((E2E / "cynapsa_e2e").glob("*.py"))
    )
    forbidden = (
        "CYNAPSA_VLAN",
        "CYNAPSA_LAN",
        "network-probe",
        "ip route",
        "ip addr",
        "subprocess",
        "socket",
        "ssl.",
    )
    for token in forbidden:
        assert token not in sources


def test_monkey_clients_use_explicit_per_address_rpc_modes() -> None:
    helpers = (E2E / "cynapsa_e2e" / "client_helpers.py").read_text(
        encoding="utf-8"
    )
    tree = ast.parse(helpers)
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "address_map"
    )
    returned = next(
        node.value for node in ast.walk(function) if isinstance(node, ast.Return)
    )
    assert isinstance(returned, ast.Dict)
    assert len(returned.values) == 2
    for value in returned.values:
        assert isinstance(value, ast.Dict)
        fields = {
            key.value: item.value
            for key, item in zip(value.keys, value.values, strict=True)
            if isinstance(key, ast.Constant) and isinstance(item, ast.Constant)
        }
        assert fields["mode"] == "rpc"
        assert len(value.keys) == 2

    sources = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted((E2E / "cynapsa_e2e").glob("*.py"))
    )
    assert "http_mode" not in sources


def test_async_target_uses_fixed_workload_cadence_between_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[tuple[str, object]] = []

    async def fake_sleep(delay: float) -> None:
        events.append(("sleep", delay))

    async def check(text: str) -> None:
        events.append(("request", text))

    monkeypatch.setattr(
        client_helpers, "MESSAGES", ("message-0", "message-1", "message-2")
    )
    monkeypatch.setattr(client_helpers.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(client_helpers, "diagnostic", lambda *args, **kwargs: None)
    failures: list[str] = []

    passed = asyncio.run(
        client_helpers.run_async_target("test-async", "native", check, 0, failures)
    )

    assert passed == 3
    assert failures == []
    assert events == [
        ("request", "message-0"),
        ("sleep", 2.0),
        ("request", "message-1"),
        ("sleep", 2.0),
        ("request", "message-2"),
    ]


def test_async_target_cadence_is_generic_and_keeps_ten_requests_per_target() -> None:
    source = (E2E / "cynapsa_e2e" / "client_helpers.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "run_async_target"
    )
    assert 'MESSAGES = tuple(f"message-{index}" for index in range(10))' in source
    assert "os.environ" not in ast.unparse(function)
    assert "transport" not in ast.unparse(function)
    assert "retry" not in ast.unparse(function)
    loop = function.body[0]
    assert isinstance(loop, ast.For)
    assert ast.unparse(loop.target) == "(index, text)"
    assert ast.unparse(loop.iter) == "enumerate(MESSAGES)"
    assert isinstance(loop.body[0], ast.If)
    assert ast.unparse(loop.body[0].test) == "index > 0"
    assert ast.unparse(loop.body[0].body[0]) == "await asyncio.sleep(2.0)"
    assert isinstance(loop.body[1], ast.Assign)


def test_fault_campaign_rpc_timeout_is_validated_and_explicit_bounds_win(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CYNAPSA_USERNAME", "test@mesh.test")
    monkeypatch.setenv("CYNAPSA_PASSWORD", "test")
    monkeypatch.delenv("CYNAPSA_E2E_RPC_TIMEOUT_MS", raising=False)
    assert settings.auth()["command_timeout_ms"] == 30_000
    assert settings.auth()["rpc_timeout_ms"] == 30_000

    monkeypatch.setenv("CYNAPSA_E2E_RPC_TIMEOUT_MS", "60000")
    assert settings.auth()["command_timeout_ms"] == 30_000
    assert settings.auth()["rpc_timeout_ms"] == 60_000
    assert settings.auth(timeout_ms=10_000)["command_timeout_ms"] == 10_000
    assert settings.auth(timeout_ms=10_000)["rpc_timeout_ms"] == 10_000


@pytest.mark.parametrize("value", ["", "abc", "0", "-1", "9223372036855"])
def test_fault_campaign_rpc_timeout_rejects_invalid_values(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("CYNAPSA_E2E_RPC_TIMEOUT_MS", value)
    with pytest.raises(RuntimeError, match="CYNAPSA_E2E_RPC_TIMEOUT_MS"):
        settings.configured_rpc_timeout_ms()


def test_async_fault_campaign_clients_share_only_the_rpc_timeout_margin() -> None:
    compose = (E2E / "compose.yaml").read_text(encoding="utf-8")
    assert compose.count('CYNAPSA_E2E_RPC_TIMEOUT_MS: "60000"') == 1
    assert compose.count("*fault-campaign-client-environment") == 2

    native = (E2E / "cynapsa_e2e" / "native_async_client.py").read_text(
        encoding="utf-8"
    )
    monkey = (E2E / "cynapsa_e2e" / "monkey_async_client.py").read_text(
        encoding="utf-8"
    )
    assert "session.request" in native
    assert "ttl_ms=" not in native
    assert "timeout=configured_rpc_timeout_ms() / 1000" in monkey


def test_compose_uses_sidecar_network_namespaces_and_limits_net_admin() -> None:
    source = (E2E / "compose.yaml").read_text(encoding="utf-8")
    for service in (
        "ejabberd",
        "native-server",
        "monkey-server",
        "native-sync-client",
        "native-async-client",
        "monkey-sync-client",
        "monkey-async-client",
    ):
        assert f"network_mode: service:{service}-net" in source
    # Seven application/ejabberd LANs plus isolated STUN and TURN LANs.
    assert source.count("CYNAPSA_VLAN_ID:") == 9
    assert source.count("cap_add: [NET_ADMIN]") == 2
    assert "cap_add: [NET_ADMIN," not in source
    assert "CYNAPSA_E2E_ROLE" not in source


def test_rendered_compose_has_exact_namespace_privilege_and_vlan_contract(
    tmp_path: Path,
) -> None:
    if shutil.which("docker") is None:
        pytest.skip("Docker CLI is unavailable")
    compose_version = subprocess.run(
        ["docker", "compose", "version"],
        check=False,
        capture_output=True,
        text=True,
    )
    if compose_version.returncode != 0:
        pytest.skip("Docker Compose is unavailable")
    environment = {
        **os.environ,
        "CYNAPSA_E2E_RUNTIME_DIR": str(tmp_path),
            "CYNAPSA_GO_CORE_PATH": str(ROOT),
            "CYNAPSA_EJABBERD_PATH": str(DEDICATED_EJABBERD),
        "CYNAPSA_E2E_IMAGE_TAG": "config-test",
        "NATIVE_SERVER_PASSWORD": "test",
        "MONKEY_SERVER_PASSWORD": "test",
        "NATIVE_SYNC_CLIENT_PASSWORD": "test",
        "NATIVE_ASYNC_CLIENT_PASSWORD": "test",
        "MONKEY_SYNC_CLIENT_PASSWORD": "test",
        "MONKEY_ASYNC_CLIENT_PASSWORD": "test",
    }
    completed = subprocess.run(
        [
            "docker",
            "compose",
            "--profile",
            "clients",
            "-f",
            str(E2E / "compose.yaml"),
            "config",
            "--format",
            "json",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )
    services = json.loads(completed.stdout)["services"]
    applications = {
        "ejabberd": "ejabberd-net",
        "native-server": "native-server-net",
        "monkey-server": "monkey-server-net",
        "native-sync-client": "native-sync-client-net",
        "native-async-client": "native-async-client-net",
        "monkey-sync-client": "monkey-sync-client-net",
        "monkey-async-client": "monkey-async-client-net",
    }
    for application, sidecar in applications.items():
        config = services[application]
        assert config["network_mode"] == f"service:{sidecar}"
        assert config.get("networks") is None
        assert config.get("privileged") in {None, False}
        assert config["cap_drop"] == ["ALL"]
        assert "NET_ADMIN" not in config.get("cap_add", [])
        assert config["read_only"] is True
        assert "no-new-privileges:true" in config["security_opt"]

    for application in applications.keys() - {"ejabberd"}:
        assert "CYNAPSA_ICE_TRANSPORT_POLICY" not in services[application]["environment"]

    sidecars = {
        "native-sync-client-net": ("101", "10.240.10.2/24", "10.240.10.1"),
        "native-async-client-net": ("102", "10.240.20.2/24", "10.240.20.1"),
        "monkey-sync-client-net": ("103", "10.240.30.2/24", "10.240.30.1"),
        "monkey-async-client-net": ("104", "10.240.40.2/24", "10.240.40.1"),
        "native-server-net": ("105", "10.240.50.2/24", "10.240.50.1"),
        "monkey-server-net": ("106", "10.240.60.2/24", "10.240.60.1"),
        "ejabberd-net": ("107", "10.240.70.2/24", "10.240.70.1"),
    }
    for sidecar, (vlan, address, gateway) in sidecars.items():
        config = services[sidecar]
        assert config.get("privileged") in {None, False}
        assert config["cap_add"] == ["NET_ADMIN"]
        assert config["cap_drop"] == ["ALL"]
        assert config["read_only"] is True
        assert "no-new-privileges:true" in config["security_opt"]
        assert config["sysctls"]["net.ipv4.tcp_retries2"] == "1"
        assert config["environment"]["CYNAPSA_VLAN_ID"] == vlan
        assert config["environment"]["CYNAPSA_LAN_ADDRESS"] == address
        assert config["environment"]["CYNAPSA_LAN_GATEWAY"] == gateway
        assert set(config["networks"]) == {"trunk"}

    router = services["router"]
    assert router.get("privileged") in {None, False}
    assert router["cap_add"] == ["NET_ADMIN"]
    assert router["cap_drop"] == ["ALL"]
    assert router["read_only"] is True
    assert set(router["networks"]) == {"trunk"}


def test_agent_image_is_unprivileged_and_has_no_network_entrypoint() -> None:
    source = (E2E / "Dockerfile.agent").read_text(encoding="utf-8")
    assert "USER 10001:10001" in source
    assert "ENTRYPOINT" not in source
    assert "iproute2" not in source
    assert "iptables" not in source
    assert "network-entrypoint" not in source


def test_runner_preserves_signal_failure_status_and_avoids_parallel_clients() -> None:
    source = (E2E / "run.sh").read_text(encoding="utf-8")
    assert "trap 'cleanup 130' INT" in source
    assert "trap 'cleanup 143' TERM" in source
    assert "CYNAPSA_E2E_CLIENT_EXECUTION" not in source


def test_runner_quiesces_the_recovered_server_between_fault_campaigns() -> None:
    source = (E2E / "run.sh").read_text(encoding="utf-8")
    assert "wait_for_resource_quiescent native-server 12" in source
    assert "ejabberd_resource_ready" in source
    assert source.index("wait_for_resource_quiescent native-server 12") < source.rindex(
        "install_monkey_async_xmpp_rules"
    )


def test_short_resume_campaigns_are_environment_only_and_cover_server_and_client() -> None:
    source = (E2E / "run.sh").read_text(encoding="utf-8")
    apps = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted((E2E / "cynapsa_e2e").glob("*.py"))
    )
    assert "run_client_with_short_resume()" in source
    assert (
        'run_client_with_short_resume "$client" monkey monkey-server-net '
        "\\\n        short-resume-monkey-server monkey-server"
    ) in source
    assert (
        'run_client_with_short_resume "$client" monkey monkey-sync-client-net '
        "\\\n        short-resume-monkey-sync-client monkey-sync-client"
    ) in source
    assert "short-resume" not in apps
    assert 'blackhole_service "$fault_service" "$label"' in source
    assert 'restore_service_network "$fault_service" "$label"' in source
    assert (
        "delay_service_xmpp_flow monkey-server-net 10ms "
        "monkey-server-short-resume-delay.txt"
    ) in source
    assert source.count(
        "monkey-server-net 500ms monkey-server-short-resume-delay.txt"
    ) == 2
    assert source.count(
        "native-server-net 500ms native-server-response-qdisc.txt"
    ) == 3
    assert source.count(
        "monkey-server-net 10ms monkey-server-short-resume-delay.txt"
    ) == 3
    assert "router-drop-out" in source
    assert "router-drop-in" in source
    assert "successful XEP-0198 resume" in source


def test_short_resume_uses_semantic_authority_and_lifecycle_counters() -> None:
    source = (E2E / "run.sh").read_text(encoding="utf-8")
    trace = dedicated_ejabberd_file("test/sdk-e2e/authority-trace-start.eval")
    readme = (E2E / "README.md").read_text(encoding="utf-8")
    campaign = source[
        source.index("run_client_with_short_resume()") : source.index(
            "run_client_with_fault()"
        )
    ]
    assert "ejabberd_resume_pending_count" in campaign
    assert "authority_trace_count authority_discovery" in campaign
    assert "authority_trace_count authority_snapshot" in campaign
    assert "authority_trace_count resume_hook" in campaign
    assert "authority_trace_count resume_authority_ready" in campaign
    assert "authority_trace_count resume_authority_not_ready" in campaign
    assert "after_discovery -eq $before_discovery" in campaign
    assert "after_snapshot -eq $before_snapshot" in campaign
    assert "after_expired -eq $before_expired" in campaign
    assert "after_ready -eq $((before_ready + 1))" in campaign
    assert "after_resume_hook -eq $((before_resume_hook + 1))" in campaign
    assert "recovery_auth_delta -eq 1" in campaign
    assert "replacement_bound_session_delta=0" in campaign
    assert "pre_resume_scram_reconnect_delta=%s" in campaign
    fault_ready = campaign.index('wait_for_resource_ready "$user"')
    client_ready = campaign.index('wait_for_resource_ready "$client"')
    discovery_baseline = campaign.index(
        "before_discovery=$(authority_trace_count authority_discovery)"
    )
    snapshot_baseline = campaign.index(
        "before_snapshot=$(authority_trace_count authority_snapshot)"
    )
    request_baseline = campaign.index("request_baseline=$(request_event_count")
    assert fault_ready < client_ready < discovery_baseline < snapshot_baseline
    assert client_ready < request_baseline < campaign.index(
        "in-flight RPC dispatch observed"
    )
    assert "Authority trace counters are global" in campaign
    assert "request_message_event_count" in campaign
    assert "500-millisecond-delayed native-server RPCs" in campaign
    assert campaign.index("resumable-stream transition") < campaign.index(
        'restore_service_network "$fault_service" "$label"'
    ) < campaign.index("successful XEP-0198 resume") < campaign.index(
        "ready resume-authority result"
    ) < campaign.index('wait "$client_pid"')
    readme_text = " ".join(readme.split())
    assert (
        "the harness waits for the pre-existing fault target and the newly "
        "started active client to have open, authority-ready resources before "
        "recording the campaign baseline"
    ) in readme_text
    assert (
        "every participant that can still move those global counters in the "
        "campaign is therefore already ready"
    ) in readme_text
    assert (
        "resume-time external-service replay or requery"
    ) in readme_text
    assert (
        "it is distinct from Cynapsa mesh-authority discovery"
    ) in readme_text
    for function in (
        "disco_local_features",
        "process_group_iq",
        "c2s_session_resumed",
        "send_resume_authority_result",
    ):
        assert function in trace
    assert "resume_authority_ready" in trace
    assert "resume_authority_not_ready" in trace
    assert trace.index("Tracer = spawn(") < trace.index(
        "ets:new(cynapsa_e2e_authority_trace_counts"
    ) < trace.index("Parent ! {self(), ready}")
    assert trace.index("Parent ! {self(), ready}") < trace.index(
        "erlang:trace_pattern("
    )
    assert "authority trace counter $key is unavailable" in source
    assert "authority_trace_available" in source


def test_short_resume_window_covers_pre_bind_tls_and_sasl_reconnect() -> None:
    config = dedicated_ejabberd_file("test/sdk-e2e/legacy.yml")
    source = (E2E / "run.sh").read_text(encoding="utf-8")
    readme = (E2E / "README.md").read_text(encoding="utf-8")
    assert "resume_timeout: 15" in config
    assert "xep0198_resume_timeout_seconds=15" in source
    assert "pre-resume SCRAM reconnect" in readme
    assert "new bound session" in readme


def test_short_resume_retains_original_exact_workload_contract() -> None:
    source = (E2E / "run.sh").read_text(encoding="utf-8")
    verifier = (E2E / "verify_results.py").read_text(encoding="utf-8")
    assert "PASS 80/80" in source
    assert 'printf \'original_workload_passed_exactly_once=1\\n\'' in source
    assert "total != 80" in verifier
    assert "count != 1" in verifier
    assert "dispatches\": len(native)" in verifier


def test_filtered_run_reports_only_executed_short_resume_campaigns() -> None:
    source = (E2E / "run.sh").read_text(encoding="utf-8")
    campaign_total = (
        '"$((DEEP_EVIDENCE_SERVER_RESUME + DEEP_EVIDENCE_CLIENT_RESUME))"'
    )
    assert "short_resume_pre_resume_scram_reconnect_delta=%s" in source
    assert "short_resume_authority_barrier_ready_delta=%s" in source
    assert source.count(campaign_total) == 2
    assert "short_resume_pre_resume_scram_reconnect_delta=2" not in source
    assert "short_resume_authority_barrier_ready_delta=2" not in source


def test_core_provenance_records_dirty_and_untracked_build_inputs(tmp_path: Path) -> None:
    core = tmp_path / "core"
    core.mkdir()
    subprocess.run(["git", "init", "-q", str(core)], check=True)
    subprocess.run(["git", "-C", str(core), "config", "user.name", "E2E"], check=True)
    subprocess.run(
        ["git", "-C", str(core), "config", "user.email", "e2e@example.test"],
        check=True,
    )
    (core / "tracked.go").write_text("package core\n", encoding="utf-8")
    (core / ".gitignore").write_text("ignored.bin\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(core), "add", "."], check=True)
    subprocess.run(["git", "-C", str(core), "commit", "-qm", "base"], check=True)
    (core / "tracked.go").write_text("package changed\n", encoding="utf-8")
    (core / "untracked.go").write_text("package extra\n", encoding="utf-8")
    (core / "ignored.bin").write_bytes(b"ignored")
    output = tmp_path / "core-provenance.json"

    subprocess.run(
        [
            sys.executable,
            str(E2E / "record_core_provenance.py"),
            str(core),
            str(output),
        ],
        check=True,
    )

    manifest = json.loads(output.read_text(encoding="utf-8"))
    paths = {entry["path"] for entry in manifest["files"]}
    assert manifest["schema_version"] == 1
    assert manifest["head"] == subprocess.run(
        ["git", "-C", str(core), "rev-parse", "HEAD"],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout.strip()
    assert "tracked.go" in paths
    assert "untracked.go" in paths
    assert "ignored.bin" not in paths
    assert manifest["source_manifest_sha256"]
    assert any(line.endswith(" tracked.go") for line in manifest["status_porcelain_v1"])


def test_sdk_has_no_ejabberd_image_or_server_fixtures() -> None:
    compose = (E2E / "compose.yaml").read_text(encoding="utf-8")
    overlay = (E2E / "compose.peer-authority.yaml").read_text(encoding="utf-8")
    runner = (E2E / "run-peer-authority.sh").read_text(encoding="utf-8")
    assert "context: ${CYNAPSA_EJABBERD_PATH:" in compose
    assert "dockerfile: Dockerfile" in compose
    ejabberd_overlay = overlay.split("  enrollment:", 1)[0]
    assert "dockerfile:" not in ejabberd_overlay
    assert not list(E2E.glob("Dockerfile.ejabberd*"))
    assert not (E2E / "ejabberd-entrypoint.sh").exists()
    assert not list(E2E.glob("ejabberd*.yml"))
    assert not (E2E / "authority-trace-start.eval").exists()
    assert '"$EJABBERD/test/sdk-e2e/peer-authority.yml"' in runner
    assert '"$ARTIFACTS/ejabberd-provenance.json"' in runner


def test_ejabberd_image_builds_dedicated_authority_with_provenance() -> None:
    dockerfile = dedicated_ejabberd_file("Dockerfile")
    assert "FROM ghcr.io/processone/ejabberd@sha256:" in dockerfile
    assert "COPY src/*.erl" in dockerfile


def test_xep0215_advertises_mixed_stun_and_authenticated_turn() -> None:
    config = dedicated_ejabberd_file("test/sdk-e2e/legacy.yml")
    authority = config[config.index("  mod_stun_disco:") : config.index("  mod_shared_roster:")]
    assert 'host: "10.240.90.2"' in authority
    assert "restricted: true" in authority
    assert "type: turn" in authority
    assert 'host: "10.240.80.2"' in authority
    assert "type: stun" in authority
    assert authority.count("type: stun") == 1
    assert authority.count("type: turn") == 1


def test_no_application_overrides_core_ice_policy() -> None:
    source = (E2E / "compose.yaml").read_text(encoding="utf-8")
    native_async = source[
        source.index("  native-async-client:") : source.index("  monkey-sync-client:")
    ]
    native_server = source[
        source.index("  native-server:") : source.index("  monkey-server:")
    ]
    monkey_server = source[
        source.index("  monkey-server:") : source.index("  native-sync-client:")
    ]
    assert "CYNAPSA_ICE_TRANSPORT_POLICY" not in source
    assert "CYNAPSA_ICE_TRANSPORT_POLICY" not in native_async
    assert "CYNAPSA_ICE_TRANSPORT_POLICY" not in native_server
    assert "CYNAPSA_ICE_TRANSPORT_POLICY" not in monkey_server


def test_native_async_udp_policy_allows_stun_and_turn_only() -> None:
    source = (E2E / "run.sh").read_text(encoding="utf-8")
    rules = source[
        source.index("install_native_async_turn_rules()") : source.index(
            "install_monkey_async_xmpp_rules()"
        )
    ]
    probe = source[
        source.index("run_native_direct_rejection_probe()") : source.index(
            "require docker"
        )
    ]

    stun_out = "-p udp -d 10.240.80.2 --dport 3478"
    stun_in = "-p udp -s 10.240.80.2 --sport 3478"
    turn_out = "-p udp -d 10.240.90.2 --dport 3478"
    turn_in = "-p udp -s 10.240.90.2 --sport 3478"
    catchall_out = "-p udp -m comment --comment e2e-native-async-other-udp-out -j DROP"
    catchall_in = "-p udp -m comment --comment e2e-native-async-other-udp-in -j DROP"
    assert stun_out in rules
    assert stun_in in rules
    assert turn_out in rules
    assert turn_in in rules
    assert '0>>22&0x3C@8>>30&0x3=1' in rules
    assert "e2e-native-async-turn-channeldata-out" in rules
    assert "e2e-native-async-turn-channeldata-in" in rules
    assert catchall_out in rules
    assert catchall_in in rules
    assert rules.index(stun_out) < rules.index(turn_out) < rules.index(catchall_out) < rules.index(
        "iptables -A E2E_NATIVE_ASYNC_OUT -j RETURN"
    )
    assert rules.index(stun_in) < rules.index(turn_in) < rules.index(catchall_in) < rules.index(
        "iptables -A E2E_NATIVE_ASYNC_IN -j RETURN"
    )
    assert "-s 10.240.20.2 -p udp -j E2E_NATIVE_ASYNC_NAT" in rules
    assert "UDP:10.240.80.2:3478" in probe
    assert "UDP:10.240.70.2:39002" in probe
    assert "e2e-native-async-stun-out" in rules
    assert "e2e-native-async-stun-in" in rules
    assert "e2e-native-async-other-udp-out" in source

    readme = (E2E / "README.md").read_text(encoding="utf-8")
    assert "STUN discovery and authenticated TURN" in readme
    assert "ordinary `all` ICE policy" in readme
    assert "TURN the sole viable" in readme
    assert "TCP/5222 remains available" in readme


def test_native_async_exact_turn_warmup_transitions_directly_to_server_fault() -> None:
    source = (E2E / "run.sh").read_text(encoding="utf-8")
    warmup = source[
        source.index("establish_native_async_turn_rpc()") : source.index(
            "run_client_plain()"
        )
    ]
    fault_runner = source[
        source.index("run_client_with_fault()") : source.index("counter_value()")
    ]

    assert "while (( request_index <= 6 ))" in warmup
    assert "dispatch_after -eq $((dispatch_before + 1))" in warmup
    assert "request_mailbox_after -eq $request_mailbox_before" in warmup
    assert "response_mailbox_after -eq $response_mailbox_before" in warmup
    assert "request_turn_data_packets -gt 0" in warmup
    assert "request_turn_data_bytes -gt 0" in warmup
    assert "response_turn_data_packets -gt 0" in warmup
    assert "response_turn_data_bytes -gt 0" in warmup
    assert "request_rank2_mailbox_admission_delta=0" in warmup
    assert "response_rank2_mailbox_admission_delta=0" in warmup
    assert 'delay_service_network "$shaping_service" 10ms' in fault_runner
    assert 'delay_service_network "$shaping_service" 500ms' in fault_runner
    assert fault_runner.index("establish_native_async_turn_rpc") < fault_runner.index(
        'pause_client_run_container "$client"'
    ) < fault_runner.index("change_service_xmpp_flow_delay") < fault_runner.index(
        'unpause_client_run_container "$client"'
    ) < fault_runner.index("fault_index=$started_count") < fault_runner.index("blackhole_service")
    assert "started_count=$(request_event_count" in fault_runner
    assert "fault_index=$started_count" in fault_runner
    assert "native-async-turn-warmup-before.txt" in source
    assert "native-async-turn-warmup-after.txt" in source
    assert "native-async-turn-warmup-evidence.txt" in source
    assert "native-async-turn-rpc-before.txt" not in source
    assert "native-async-turn-rpc-after.txt" not in source
    assert "native-async-turn-rpc-evidence.txt" not in source
    assert "install_native_async_xmpp_blackhole" not in source
    assert "remove_native_async_xmpp_blackhole" not in source
    assert "prove_native_async_turn_rpc" not in source
    assert "controlled TURN RPC" not in source
    assert "standalone_stun_role=advertised_discovery_only" in source
    assert "core_ice_policy=all" in source
    assert "connectivity_and_data_path=turn_relay_forced_by_network" in source
    assert "native_async_standalone_stun_advertised=1" in source
    assert "native_async_core_ice_policy=all" in source
    assert "native_async_connectivity_and_data_path=turn_relay_forced_by_network" in source
    assert "native_async_exact_turn_warmup_rpc" in source

    readme = (E2E / "README.md").read_text(encoding="utf-8")
    readme_text = " ".join(readme.split())
    assert "bounded sequence of ordinary RPCs" in readme_text
    assert "no matching exact-resource Rank2 mailbox" in readme_text
    assert "positive bidirectional TURN ChannelData" in readme_text
    assert "exact warm-up RPC is the relay application-carriage proof" in readme_text
    assert "no separate XMPP-blackout proof" in readme_text
    assert "reserves the next ordinary RPC" in readme_text


def test_fault_timing_assertion_tracks_the_rpc_held_by_the_environment() -> None:
    source = (E2E / "run.sh").read_text(encoding="utf-8")
    assertion = source[
        source.index("assert_faulted_request_elapsed()") : source.index(
            "run_client_plain()"
        )
    ]
    assert 'event.get("message") == "message-0"' not in assertion
    assert "len(passes) != 10" in assertion
    assert "12_000 <= event[\"elapsed_ms\"] < 60_000" in assertion
    assert "len(matches) != 1" in assertion


def test_response_delay_uses_one_fifo_for_the_complete_xmpp_flow() -> None:
    source = (E2E / "run.sh").read_text(encoding="utf-8")
    assert "delay_service_xmpp_payload" not in source
    assert "--length 129:65535" not in source
    assert "-p tcp --dport 5222" in source
    assert "-j MARK --set-mark 0xc1" in source
    assert "tc qdisc add dev cynapsa-lan root handle 1: prio" in source
    assert "parent 1:1 handle 10: netem limit 10000" in source
    assert "handle 0xc1 fw flowid 1:1" in source
    assert "grep -Eq '(classid|flowid) 1:1'" in source
    assert "matchall flowid 1:2" in source
    assert 'delay_service_xmpp_flow native-server-net 10ms native-server-response-qdisc.txt' in source
    assert "change_service_xmpp_flow_delay" in source
    assert '"$response_service" 1s "$fault_label-response-qdisc.txt"' in source
    assert '500ms native-server-recovery-response-qdisc.txt' in source
    assert 'delay_service_network "$response_service" 1s' not in source
    assert source.index(
        "delay_service_xmpp_flow native-server-net 10ms native-server-response-qdisc.txt"
    ) < source.index("compose up -d native-server monkey-server")
    assert "validated_xmpp_flow_delay_state" in source
    assert "tc -s -d qdisc show dev cynapsa-lan" in source
    assert "tc -s -d filter show dev cynapsa-lan parent 1:" in source
    installer = source[
        source.index("delay_service_xmpp_flow()") : source.index(
            "capture_service_filter()"
        )
    ]
    assert "set -eu" in installer
    assert installer.index("tc qdisc add") < installer.index(
        "record_xmpp_flow_delay_state"
    )
    assert '"$fault_label-response-qdisc.txt" dispatch-observed' in source
    changer = source[
        source.index("change_service_xmpp_flow_delay()") : source.index(
            "capture_service_filter()"
        )
    ]
    assert "tc qdisc change dev cynapsa-lan parent 1:1 handle 10: netem" in changer
    assert "tc qdisc del" not in changer
    fault_runner = source[
        source.index("run_client_with_fault()") : source.index("counter_value()")
    ]
    assert fault_runner.index("establish_native_async_turn_rpc") < fault_runner.index(
        'pause_client_run_container "$client"'
    ) < fault_runner.index("change_service_xmpp_flow_delay") < fault_runner.index(
        'fault_index=$started_count'
    ) < fault_runner.index('blackhole_service "$fault_service"')
    assert "response_delay=10ms" in fault_runner
    assert "response_delay=500ms" in fault_runner
    assert "response_delay=1s" in fault_runner
    assert fault_runner.index("dispatch observed count=$dispatch_after") < fault_runner.index(
        '"$fault_label-response-qdisc.txt" dispatch-observed'
    ) < fault_runner.index('blackhole_service "$fault_service"')
    assert fault_runner.index(
        'record_fault_socket_state "$fault_service" "$fault_label"'
    ) < fault_runner.index(
        '500ms native-server-recovery-response-qdisc.txt'
    ) < fault_runner.rindex(
        'restore_service_network "$fault_service" "$fault_label"'
    )
    assert "assert_fault_socket_closed" not in source
    blackhole = source[
        source.index("blackhole_service()") : source.index(
            "diagnostic_xmpp_socket_count()"
        )
    ]
    assert "established_before_blackhole" in blackhole
    assert "established_before_blackhole -lt 1" not in blackhole
    assert "without an established XMPP socket" not in blackhole
    assert blackhole.index("iptables -I FORWARD") < blackhole.index(
        "iptables -I INPUT"
    ) < blackhole.index("ss -Hnt state established")
    socket_counter = source[
        source.index("diagnostic_xmpp_socket_count()") : source.index(
            "record_fault_socket_state()"
        )
    ]
    assert 'sockets=$(ss -Hnt state established' in socket_counter
    assert '|| exit $?' in socket_counter
    assert 'printf "0\\n"' in socket_counter
    assert "| wc -l" not in socket_counter
    socket_recorder = source[
        source.index("record_fault_socket_state()") : source.index(
            "restore_service_network()"
        )
    ]
    assert "established_during_blackhole" in socket_recorder
    assert "diagnostic_xmpp_socket_count" in socket_recorder
    assert 'test "$after" -eq 0' not in socket_recorder
    assert "established_during_blackhole -lt 1" not in socket_recorder
    dispatch_qdisc_observed = fault_runner.index(
        '"$fault_label-response-qdisc.txt" dispatch-observed'
    )
    active_dispatch = fault_runner.index(
        'timeline "$client active $target rpc observed"'
    )
    expiry_baseline = fault_runner.index("expired_before=$(ejabberd_log_count")
    blackhole_install = fault_runner.index(
        'blackhole_service "$fault_service" "$fault_label"'
    )
    newer_expiry = fault_runner.index("expired_after=$(wait_for_count_above")
    assert (
        dispatch_qdisc_observed
        < active_dispatch
        < expiry_baseline
        < blackhole_install
        < newer_expiry
    )
    assert newer_expiry < fault_runner.index(
        'record_fault_socket_state "$fault_service" "$fault_label"'
    )
    assert fault_runner.index(
        'connected=$(ejabberd_session_count "$user")',
        fault_runner.index("expired_after="),
    ) < fault_runner.rindex(
        'restore_service_network "$fault_service" "$fault_label"'
    )
    assert "e2e-$label-router-drop-out" in source
    assert "e2e-$label-router-drop-in" in source
    assert "clear_network_delay \"$response_service\"" not in source


def test_runner_focused_mode_starts_the_sidecars_it_uses() -> None:
    source = (E2E / "run.sh").read_text(encoding="utf-8")
    focused_clients = (
        "if [[ ${CYNAPSA_E2E_FOCUSED:-0} == 1 ]]; then\n"
        "  CLIENTS=(native-sync-client native-async-client)\n"
        "fi"
    )
    assert source.index(focused_clients) > source.index("CYNAPSA_E2E_CLIENT_FILTER")
    assert source.index(focused_clients) < source.index("client_sidecars")
    first_sequence = "run_sequence native-then-monkey 'native,monkey' native-sync-client"
    second_sequence = "run_sequence monkey-then-native 'monkey,native' native-async-client"
    assert source.index(first_sequence) < source.index("sleep 5") < source.index(second_sequence)


def test_sequence_client_returns_after_all_target_groups() -> None:
    tree = ast.parse(
        (E2E / "cynapsa_e2e" / "sequence_client.py").read_text(encoding="utf-8")
    )
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "main"
    )
    target_loop = next(node for node in ast.walk(function) if isinstance(node, ast.For))
    assert not any(isinstance(node, ast.Return) for node in ast.walk(target_loop))
    assert isinstance(function.body[-1], ast.Return)


def _write_server_logs(tmp_path: Path) -> tuple[Path, Path]:
    native = tmp_path / "native-server.log"
    monkey = tmp_path / "monkey-server.log"
    native_records = [
        {
            "event": "native-dispatch",
            "payload_type": payload_type,
            "body_bytes": 9,
        }
        for payload_type in ("CynapsaRequest",)
        for _ in range(40)
    ]
    monkey_records = [
        {"event": "asgi-dispatch", "path": "/reverse", "body_bytes": 9}
        for _ in range(40)
    ]
    native.write_text(
        "\n".join(
            "native-server-1 | E2E_DIAGNOSTIC=" + json.dumps(record)
            for record in native_records
        )
        + "\n",
        encoding="utf-8",
    )
    monkey.write_text(
        "\n".join(
            "monkey-server-1 | E2E_DIAGNOSTIC=" + json.dumps(record)
            for record in monkey_records
        )
        + "\n",
        encoding="utf-8",
    )
    return native, monkey


def _verifier_command(tmp_path: Path, paths: list[Path]) -> list[str]:
    native, monkey = _write_server_logs(tmp_path)
    return [
        sys.executable,
        str(E2E / "verify_results.py"),
        "--native-server-log",
        str(native),
        "--monkey-server-log",
        str(monkey),
        *map(str, paths),
    ]


def test_result_verifier_accepts_exact_80_and_rejects_79(tmp_path: Path) -> None:
    clients = ("native-sync", "native-async", "monkey-sync", "monkey-async")
    paths: list[Path] = []
    for client in clients:
        path = tmp_path / f"{client}.log"
        report = {
            "client": client,
            "passed": 20,
            "failed": 0,
            "expected": 20,
            "failures": [],
        }
        diagnostics = []
        for target in ("native", "monkey"):
            for index in range(10):
                message = f"message-{index}"
                for event in ("request-start", "request-pass"):
                    diagnostics.append(
                        "E2E_DIAGNOSTIC="
                        + json.dumps(
                            {"event": event, "target": target, "message": message}
                        )
                    )
        path.write_text(
            "\n".join([*diagnostics, f"E2E_RESULT={json.dumps(report)}", ""]),
            encoding="utf-8",
        )
        paths.append(path)

    command = _verifier_command(tmp_path, paths)
    completed = subprocess.run(command, check=True, capture_output=True, text=True)
    summary = json.loads(completed.stdout)
    assert summary["passed"] == 80
    assert summary["servers"]["native"]["dispatches"] == 40
    assert summary["servers"]["monkey"]["dispatches"] == 40

    hop_command = [*command[:2], "--require-hop", *command[2:]]
    assert subprocess.run(hop_command, capture_output=True, text=True).returncode != 0
    hop_checks = ["/forward", "/remap", "/unhandled"]
    for client, path in zip(clients, paths):
        if client in {"native-sync", "monkey-sync"}:
            with path.open("a", encoding="utf-8") as stream:
                stream.write(
                    "E2E_HOP_RESULT="
                    + json.dumps({"client": client, "checks": hop_checks})
                    + "\n"
                )
    native_log = Path(command[command.index("--native-server-log") + 1])
    monkey_log = Path(command[command.index("--monkey-server-log") + 1])
    for log, records in (
        (native_log, [{"event": "hop-intermediate", "path": path} for path in hop_checks * 2]),
        (monkey_log, [{"event": "hop-upstream", "body": "hop-probe"} for _ in range(6)]),
    ):
        with log.open("a", encoding="utf-8") as stream:
            for record in records:
                stream.write("E2E_DIAGNOSTIC=" + json.dumps(record) + "\n")
    assert subprocess.run(hop_command, capture_output=True, text=True).returncode == 0

    lines = paths[0].read_text().splitlines()
    lines[1] += 'CYNAPSA_CORE_EVIDENCE {"event":"rank2_inbound_disposition"}'
    paths[0].write_text("\n".join([*lines, ""]), encoding="utf-8")
    interleaved = subprocess.run(command, check=True, capture_output=True, text=True)
    assert json.loads(interleaved.stdout)["passed"] == 80

    lines[1] += "junk"
    paths[0].write_text("\n".join([*lines, ""]), encoding="utf-8")
    malformed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert malformed.returncode != 0
    lines[1] = lines[1].removesuffix("junk")

    report_index = next(index for index, line in enumerate(lines) if line.startswith("E2E_RESULT="))
    failing = json.loads(lines[report_index].removeprefix("E2E_RESULT="))
    failing.update(passed=19, failed=1, failures=["synthetic"])
    lines[report_index] = f"E2E_RESULT={json.dumps(failing)}"
    paths[0].write_text("\n".join([*lines, ""]), encoding="utf-8")
    rejected = subprocess.run(command, check=False, capture_output=True, text=True)
    assert rejected.returncode != 0
    assert "invalid E2E report" in rejected.stderr


def test_result_verifier_rejects_missing_server_dispatch(tmp_path: Path) -> None:
    client_paths: list[Path] = []
    for client in ("native-sync", "native-async", "monkey-sync", "monkey-async"):
        path = tmp_path / f"{client}.log"
        diagnostics = []
        for target in ("native", "monkey"):
            for index in range(10):
                message = f"message-{index}"
                for event in ("request-start", "request-pass"):
                    diagnostics.append(
                        "E2E_DIAGNOSTIC="
                        + json.dumps(
                            {"event": event, "target": target, "message": message}
                        )
                    )
        report = {
            "client": client,
            "passed": 20,
            "failed": 0,
            "expected": 20,
            "failures": [],
        }
        path.write_text(
            "\n".join([*diagnostics, f"E2E_RESULT={json.dumps(report)}", ""]),
            encoding="utf-8",
        )
        client_paths.append(path)
    command = _verifier_command(tmp_path, client_paths)
    native_log = Path(command[command.index("--native-server-log") + 1])
    lines = native_log.read_text(encoding="utf-8").splitlines()
    native_log.write_text("\n".join(lines[:-1]) + "\n", encoding="utf-8")
    rejected = subprocess.run(command, check=False, capture_output=True, text=True)
    assert rejected.returncode != 0
    assert "native server dispatch mismatch" in rejected.stderr


def test_result_verifier_rejects_incomplete_target_matrix(tmp_path: Path) -> None:
    path = tmp_path / "native-sync.log"
    events = []
    for target in ("native", "monkey"):
        for index in range(10):
            message = "message-0" if target == "monkey" and index == 9 else f"message-{index}"
            for event in ("request-start", "request-pass"):
                events.append(
                    "E2E_DIAGNOSTIC="
                    + json.dumps({"event": event, "target": target, "message": message})
                )
    report = {
        "client": "native-sync",
        "passed": 20,
        "failed": 0,
        "expected": 20,
        "failures": [],
    }
    path.write_text(
        "\n".join([*events, f"E2E_RESULT={json.dumps(report)}", ""]),
        encoding="utf-8",
    )
    rejected = subprocess.run(
        _verifier_command(tmp_path, [path]),
        check=False,
        capture_output=True,
        text=True,
    )
    assert rejected.returncode != 0
    assert "matrix mismatch" in rejected.stderr
