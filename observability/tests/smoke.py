#!/usr/bin/env python3
"""Opt-in, credential-free local telemetry smoke test. No agent is launched."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request


ROOT = Path(__file__).resolve().parents[1]
IMAGES = {
    "otel-collector": "otel/opentelemetry-collector-contrib:0.123.0",
    "prometheus": "prom/prometheus:v3.2.1",
    "tempo": "grafana/tempo:2.7.2",
    "loki": "grafana/loki:3.4.2",
}


def compose_document(ports: dict[str, int], subnet: str | None = None) -> dict:
    services = {
        "otel-collector": {
            "image": IMAGES["otel-collector"],
            "command": ["--config=/etc/otelcol-contrib/config.yaml"],
            "volumes": [f"{ROOT / 'otel-collector.yaml'}:/etc/otelcol-contrib/config.yaml:ro"],
            "ports": [{"target": 4318, "published": str(ports["otel-collector"]), "host_ip": "127.0.0.1"}],
        },
        "prometheus": {
            "image": IMAGES["prometheus"],
            "command": ["--config.file=/etc/prometheus/prometheus.yml", "--storage.tsdb.path=/prometheus"],
            "volumes": [f"{ROOT / 'prometheus.yml'}:/etc/prometheus/prometheus.yml:ro", "/prometheus"],
            "ports": [{"target": 9090, "published": str(ports["prometheus"]), "host_ip": "127.0.0.1"}],
        },
        "tempo": {
            "image": IMAGES["tempo"],
            "command": ["-config.file=/etc/tempo/tempo.yaml"],
            "volumes": [f"{ROOT / 'tempo.yaml'}:/etc/tempo/tempo.yaml:ro", "/var/tempo"],
            "ports": [{"target": 3200, "published": str(ports["tempo"]), "host_ip": "127.0.0.1"}],
        },
        "loki": {
            "image": IMAGES["loki"],
            "command": ["-config.file=/etc/loki/loki.yaml"],
            "volumes": [f"{ROOT / 'loki.yaml'}:/etc/loki/loki.yaml:ro", "/loki"],
            "ports": [{"target": 3100, "published": str(ports["loki"]), "host_ip": "127.0.0.1"}],
        },
    }
    for service in services.values():
        service["restart"] = "no"
        service["networks"] = ["smoke"]
    network = {}
    if subnet:
        network["ipam"] = {"config": [{"subnet": subnet}]}
    return {"services": services, "networks": {"smoke": network}}


def available_ports() -> dict[str, int]:
    # Reserve distinct loopback ports until all are chosen. Compose may lose
    # the release/start race; in that case `up` fails and cleanup still runs.
    sockets = []
    try:
        for _ in IMAGES:
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return {name: sock.getsockname()[1] for name, sock in zip(IMAGES, sockets)}
    finally:
        for sock in sockets:
            sock.close()


def run(argv: list[str], *, cwd: Path | None = None, env: dict | None = None, timeout: int = 120) -> str:
    result = subprocess.run(argv, cwd=cwd, env=env, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        # Avoid forwarding arbitrary service logs, environment, or request bodies.
        raise RuntimeError(f"{argv[0]} command failed (exit {result.returncode})")
    return result.stdout.strip()


def http(method: str, url: str, payload: dict | None = None) -> dict:
    data = None if payload is None else json.dumps(payload, separators=(",", ":")).encode("ascii")
    headers = {"Content-Type": "application/json"} if data is not None else {}
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=3) as response:
        body = response.read(1_000_001)
        if len(body) > 1_000_000:
            raise RuntimeError("oversized local telemetry response")
        return json.loads(body) if body else {}


def wait_for(label: str, check, timeout: float = 45) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except (urllib.error.URLError, TimeoutError, ValueError, KeyError):
            pass
        time.sleep(0.7)
    raise RuntimeError(f"timed out waiting for {label}")


def query(base: str, expression: str) -> list:
    url = base + "/api/v1/query?" + urllib.parse.urlencode({"query": expression})
    result = http("GET", url)
    if result.get("status") != "success":
        raise RuntimeError("Prometheus query failed")
    return result["data"]["result"]


def otlp_metric(name: str, service: str, instance: str, value: int) -> dict:
    return {"resourceMetrics": [{
        "resource": {"attributes": [
            {"key": "service.name", "value": {"stringValue": service}},
            {"key": "service.instance.id", "value": {"stringValue": instance}},
        ]},
        "scopeMetrics": [{"scope": {"name": "cynapsa.smoke.synthetic"}, "metrics": [{
            "name": name, "gauge": {"dataPoints": [{
                "timeUnixNano": str(time.time_ns()), "asInt": str(value),
            }]},
        }]}],
    }]}


def synthetic_signals(collector: str, prom: str, tempo: str, loki: str, marker: str) -> None:
    service = "cynapsa-smoke-synthetic"
    trace_id, span_id = secrets.token_hex(16), secrets.token_hex(8)
    now = time.time_ns()
    attrs = [{"key": "service.name", "value": {"stringValue": service}}]
    http("POST", collector + "/v1/metrics", otlp_metric("cynapsa.smoke.synthetic_count", service, marker, 7))
    http("POST", collector + "/v1/traces", {"resourceSpans": [{
        "resource": {"attributes": attrs}, "scopeSpans": [{"scope": {"name": "cynapsa.smoke.synthetic"},
            "spans": [{"traceId": trace_id, "spanId": span_id, "name": "synthetic.smoke",
                       "startTimeUnixNano": str(now - 1_000_000), "endTimeUnixNano": str(now)}]}],
    }]})
    http("POST", collector + "/v1/logs", {"resourceLogs": [{
        "resource": {"attributes": attrs}, "scopeLogs": [{"scope": {"name": "cynapsa.smoke.synthetic"},
            "logRecords": [{"timeUnixNano": str(now), "body": {"stringValue": "synthetic-smoke-" + marker}}]}],
    }]})
    try:
        wait_for("synthetic metric in Prometheus", lambda: any(
            item["value"][1] == "7" for item in query(prom, 'cynapsa_smoke_synthetic_count')), timeout=20)
    except RuntimeError as exc:
        raise RuntimeError(f"{exc}; scrape up={query(prom, 'up')}; "
                           f"names={http('GET', prom + '/api/v1/label/__name__/values?match%5B%5D=%7B__name__%3D~%22cynapsa.%2A%22%7D').get('data')}") from exc
    matches = query(prom, 'cynapsa_smoke_synthetic_count')
    if not any(item["metric"].get("job") == service and item["metric"].get("instance") == marker for item in matches):
        raise RuntimeError(f"synthetic metric labels unexpected: {[item['metric'] for item in matches]}")
    wait_for("synthetic trace in Tempo", lambda: bool(http("GET", tempo + "/api/traces/" + trace_id).get("batches")))
    expression = '{service_name="' + service + '"} |= "synthetic-smoke-' + marker + '"'
    def log_present() -> bool:
        params = urllib.parse.urlencode({"query": expression, "start": str(now - 10_000_000_000),
                                         "end": str(time.time_ns() + 10_000_000_000)})
        return bool(http("GET", loki + "/loki/api/v1/query_range?" + params)["data"]["result"])
    wait_for("synthetic log in Loki", log_present)
    print("PASS synthetic Collector→Prometheus/Tempo/Loki (OTLP/JSON; unique marker)")


def mesh_fixture(script: Path, collector: str, prom: str, marker: str) -> None:
    spec = importlib.util.spec_from_file_location("cynapsa_mesh_smoke_exporter", script)
    if spec is None or spec.loader is None:
        raise RuntimeError("mesh exporter cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    endpoint = module.validate_endpoint(collector + "/v1/metrics", True)
    body = module.payload(1, tuple(range(1, 14)), 0, 0, marker, time.time_ns())
    if not module.export(endpoint, body, 3):
        raise RuntimeError("mesh exporter fixture delivery failed")
    wait_for("mesh fixture metric", lambda: any(
        item["value"][1] == "1" for item in query(prom,
            f'{{__name__=~"cynapsa_mesh_server_authority_ready.*",job="cynapsa-mesh-server",instance="{marker}"}}')))
    print("PASS mesh-server exporter encoder/sender→Collector→Prometheus (fixture; no ejabberd poll)")


def core_exporter(core: Path, collector: str, prom: str) -> None:
    env = os.environ.copy()
    env.update({"CYNAPSA_CORE_OTLP_METRICS_SMOKE": "1",
                "CYNAPSA_CORE_OTLP_METRICS_ENDPOINT": collector + "/v1/metrics",
                "CYNAPSA_CORE_OTLP_METRICS_INSECURE_DEV": "1"})
    run(["go", "test", ".", "-run", "^TestMetricsExporterRealCollectorSmoke$", "-count=1"],
        cwd=core, env=env, timeout=180)
    wait_for("Core exporter metric", lambda: bool(query(prom,
        '{__name__=~"cynapsa_core_rpc_timeout_count.*",job="cynapsa-core"}')))
    print("PASS real Core exporter smoke→Collector→Prometheus (test-local Core; no mesh)")


def sdk_exporter(sdk: Path, python: Path, collector: str, prom: str, tempo: str, loki: str) -> None:
    try:
        run([str(python), "-c", "import opentelemetry.sdk, opentelemetry.proto"], timeout=10)
    except RuntimeError as exc:
        raise RuntimeError("SDK [otel] extras unavailable in --sdk-python interpreter") from exc
    run([str(python), str(Path(__file__).with_name("sdk_fixture.py")),
         "--sdk-root", str(sdk), "--collector", collector], timeout=30)
    wait_for("SDK exporter metric", lambda: any(
        item["value"][1] == "1" for item in query(prom,
            'cynapsa_sdk_native_handler_started{job="cynapsa-sdk"}')))
    expression = '{service_name="cynapsa-sdk"} |= "outbound.native.request"'
    def log_present() -> bool:
        params = urllib.parse.urlencode({"query": expression, "since": "5m"})
        return bool(http("GET", loki + "/loki/api/v1/query_range?" + params)["data"]["result"])
    wait_for("SDK fixed-name event in Loki", log_present)
    def trace_present() -> bool:
        params = urllib.parse.urlencode({"tags": "service.name=cynapsa-sdk", "limit": 20})
        return bool(http("GET", tempo + "/api/search?" + params).get("traces"))
    wait_for("SDK process-local span in Tempo", trace_present)
    print("PASS real SDK exporters→Collector→Prometheus/Loki/Tempo (fake local handle; no agent)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true", help="explicitly start an isolated disposable Docker stack")
    parser.add_argument("--subnet", help="optional dedicated Docker subnet if default address pools are exhausted")
    parser.add_argument("--core-root", type=Path, help="optional local Go Core source; run its existing exporter smoke test")
    parser.add_argument("--sdk-root", type=Path, help="optional local Python SDK source; requires installed [otel] extras")
    parser.add_argument("--sdk-python", type=Path, default=Path(sys.executable), help="Python interpreter with SDK [otel] extras")
    parser.add_argument("--mesh-exporter", type=Path, help="optional local mesh-server exporter script; use fixture input")
    args = parser.parse_args()
    if not args.live:
        parser.error("nothing is started by default; pass --live explicitly")
    for path in (args.core_root, args.sdk_root, args.mesh_exporter):
        if path is not None and not path.exists():
            parser.error(f"optional source missing: {path}")
    marker = "smoke" + secrets.token_hex(5)
    project = "cynapsasmoke" + secrets.token_hex(5)
    def terminate(_signum, _frame):
        raise KeyboardInterrupt("smoke interrupted; cleaning up")
    previous_term = signal.signal(signal.SIGTERM, terminate)
    with tempfile.TemporaryDirectory(prefix="cynapsa-otel-smoke-") as temp:
        compose_file = Path(temp) / "compose.json"
        compose_file.write_text(json.dumps(compose_document(available_ports(), args.subnet)), encoding="utf-8")
        base = ["docker", "compose", "-p", project, "-f", str(compose_file)]
        try:
            run(base + ["config", "-q"])
            run(base + ["up", "-d", "--pull", "never"], timeout=180)
            def port(service: str, number: int) -> str:
                container = run(base + ["ps", "-q", service])
                if not container:
                    raise RuntimeError("smoke service container missing")
                inspected = json.loads(run(["docker", "inspect", container]))[0]
                bindings = inspected["NetworkSettings"]["Ports"].get(f"{number}/tcp") or []
                if len(bindings) != 1 or bindings[0]["HostIp"] != "127.0.0.1":
                    raise RuntimeError(f"smoke port is not bound exclusively to loopback: {service} "
                                       f"state={inspected['State']['Status']} "
                                       f"configured={inspected['HostConfig']['PortBindings']} actual={bindings}")
                return "http://127.0.0.1:" + bindings[0]["HostPort"]
            collector, prom, tempo, loki = (
                port("otel-collector", 4318), port("prometheus", 9090),
                port("tempo", 3200), port("loki", 3100))
            wait_for("Prometheus", lambda: http("GET", prom + "/api/v1/status/runtimeinfo").get("status") == "success")
            wait_for("Tempo", lambda: bool(http("GET", tempo + "/api/status/buildinfo")))
            wait_for("Loki", lambda: bool(http("GET", loki + "/loki/api/v1/status/buildinfo")))
            synthetic_signals(collector, prom, tempo, loki, marker)
            if args.mesh_exporter:
                mesh_fixture(args.mesh_exporter.resolve(), collector, prom, marker)
            else:
                print("SKIP mesh-server exporter fixture (pass --mesh-exporter); no real server exercised")
            if args.core_root:
                core_exporter(args.core_root.resolve(), collector, prom)
            else:
                print("SKIP Core exporter (pass --core-root)")
            if args.sdk_root:
                sdk_exporter(args.sdk_root.resolve(), args.sdk_python.resolve(), collector, prom, tempo, loki)
            else:
                print("SKIP SDK exporter (pass --sdk-root and interpreter with SDK [otel] extras)")
            print("GAP no real agent, real mesh delivery, or cross-agent trace exercised")
        finally:
            # Unique project; anonymous volumes only. Never down the user's stack.
            try:
                run(base + ["down", "--volumes"], timeout=90)
                print("CLEANUP isolated smoke containers/network/volumes removed")
            except Exception:
                print(f"CLEANUP FAILED: inspect Docker Compose project {project}", file=sys.stderr)
                raise
    signal.signal(signal.SIGTERM, previous_term)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"FAIL smoke: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1)
