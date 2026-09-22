#!/usr/bin/env python3
"""Validate the exact client matrix and matching server dispatches."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path


EXPECTED_CLIENTS = {
    "native-sync",
    "native-async",
    "monkey-sync",
    "monkey-async",
}
EXPECTED_TARGETS = {"native", "monkey"}
EXPECTED_MESSAGES = {f"message-{index}" for index in range(10)}


def _validate_log(path: Path) -> dict[str, object]:
    reports: list[dict[str, object]] = []
    events: Counter[tuple[str, str, str]] = Counter()
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("E2E_RESULT="):
            value = json.loads(line.removeprefix("E2E_RESULT="))
            if type(value) is not dict:
                raise RuntimeError(f"{path} contains a non-object E2E report")
            reports.append(value)
        elif line.startswith("E2E_DIAGNOSTIC="):
            value = json.loads(line.removeprefix("E2E_DIAGNOSTIC="))
            if value.get("event") in {"request-start", "request-pass", "request-fail"}:
                events[(str(value.get("event")), str(value.get("target")), str(value.get("message")))] += 1

    if len(reports) != 1:
        raise RuntimeError(f"{path} contains {len(reports)} E2E reports; expected one")
    report = reports[0]
    client = report.get("client")
    if client not in EXPECTED_CLIENTS:
        raise RuntimeError(f"{path} contains unknown client report {client!r}")
    if (
        type(report.get("passed")) is not int
        or report["passed"] != 20
        or type(report.get("failed")) is not int
        or report["failed"] != 0
        or type(report.get("expected")) is not int
        or report["expected"] != 20
        or report.get("failures") != []
    ):
        raise RuntimeError(f"invalid E2E report in {path}: {report}")

    expected_pairs = {
        (target, message) for target in EXPECTED_TARGETS for message in EXPECTED_MESSAGES
    }
    for event in ("request-start", "request-pass"):
        observed = {
            (target, message)
            for (kind, target, message), count in events.items()
            if kind == event and count == 1
        }
        duplicates = sorted(
            (target, message, count)
            for (kind, target, message), count in events.items()
            if kind == event and count != 1
        )
        if observed != expected_pairs or duplicates:
            raise RuntimeError(
                f"{path} {event} matrix mismatch: missing={sorted(expected_pairs - observed)}, "
                f"extra={sorted(observed - expected_pairs)}, duplicates={duplicates}"
            )
    failures = [key for key, count in events.items() if key[0] == "request-fail" and count]
    if failures:
        raise RuntimeError(f"{path} contains request-fail diagnostics: {failures}")
    return report


def _server_diagnostics(path: Path, event: str) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        marker, separator, payload = line.partition("E2E_DIAGNOSTIC=")
        if not separator:
            continue
        value = json.loads(payload)
        if type(value) is not dict:
            raise RuntimeError(f"{path} contains a non-object diagnostic after {marker!r}")
        if value.get("event") == event:
            records.append(value)
    return records


def _validate_servers(native_path: Path, monkey_path: Path) -> dict[str, object]:
    native = _server_diagnostics(native_path, "native-dispatch")
    monkey = _server_diagnostics(monkey_path, "asgi-dispatch")
    native_payloads = Counter(str(record.get("payload_type")) for record in native)
    expected_native_payloads = Counter({"CynapsaRequest": 40})
    if len(native) != 40 or native_payloads != expected_native_payloads:
        raise RuntimeError(
            f"native server dispatch mismatch: count={len(native)}, "
            f"payloads={dict(native_payloads)}"
        )
    if any(record.get("body_bytes") != 9 for record in native):
        raise RuntimeError("native server received an unexpected request body size")
    if len(monkey) != 40:
        raise RuntimeError(f"monkey server dispatch mismatch: count={len(monkey)}")
    if any(record.get("path") != "/reverse" for record in monkey):
        raise RuntimeError("monkey server received an unexpected request path")
    if any(record.get("body_bytes") != 9 for record in monkey):
        raise RuntimeError("monkey server received an unexpected request body size")
    return {
        "native": {"dispatches": len(native), "payloads": dict(native_payloads)},
        "monkey": {"dispatches": len(monkey)},
    }


def main(paths: list[str], native_server_log: str, monkey_server_log: str) -> int:
    reports: dict[str, dict[str, object]] = {}
    for raw_path in paths:
        path = Path(raw_path)
        report = _validate_log(path)
        client = str(report["client"])
        if client in reports:
            raise RuntimeError(f"duplicate E2E report for {client}")
        reports[client] = report

    if set(reports) != EXPECTED_CLIENTS:
        raise RuntimeError(
            f"client set mismatch: expected {sorted(EXPECTED_CLIENTS)}, got {sorted(reports)}"
        )
    total = sum(int(report["passed"]) for report in reports.values())
    failed = sum(int(report["failed"]) for report in reports.values())
    if total != 80 or failed != 0:
        raise RuntimeError(f"E2E result is {total}/80 with {failed} failures: {reports}")
    servers = _validate_servers(Path(native_server_log), Path(monkey_server_log))
    print(
        json.dumps(
            {"passed": total, "expected": 80, "clients": reports, "servers": servers},
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--native-server-log", required=True)
    parser.add_argument("--monkey-server-log", required=True)
    parser.add_argument("client_logs", nargs="+")
    arguments = parser.parse_args()
    raise SystemExit(
        main(
            arguments.client_logs,
            arguments.native_server_log,
            arguments.monkey_server_log,
        )
    )
