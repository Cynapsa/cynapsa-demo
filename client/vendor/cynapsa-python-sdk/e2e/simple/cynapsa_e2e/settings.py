from __future__ import annotations

import json
import os
import queue
import signal
import stat
import threading
from pathlib import Path
from typing import Any

import cynapsa

NATIVE_ORIGIN = "https://native-server.test"
MONKEY_ORIGIN = "https://monkey-server.test"
ROUTE = "/reverse"
DEFAULT_TIMEOUT_MS = 30_000
MAX_TIMEOUT_MS = (2**63 - 1) // 1_000_000


def diagnostic(event: str, **fields: object) -> None:
    record = {"event": event, **fields}
    print("E2E_DIAGNOSTIC=" + json.dumps(record, sort_keys=True), flush=True)


def required(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise RuntimeError(f"required environment variable is missing: {name}")
    return value


NATIVE_SERVER = os.environ.get("CYNAPSA_NATIVE_SERVER_AGENT_ID", "")
MONKEY_SERVER = os.environ.get("CYNAPSA_MONKEY_SERVER_AGENT_ID", "")


def _enrollment_token(path_value: str) -> str:
    path = Path(path_value)
    try:
        file_status = path.lstat()
        if stat.S_ISLNK(file_status.st_mode) or not stat.S_ISREG(file_status.st_mode):
            raise RuntimeError("CYNAPSA_ENROLLMENT_TOKEN_FILE must be a regular file")
        if stat.S_IMODE(file_status.st_mode) & 0o077:
            raise RuntimeError(
                "CYNAPSA_ENROLLMENT_TOKEN_FILE must not be accessible by group or others"
            )
        token = path.read_text(encoding="utf-8").rstrip("\r\n")
    except RuntimeError:
        raise
    except (OSError, UnicodeError) as exc:
        raise RuntimeError("CYNAPSA_ENROLLMENT_TOKEN_FILE could not be read") from exc
    if not token:
        raise RuntimeError("CYNAPSA_ENROLLMENT_TOKEN_FILE must be nonempty")
    return token


def configured_rpc_timeout_ms() -> int:
    raw = os.environ.get("CYNAPSA_E2E_RPC_TIMEOUT_MS")
    if raw is None:
        return DEFAULT_TIMEOUT_MS
    try:
        timeout_ms = int(raw, 10)
    except ValueError as exc:
        raise RuntimeError("CYNAPSA_E2E_RPC_TIMEOUT_MS must be an integer") from exc
    if not 1 <= timeout_ms <= MAX_TIMEOUT_MS:
        raise RuntimeError(
            f"CYNAPSA_E2E_RPC_TIMEOUT_MS must be between 1 and {MAX_TIMEOUT_MS}"
        )
    return timeout_ms


def auth(*, timeout_ms: int | None = None) -> dict[str, Any]:
    required("CYNAPSA_NATIVE_SERVER_AGENT_ID")
    required("CYNAPSA_MONKEY_SERVER_AGENT_ID")
    command_timeout_ms = DEFAULT_TIMEOUT_MS if timeout_ms is None else timeout_ms
    rpc_timeout_ms = (
        configured_rpc_timeout_ms() if timeout_ms is None else timeout_ms
    )
    configuration: dict[str, Any] = {
        "mesh_id": required("CYNAPSA_MESH_ID"),
        "profile_id": required("CYNAPSA_PROFILE_ID"),
        "command_timeout_ms": command_timeout_ms,
        "rpc_timeout_ms": rpc_timeout_ms,
    }
    token_file = os.environ.get("CYNAPSA_ENROLLMENT_TOKEN_FILE", "")
    if token_file:
        secret_box = [_enrollment_token(token_file)]
        try:
            configuration["enrollment_token"] = secret_box.pop()
        finally:
            secret_box.clear()
    return configuration


def allow_mesh_traffic(owner: Any) -> None:
    """Install test policy until the high-level SDK exposes policy.set."""

    completion = owner.execute(
        "policy.set",
        {"rules": [{"action": "allow", "path": "", "agent_id": ""}]},
    )
    if not completion.ok or completion.result_type != "policy":
        raise RuntimeError("failed to install the E2E application policy")


def print_core_diagnostics(owner: Any, peer: str) -> None:
    """Capture bounded public Core state at the exact point an E2E RPC fails."""

    def thaw(value: Any) -> Any:
        if isinstance(value, dict) or hasattr(value, "items"):
            return {str(key): thaw(item) for key, item in value.items()}
        if isinstance(value, (tuple, list)):
            return [thaw(item) for item in value]
        return value

    commands = (
        ("diagnostics.snapshot", {}),
        ("diagnostics.connectivity_status", {}),
        ("diagnostics.peer_status", {"peer": peer}),
    )
    for name, args in commands:
        try:
            completion = owner.execute(name, args)
            diagnostic(
                "core-diagnostic",
                command=name,
                ok=completion.ok,
                result_type=completion.result_type,
                result=thaw(completion.result or {}),
                error=str(completion.error) if completion.error is not None else "",
            )
        except Exception as exc:
            diagnostic(
                "core-diagnostic-fail",
                command=name,
                error_type=type(exc).__name__,
                error=str(exc),
            )


def mark_ready() -> None:
    Path("/tmp/cynapsa-ready").touch()


def wait_for_stop() -> None:
    stopped = threading.Event()

    def stop(_signum: int, _frame: object) -> None:
        stopped.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    while not stopped.wait(1):
        pass


def print_local_diagnostics(session: cynapsa.AztmSession) -> None:
    """Expose normalized SDK-side handler failures in container logs."""

    def consume() -> None:
        while True:
            try:
                local = session.next_local_diagnostic(timeout=0.25)
            except queue.Empty:
                continue
            except (RuntimeError, AttributeError):
                return
            diagnostic(
                "sdk-local-diagnostic",
                code=local.code,
                message=local.message,
                event_id=local.event_id,
                message_id=local.message_id,
            )

    threading.Thread(target=consume, name="e2e-local-diagnostics", daemon=True).start()
