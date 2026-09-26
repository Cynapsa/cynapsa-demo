"""Console alerts observe SDK events without making network/model calls."""
import asyncio
import importlib.util
import queue
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]


def helper(entity):
    spec = importlib.util.spec_from_file_location(
        f"{entity}_alerts", ROOT / entity / "connection_alerts.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def event(name, **payload):
    return SimpleNamespace(event_name=name, payload=SimpleNamespace(**payload))


@pytest.mark.parametrize("entity", ["maps", "orchestrator", "client"])
def test_sync_monitor_reports_down_recovery_and_real_error(entity, capsys):
    alerts = helper(entity)
    pending = queue.Queue()
    observed = threading.Event()
    original = alerts.report_event

    def report(role, item):
        original(role, item)
        if item.event_name == "core.error":
            observed.set()

    alerts.report_event = report
    session = SimpleNamespace(next_event=lambda timeout: pending.get(timeout=timeout))
    with alerts.watch_connection(session, entity):
        # Main application can remain blocked; observer is a separate consumer.
        pending.put(event("connectivity.state_changed", previous="available", current="degraded"))
        pending.put(event("connectivity.state_changed", previous="degraded", current="unavailable"))
        pending.put(event("connectivity.state_changed", previous="unavailable", current="available"))
        pending.put(event("session.state_changed", previous="ready", current="failed"))
        pending.put(event("peer.unreachable", peer="maps@example.test"))
        pending.put(event("core.error", error=SimpleNamespace(
            code="authorization_rejected", message="The requested operation is not authorized",
            stage="auth", retryable=False, local_or_remote="remote",
        )))
        assert observed.wait(2)
    output = capsys.readouterr().out
    assert "available -> degraded" in output
    assert "degraded -> unavailable" in output
    assert "unavailable -> available" in output
    assert "SDK session: ready -> failed" in output
    assert "code=authorization_rejected" in output
    assert "peer.unreachable" not in output
    assert "revoked" not in output
    assert not any(t.name == f"{entity}-connection-alerts" for t in threading.enumerate())


def test_async_monitor_runs_during_application_wait_and_cleans_up(capsys):
    async def run():
        alerts = helper("orchestrator")
        pending = asyncio.Queue()
        observed = asyncio.Event()
        original = alerts.report_event

        def report(role, item):
            original(role, item)
            observed.set()

        alerts.report_event = report
        async def next_event(timeout):
            try:
                return await asyncio.wait_for(pending.get(), timeout)
            except TimeoutError:
                raise queue.Empty from None

        session = SimpleNamespace(next_event=next_event)
        async with alerts.watch_connection_async(session, "demo-orchestrator"):
            # A public queue.Empty timeout must not end the observer.
            await asyncio.sleep(0.3)
            await pending.put(event("connectivity.state_changed", previous="available", current="unavailable"))
            await asyncio.wait_for(observed.wait(), 2)
        assert not any(t.get_name() == "demo-orchestrator-connection-alerts" for t in asyncio.all_tasks())
    asyncio.run(run())
    assert "available -> unavailable" in capsys.readouterr().out


def test_monitor_failure_is_visible_once_without_retry_spin(capsys):
    calls = []

    def next_event(timeout):
        calls.append(timeout)
        raise RuntimeError("the inbound runtime is closed")

    with helper("client").watch_connection(SimpleNamespace(next_event=next_event), "client"):
        pass
    assert len(calls) == 1
    assert "connection event monitor stopped: RuntimeError" in capsys.readouterr().out


def test_each_independent_image_includes_its_local_helper():
    copies = [(ROOT / entity / "connection_alerts.py").read_bytes()
              for entity in ("client", "maps", "orchestrator")]
    assert copies[0] == copies[1] == copies[2]
    for entity in ("client", "maps", "orchestrator"):
        dockerfile = (ROOT / entity / "Dockerfile").read_text()
        assert "connection_alerts.py" in dockerfile
        assert "--core-checkout /src/core --commit WORKTREE" in dockerfile
        assert "git -C /src/core rev-parse HEAD" in dockerfile
