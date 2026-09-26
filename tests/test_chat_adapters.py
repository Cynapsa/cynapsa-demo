"""Exercise actual demo handlers/client with fake Cynapsa sessions, no network."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import queue
import sys
import threading
from pathlib import Path
from contextlib import asynccontextmanager
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest


ROOT = Path(__file__).resolve().parents[1]


def load(name, path, monkeypatch):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


def sdk_stub(monkeypatch):
    sdk = ModuleType("cynapsa")

    class RPCException(Exception):
        def __init__(self, status_code, *, code, detail):
            super().__init__(detail)
            self.status_code, self.code = status_code, code

    class NativeError(Exception):
        pass

    class RemoteNativeError(NativeError):
        pass

    sdk.RPCException = RPCException
    sdk.NativeError = NativeError
    sdk.RemoteNativeError = RemoteNativeError
    sdk.SdkSafetyTimeout = type("SdkSafetyTimeout", (Exception,), {})
    monkeypatch.setitem(sys.modules, "cynapsa", sdk)
    return sdk


def test_orchestrator_handler_uses_authenticated_metadata_and_remote_rpc(monkeypatch, tmp_path):
    async def run():
        sdk = sdk_stub(monkeypatch)
        ready = asyncio.Event()
        exit_started = asyncio.Event()
        release_model = threading.Event()

        class Session:
            agent_id = "orchestrator@example.test"

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_):
                exit_started.set()
                if hasattr(self, "pending_task"):
                    await self.pending_task
                return None

            async def next_event(self, timeout):
                await asyncio.sleep(timeout)
                raise queue.Empty

            def on(self, path):
                assert path == "/ask"

                def register(handler):
                    self.handler = handler
                    ready.set()
                    return handler
                return register

        session = Session()
        session.request = AsyncMock(return_value=SimpleNamespace(json=lambda: {"answer": "Gym A", "sources": []}))
        sdk.connect_async = AsyncMock(return_value=session)
        monkeypatch.setitem(sys.modules, "runtime", SimpleNamespace(
            prepare_runtime=lambda: None, STATE=tmp_path,
            connection_options=lambda **_: {}, litellm_key=lambda: "test-only-key",
            maps_agent_id=lambda: "maps@example.test",
        ))
        client = SimpleNamespace(complete=Mock(side_effect=[
            {"tool_calls": [{"id": "call-1", "function": {
                "name": "ask_maps_agent", "arguments": '{"question":"gyms near test address"}',
            }}]},
            {"content": "Here is Gym A"},
            {"content": "Remembered"},
            {"content": "Hello, other client"},
        ]), close=Mock())
        monkeypatch.setitem(sys.modules, "llm", SimpleNamespace(
            ModelClient=lambda *_, **__: client,
            ModelError=type("ModelError", (Exception,), {}),
        ))
        load("workflow", ROOT / "orchestrator/workflow.py", monkeypatch)
        # Runtime login strips configuration whitespace; routing must agree.
        monkeypatch.setenv("DEMO_MESH_ID", " test-mesh ")
        load("connection_alerts", ROOT / "orchestrator/connection_alerts.py", monkeypatch)
        app = load("demo_orchestrator_adapter", ROOT / "orchestrator/app.py", monkeypatch)
        service = asyncio.create_task(app.serve(enroll=False))
        try:
            await asyncio.wait_for(ready.wait(), 2)

            def request(prompt, sender="client-a@example.test", mesh="test-mesh", cid="chat-1"):
                return SimpleNamespace(
                    from_agent_id=sender, mesh_id=mesh,
                    json=lambda: {"prompt": prompt, "conversation_id": cid},
                )

            first = await session.handler(request("My test address; find a gym"))
            assert first["conversation_id"] == "chat-1"
            session.request.assert_awaited_once_with(
                "maps@example.test", {"question": "gyms near test address"}, path="/maps", ttl_ms=100_000,
            )
            second = await session.handler(request("What did I ask?"))
            assert second["maps"] is None
            assert "My test address" in json.dumps(client.complete.call_args.args[0])
            await session.handler(request("Hi", sender="client-b@example.test"))
            assert "My test address" not in json.dumps(client.complete.call_args.args[0])
            for bad in [request("x", sender=None), request("x", mesh="wrong"), request("x", cid="bad/id")]:
                with pytest.raises(sdk.RPCException) as failure:
                    await session.handler(bad)
                assert failure.value.code == "bad_request"
            assert (tmp_path / "conversations.sqlite").stat().st_mode & 0o777 == 0o600

            # A genuine in-flight graph turn must finish its checkpoint before
            # session teardown closes the saver (contexts exit right-to-left).
            model_started = threading.Event()

            def slow_complete(*_, **__):
                model_started.set()
                if not release_model.wait(5):
                    raise RuntimeError("test release deadline exceeded")
                return {"content": "Finished during shutdown"}

            client.complete = Mock(side_effect=slow_complete)
            session.pending_task = asyncio.create_task(session.handler(request("pending turn")))
            assert await asyncio.to_thread(model_started.wait, 2)
            service.cancel()
            await asyncio.wait_for(exit_started.wait(), 2)
            release_model.set()
            with pytest.raises(asyncio.CancelledError):
                await service
            pending_result = await session.pending_task
            assert pending_result["answer"] == "Finished during shutdown"
        finally:
            release_model.set()
            service.cancel()
            with pytest.raises(asyncio.CancelledError):
                await service
        client.close.assert_called_once()
    asyncio.run(run())


@pytest.mark.parametrize("failure_type", [RuntimeError, asyncio.CancelledError])
def test_orchestrator_storage_startup_failure_never_opens_session(monkeypatch, tmp_path, failure_type):
    sdk = sdk_stub(monkeypatch)
    sdk.connect_async = AsyncMock()
    monkeypatch.setitem(sys.modules, "runtime", SimpleNamespace(
        prepare_runtime=lambda: None, STATE=tmp_path,
        connection_options=lambda **_: {}, litellm_key=lambda: "test-only-key",
        maps_agent_id=lambda: "maps@example.test",
    ))
    client = SimpleNamespace(close=Mock())
    monkeypatch.setitem(sys.modules, "llm", SimpleNamespace(
        ModelClient=lambda *_, **__: client,
        ModelError=type("ModelError", (Exception,), {}),
    ))
    load("workflow", ROOT / "orchestrator/workflow.py", monkeypatch)
    load("connection_alerts", ROOT / "orchestrator/connection_alerts.py", monkeypatch)
    app = load("demo_orchestrator_startup", ROOT / "orchestrator/app.py", monkeypatch)

    @asynccontextmanager
    async def failing_storage(_):
        raise failure_type("storage startup failed")
        yield  # pragma: no cover

    monkeypatch.setattr(app.AsyncSqliteSaver, "from_conn_string", failing_storage)
    with pytest.raises(failure_type):
        asyncio.run(app.serve(enroll=False))
    sdk.connect_async.assert_not_awaited()
    client.close.assert_called_once()


def test_client_reuses_conversation_then_new_resets_it(monkeypatch, capsys):
    sdk = sdk_stub(monkeypatch)

    class Session:
        agent_id = "client@example.test"

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

        def next_event(self, timeout):
            threading.Event().wait(timeout)
            raise queue.Empty

    session = Session()
    session.request = Mock(return_value=SimpleNamespace(
        status_code=200, json=lambda: {"answer": "ok", "maps": None},
    ))
    sdk.connect = Mock(return_value=session)
    monkeypatch.setitem(sys.modules, "runtime", SimpleNamespace(
        prepare_runtime=lambda: None, connection_options=lambda **_: {},
        orchestrator_agent_id=lambda: "orchestrator@example.test",
    ))
    monkeypatch.setenv("DEMO_CONVERSATION_ID", "resumed-chat")
    monkeypatch.setattr(sys, "argv", ["app.py"])
    inputs = iter(["first", "follow up", "new", "fresh question", "quit"])
    monkeypatch.setattr("builtins.input", lambda _: next(inputs))
    load("connection_alerts", ROOT / "client/connection_alerts.py", monkeypatch)
    app = load("demo_client_adapter", ROOT / "client/app.py", monkeypatch)
    app.main()
    bodies = [call.args[1] for call in session.request.call_args_list]
    assert bodies[0]["conversation_id"] == bodies[1]["conversation_id"] == "resumed-chat"
    assert bodies[2]["conversation_id"] != "resumed-chat"
    assert "Conversation: resumed-chat" in capsys.readouterr().out
