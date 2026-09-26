"""Real SDK handler workers and LangGraph/SQLite; fake ingress/model/remote RPC."""
from __future__ import annotations

import asyncio
import base64
import importlib.util
import json
import queue
import sys
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

ROOT = Path(__file__).resolve().parents[1]


def test_sdk_dispatch_reuses_owner_loop_for_shared_graph_memory(tmp_path):
    pytest.importorskip("cynapsa", reason="install the paired SDK for real dispatch QA")
    from cynapsa._inbound import InboundRuntime
    from cynapsa.session import AsyncAztmSession

    spec = importlib.util.spec_from_file_location(
        "sdk_dispatch_workflow", ROOT / "orchestrator/workflow.py",
    )
    workflow = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = workflow
    spec.loader.exec_module(workflow)

    async def run():
        owner_loop = asyncio.get_running_loop()
        owner_thread = threading.get_ident()
        ingress = queue.Queue()
        replies = queue.Queue()
        entered = threading.Event()
        loops = []
        accepted = []

        class Core:
            _queue_limit = 8

            def _next_callback_event(self, *, diagnostic, timeout):
                if diagnostic:
                    threading.Event().wait(timeout)
                    raise queue.Empty
                return SimpleNamespace(payload=ingress.get(timeout=timeout))

        def execute(command, args):
            assert command == "delivery.accept"
            accepted.append(args["event_id"])
            return SimpleNamespace(ok=True, result_type="empty", result={})

        owner = SimpleNamespace(
            core=Core(), payload_limit=1 << 20, command_timeout=2,
            require_open=lambda: None, execute=execute,
        )

        class Session:
            _owner = owner
            _new_request = AsyncAztmSession._new_request

            async def _reply(self, handle, response):
                replies.put(response)

        model = SimpleNamespace(complete=Mock(side_effect=[
            {"content": "Hello!"},
            {"tool_calls": [{"id": "maps-call", "function": {
                "name": "ask_maps_agent",
                "arguments": '{"question":"indoor skydiving near Fort Lauderdale"}',
            }}]},
            {"content": "Here is an indoor skydiving place."},
            {"content": "Yes, near Fort Lauderdale."},
            {"content": "First simultaneous reply"},
            {"content": "Second simultaneous reply"},
        ]))
        remote = AsyncMock(return_value={"answer": "A test place", "sources": []})
        key = workflow.thread_key("orchestrator", "mesh-one", "client@example.test", "chat")
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "memory.sqlite")) as saver:
            agent = workflow.ConversationAgent(model, remote, saver)
            # Bind the real SQLite saver lock to its creating loop through
            # contention, not merely through an uncontended fast-path acquire.
            async with saver.lock:
                waiting = asyncio.create_task(saver.aget_tuple({
                    "configurable": {"thread_id": key},
                }))
                await asyncio.sleep(0)
            await waiting
            runtime = InboundRuntime(owner, Session(), asynchronous=True)

            async def ask(request):
                loops.append((asyncio.get_running_loop(), threading.get_ident()))
                entered.set()
                return await agent.answer(request.json()["prompt"], key)

            runtime.register("/ask", ask)

            def emit(index, prompt):
                body = json.dumps({"prompt": prompt}).encode()
                ingress.put(json.dumps({
                    "abi_version": 1, "event_id": f"event-{index}",
                    "event_name": "message.received",
                    "created_at": "2026-09-26T12:00:00Z",
                    "payload": {
                        "message_id": f"message-{index}", "conversation_id": "chat",
                        "from_agent_id": "client@example.test", "mesh_id": "mesh-one",
                        "mode": "rpc",
                        "request_handle": "reqh_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                        "payload": {"native": {
                            "content_type": "application/json", "path": "/ask",
                            "body": base64.b64encode(body).decode(),
                        }},
                    },
                }).encode())

            async def response():
                result = await asyncio.to_thread(replies.get, True, 5)
                assert result.status_code == 200, result.text()
                return result.json()

            try:
                await saver.lock.acquire()
                try:
                    emit(1, "hello")
                    assert await asyncio.to_thread(entered.wait, 2)
                    await asyncio.sleep(0.03)
                finally:
                    saver.lock.release()
                assert (await response())["answer"] == "Hello!"
                emit(2, "where can I do indoor skydiving near Fort Lauderdale?")
                assert (await response())["maps"]["answer"] == "A test place"
                emit(3, "Is it near the city I mentioned?")
                assert (await response())["answer"] == "Yes, near Fort Lauderdale."
                history = model.complete.call_args.args[0]
                assert any("Fort Lauderdale" in (m.get("content") or "") for m in history[:-1])
                emit(4, "simultaneous first")
                emit(5, "simultaneous second")
                await response()
                await response()
                assert len(accepted) == len(loops) == 5
                assert all(loop is owner_loop and thread == owner_thread for loop, thread in loops)
                remote.assert_awaited_once()
                saved = await saver.aget_tuple({"configurable": {"thread_id": key}})
                assert len(saved.checkpoint["channel_values"]["history"]) == 10
            finally:
                # Real worker teardown executes while its owner loop is alive.
                await asyncio.to_thread(runtime.close, 2)
            assert not any(t.is_alive() for t in (*runtime._workers, *runtime._consumers))

    try:
        asyncio.run(run())
    finally:
        sys.modules.pop(spec.name, None)
