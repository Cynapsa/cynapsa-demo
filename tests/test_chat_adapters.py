"""HTTP adapters keep ordinary app semantics with or without the CLI bridge."""

from __future__ import annotations

import asyncio
import importlib.util
import logging
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest
import requests


ROOT = Path(__file__).resolve().parents[1]


def load(name, path, monkeypatch):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


def orchestrator(monkeypatch):
    root = ROOT / "orchestrator"
    load("llm", root / "llm.py", monkeypatch)
    load("runtime", root / "runtime.py", monkeypatch)
    load("workflow", root / "workflow.py", monkeypatch)
    return load("demo_orchestrator_http", root / "app.py", monkeypatch)


def maps(monkeypatch):
    root = ROOT / "maps"
    for name in ("llm", "places", "runtime"):
        load(name, root / f"{name}.py", monkeypatch)
    return load("demo_maps_http", root / "app.py", monkeypatch)


def test_maps_http_endpoint_and_error_status(monkeypatch):
    async def run():
        app = maps(monkeypatch)
        app.app.state.model_client = object()
        app.app.state.places = object()
        monkeypatch.setattr(app, "answer_question", lambda question, *_: {"answer": question, "sources": []})
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app.app), base_url="http://localhost") as client:
            good = await client.post("/maps", json={"question": "hello"})
            wildcard = await client.post("/other", json={"question": "world"})
            bad = await client.post("/maps", json={"question": ""})
            assert good.status_code == wildcard.status_code == 200
            assert good.json()["answer"] == "hello"
            assert wildcard.json()["answer"] == "world"
            assert bad.status_code == 400

            def rejected(*_):
                raise app.MapsApplicationError(502, "maps_no_answer", "No answer")

            monkeypatch.setattr(app, "answer_question", rejected)
            error = await client.post("/maps", json={"question": "hello"})
            assert error.status_code == 502
            assert error.json()["detail"]["code"] == "maps_no_answer"

    asyncio.run(run())


def test_maps_lifespan_runs_http_handler_without_mesh(monkeypatch):
    async def run():
        app = maps(monkeypatch)
        model = SimpleNamespace(close=Mock())
        places = SimpleNamespace(close=Mock())
        monkeypatch.setattr(app, "ModelClient", lambda *_, **__: model)
        monkeypatch.setattr(app, "GooglePlaces", lambda *_: places)
        monkeypatch.setattr(app, "litellm_key", lambda: "test-key")
        monkeypatch.setattr(app, "google_maps_key", lambda: "test-key")
        monkeypatch.setattr(app, "answer_question", lambda question, *_: {"answer": question, "sources": []})
        async with app.lifespan(app.app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app.app), base_url="http://localhost") as client:
                response = await client.post("/maps", json={"question": "cafe"})
                assert response.status_code == 200
                assert response.json()["answer"] == "cafe"
        model.close.assert_called_once()
        places.close.assert_called_once()

    asyncio.run(run())


def test_orchestrator_http_request_uses_bridge_sender_for_memory_scope(monkeypatch):
    async def run():
        app = orchestrator(monkeypatch)
        keys = []

        class Agent:
            async def answer(self, prompt, key):
                keys.append(key)
                return {"answer": "ok", "maps": None, "files": None}

        app.app.state.agent = Agent()
        sender = "client-a@example.test"

        async def bridge(scope, receive, send):
            if scope["type"] == "http":
                scope = {**scope, "state": {"cynapsa_sender": sender}, "client": None}
            await app.app(scope, receive, send)

        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=bridge), base_url="http://demo-orchestrator.local") as client:
            first = await client.post("/ask", json={"prompt": "hello", "conversation_id": "chat-1"})
            second = await client.post("/ask", json={"prompt": "again", "conversation_id": "chat-1"})
            assert first.status_code == second.status_code == 200
            assert first.json()["conversation_id"] == "chat-1"
            assert keys[0] == keys[1]
            sender = "client-b@example.test"
            third = await client.post("/ask", json={"prompt": "hello", "conversation_id": "chat-1"})
            assert third.status_code == 200
            assert keys[2] != keys[0]
            invalid = await client.post("/ask", json={"prompt": "", "conversation_id": "bad/id"})
            assert invalid.status_code == 400
            assert invalid.json()["detail"]["code"] == "bad_request"

    asyncio.run(run())


def test_orchestrator_http_errors_are_canonical(monkeypatch):
    async def run():
        app = orchestrator(monkeypatch)

        class Agent:
            async def answer(self, *_):
                raise RuntimeError("private downstream error")

        app.app.state.agent = Agent()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app.app), base_url="http://localhost") as client:
            response = await client.post("/ask", json={"prompt": "hello"})
        assert response.status_code == 502
        assert response.json()["detail"]["code"] == "orchestrator_failed"
        assert "private downstream error" not in response.text

    asyncio.run(run())


def test_orchestrator_lifespan_persists_http_conversation(monkeypatch, tmp_path):
    async def run():
        monkeypatch.setenv("DEMO_MEMORY_DIRECTORY", str(tmp_path))
        app = orchestrator(monkeypatch)
        model = SimpleNamespace(complete=Mock(return_value={"content": "hello"}), close=Mock())
        monkeypatch.setattr(app, "ModelClient", lambda *_, **__: model)
        monkeypatch.setattr(app, "litellm_key", lambda: "test-key")
        async with app.lifespan(app.app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app.app), base_url="http://localhost") as client:
                response = await client.post("/ask", json={"prompt": "hi", "conversation_id": "chat-1"})
                assert response.status_code == 200, response.text
                assert response.json()["answer"] == "hello"
        assert (tmp_path / "conversations.sqlite").stat().st_mode & 0o777 == 0o600
        model.close.assert_called_once()

    asyncio.run(run())


@pytest.mark.parametrize("quiet", [False, True])
def test_client_reuses_conversation_then_new_resets_it(monkeypatch, capsys, caplog, quiet):
    previous_disable = logging.root.manager.disable
    caplog.set_level(logging.INFO)
    response = requests.Response()
    response.status_code = 200
    response._content = b'{"answer":"ok","maps":null}'
    response.url = "http://demo-orchestrator.local/ask"
    post = Mock(return_value=response)
    monkeypatch.setattr(requests, "post", post)
    monkeypatch.setenv("DEMO_CONVERSATION_ID", "resumed-chat")
    monkeypatch.setattr(sys, "argv", ["app.py"] + (["--quiet"] if quiet else []))
    inputs = iter(["first", "follow up", "new", "fresh question", "quit"])
    monkeypatch.setattr("builtins.input", lambda _: next(inputs))
    app = load("demo_client_adapter", ROOT / "client/app.py", monkeypatch)
    try:
        app.main()
    finally:
        logging.disable(previous_disable)
    bodies = [call.kwargs["json"] for call in post.call_args_list]
    assert bodies[0]["conversation_id"] == bodies[1]["conversation_id"] == "resumed-chat"
    assert bodies[2]["conversation_id"] != "resumed-chat"
    assert all(call.args == ("http://demo-orchestrator.local/ask",)
               and call.kwargs["timeout"] == 140 for call in post.call_args_list)
    output = capsys.readouterr().out
    assert ("Conversation: resumed-chat" in output) is not quiet
    assert ("demo-client ready" in output) is not quiet
    assert "ok" in output
    assert ("HTTP request sent to orchestrator" in caplog.text) is not quiet


def test_quiet_client_keeps_prompts_answers_and_failures(monkeypatch, capsys, caplog):
    denied = requests.Response()
    denied.status_code = 404
    denied._content = b'{"detail":"Missing endpoint"}'
    denied.url = "http://demo-orchestrator.local/ask"
    answered = requests.Response()
    answered.status_code = 200
    answered._content = b'{"answer":"Chat answer","maps":null}'
    answered.url = denied.url
    post = Mock(side_effect=[requests.ConnectionError("request denied"), denied, answered])
    monkeypatch.setattr(requests, "post", post)
    monkeypatch.setattr(sys, "argv", ["app.py", "--quiet"])
    inputs = iter(["one", "two", "three", "quit"])

    def input_prompt(prompt):
        print(prompt, end="")
        logging.getLogger("third.party").error("Hidden diagnostic")
        return next(inputs)

    monkeypatch.setattr("builtins.input", input_prompt)
    app = load("demo_quiet_client_adapter", ROOT / "client/app.py", monkeypatch)
    previous_disable = logging.root.manager.disable
    try:
        app.main()
    finally:
        logging.disable(previous_disable)
    output = capsys.readouterr().out
    assert 'Ask a question (or "quit"):' in output
    assert output.count("Request failed: The demo could not answer") == 2
    assert "Chat answer" in output
    assert "ready as" not in output and "Conversation:" not in output
    assert "CYNAPSA" not in caplog.text and "Hidden diagnostic" not in caplog.text
    assert post.call_count == 3
