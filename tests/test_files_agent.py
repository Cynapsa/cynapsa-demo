"""Real local tools and LangGraphs; fake model/RPC, no keys or network."""
import asyncio
import importlib.util
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

ROOT = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


tools = load("demo_files_tools", "files/tools.py")
workflow = load("demo_files_workflow", "files/workflow.py")
orchestrator = load("demo_files_orchestrator", "orchestrator/workflow.py")


def test_search_and_read_text_database(tmp_path):
    (tmp_path / "people.txt").write_text("Guy\nAddress: 12 Park Drive\n", encoding="utf-8")
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested/data.json").write_text('{"owner": "Guy"}', encoding="utf-8")
    db = tools.FileDatabase(tmp_path)
    assert db.list_files()["files"] == ["nested/data.json", "people.txt"]
    assert db.search_files("Guy")["matches"][0]["line"] == 1
    assert "2: Address: 12 Park Drive" in db.read_file("people.txt", 1, 3)["excerpt"]


@pytest.mark.parametrize("path", ["../outside.txt", "/etc/passwd", "nested/../../x.txt", ".env", "x.py", "x\\file.txt"])
def test_rejects_unsafe_paths(tmp_path, path):
    with pytest.raises(ValueError):
        tools.FileDatabase(tmp_path).read_file(path)


def test_symlinks_special_files_hidden_and_binary_are_not_read(tmp_path):
    root = tmp_path / "db"
    root.mkdir()
    (tmp_path / "secret.txt").write_text("private-sentinel")
    (root / "link.txt").symlink_to(tmp_path / "secret.txt")
    (root / "subdir").symlink_to(tmp_path, target_is_directory=True)
    (root / ".hidden.txt").write_text("secret")
    os.mkfifo(root / "pipe.txt")
    (root / "binary.txt").write_bytes(b"\x00Guy")
    db = tools.FileDatabase(root)
    assert db.list_files()["files"] == ["binary.txt"]
    for path in ["link.txt", "subdir/secret.txt", "pipe.txt", "binary.txt"]:
        with pytest.raises(ValueError):
            db.read_file(path)
    assert not db.search_files("Guy")["matches"]
    with pytest.raises(ValueError):
        tools.FileDatabase(root / "subdir")


def test_budgets_and_line_validation(tmp_path):
    (tmp_path / "large.txt").write_bytes(b"x" * (tools.MAX_FILE_BYTES + 1))
    (tmp_path / "long.txt").write_text("Guy " * 10_000)
    db = tools.FileDatabase(tmp_path)
    with pytest.raises(ValueError):
        db.read_file("large.txt")
    assert db.search_files("Guy")["skipped_files"] == 1
    assert len(db.read_file("long.txt")["excerpt"]) < 600
    for start, end in [(0, 1), (1, 101), (True, 2), (5, 2)]:
        with pytest.raises(ValueError):
            db.read_file("long.txt", start, end)


def test_invalid_text_counts_toward_search_budget(tmp_path, monkeypatch):
    monkeypatch.setattr(tools, "MAX_SEARCH_BYTES", 8)
    (tmp_path / "a.txt").write_bytes(b"\x00" * 8)
    (tmp_path / "b.txt").write_text("Guy")
    result = tools.FileDatabase(tmp_path).search_files("Guy")
    assert result["matches"] == []
    assert result["skipped_files"] == 1
    assert result["truncated"]


def test_file_replaced_with_symlink_after_listing_is_rejected(tmp_path):
    root = tmp_path / "db"
    root.mkdir()
    file = root / "people.txt"
    file.write_text("Guy")
    (tmp_path / "outside.txt").write_text("external-secret")
    db = tools.FileDatabase(root)
    assert db.list_files()["files"] == ["people.txt"]
    file.unlink()
    file.symlink_to(tmp_path / "outside.txt")
    with pytest.raises(ValueError):
        db.read_file("people.txt")


def call(name, args, id="tool-1"):
    return {"id": id, "function": {"name": name, "arguments": json.dumps(args)}}


def test_files_graph_grounded_answer_and_sources(tmp_path):
    (tmp_path / "people.txt").write_text("Guy lives at 12 Park Drive.")
    model = SimpleNamespace(complete=Mock(side_effect=[
        {"tool_calls": [call("search_files", {"query": "Guy"})]},
        {"tool_calls": [call("read_file", {"path": "people.txt"})]},
        {"content": "Guy lives at 12 Park Drive (people.txt:1)."},
    ]))
    result = workflow.answer_question("Where does Guy live?", model, tools.FileDatabase(tmp_path))
    assert result["sources"][0]["path"] == "people.txt"
    assert "12 Park Drive" in result["answer"]
    assert model.complete.call_args_list[0].kwargs["tool_choice"] == "required"


def test_no_evidence_cannot_return_invented_address(tmp_path):
    model = SimpleNamespace(complete=Mock(side_effect=[
        {"tool_calls": [call("search_files", {"query": "Guy"})]},
        {"content": "Guy lives at invented address"},
    ]))
    result = workflow.answer_question("Guy's address?", model, tools.FileDatabase(tmp_path))
    assert result["answer"] == "I could not find the answer in the searched files."
    assert result["sources"] == []


def test_bad_tool_path_is_reported_without_disclosing_external_data(tmp_path):
    model = SimpleNamespace(complete=Mock(side_effect=[
        {"tool_calls": [call("read_file", {"path": "../secret.txt"})]},
        {"content": "Not found"},
    ]))
    workflow.answer_question("Find secret", model, tools.FileDatabase(tmp_path))
    tool_output = model.complete.call_args.args[0][-1]
    assert "Invalid arguments" in tool_output["content"]


def test_orchestrator_dispatches_files_and_remembers_answer(tmp_path):
    async def run():
        model = SimpleNamespace(complete=Mock(side_effect=[
            {"tool_calls": [call("ask_files_agent", {"question": "Guy's address?"})]},
            {"content": "12 Park Drive (people.txt:1)"},
            {"content": "Your earlier answer was 12 Park Drive."},
        ]))
        maps = AsyncMock()
        files = AsyncMock(return_value={"answer": "12 Park Drive", "sources": [{"path": "people.txt", "line": 1}]})
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "chat.sqlite")) as saver:
            agent = orchestrator.ConversationAgent(model, maps, saver, ask_files=files)
            answer = await agent.answer("Where does Guy live?", "chat")
            assert answer["files"]["sources"][0]["path"] == "people.txt"
            files.assert_awaited_once_with("Guy's address?")
            maps.assert_not_awaited()
            await agent.answer("What was the address?", "chat")
            assert "12 Park Drive" in json.dumps(model.complete.call_args.args[0])
    asyncio.run(run())


def test_files_native_handler_returns_answer_and_canonical_errors(monkeypatch, tmp_path):
    (tmp_path / "people.txt").write_text("Guy lives at 12 Park Drive.")
    model = SimpleNamespace(complete=Mock(side_effect=[
        {"tool_calls": [call("search_files", {"query": "Guy"})]},
        {"content": "12 Park Drive (people.txt:1)"},
    ]), close=Mock())

    class RPCException(Exception):
        def __init__(self, status_code, *, code, detail):
            super().__init__(detail)
            self.status_code, self.code = status_code, code

    class Session:
        agent_id = "files@example.test"
        def __enter__(self):
            return self
        def __exit__(self, *_):
            return None
        def on(self, path):
            assert path == "*"
            def register(handler):
                self.handler = handler
                return handler
            return register

    session = Session()
    from contextlib import nullcontext
    monkeypatch.setitem(sys.modules, "cynapsa", SimpleNamespace(
        connect=Mock(return_value=session), RPCException=RPCException))
    monkeypatch.setitem(sys.modules, "runtime", SimpleNamespace(
        prepare_runtime=lambda: None, connection_options=lambda **_: {},
        litellm_key=lambda: "test-key"))
    monkeypatch.setitem(sys.modules, "llm", SimpleNamespace(
        ModelClient=lambda *_, **__: model, ModelError=type("ModelError", (Exception,), {})))
    monkeypatch.setitem(sys.modules, "connection_alerts", SimpleNamespace(
        watch_connection=lambda *_: nullcontext()))
    monkeypatch.setitem(sys.modules, "tools", tools)
    monkeypatch.setitem(sys.modules, "workflow", workflow)
    monkeypatch.setenv("DEMO_FILES_DIRECTORY", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["app.py"])
    app = load("demo_files_native_app", "files/app.py")
    monkeypatch.setattr(app, "threading", SimpleNamespace(Event=lambda: SimpleNamespace(wait=lambda: None)))
    app.main()
    result = session.handler(SimpleNamespace(json=lambda: {"question": "Guy's address?"}))
    assert "12 Park Drive" in result["answer"]
    with pytest.raises(RPCException) as failure:
        session.handler(SimpleNamespace(json=lambda: {"question": ""}))
    assert failure.value.status_code == 400
    assert failure.value.code == "bad_request"
    model.close.assert_called_once()
