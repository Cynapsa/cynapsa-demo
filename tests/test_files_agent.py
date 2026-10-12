"""Real local tools and LangGraphs; fake model/HTTP, no keys or network."""
import asyncio
import ast
import importlib.util
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi.testclient import TestClient
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


def test_files_app_is_plain_http_and_cli_owns_mesh_bootstrap():
    source = (ROOT / "files/app.py").read_text()
    tree = ast.parse(source)
    assert not any(
        isinstance(node, ast.Import)
        and any(alias.name == "cynapsa" or alias.name.startswith("cynapsa.") for alias in node.names)
        or isinstance(node, ast.ImportFrom)
        and node.module is not None
        and (node.module == "cynapsa" or node.module.startswith("cynapsa."))
        for node in ast.walk(tree)
    )
    assert '@app.post("/files")' in source
    assert "cynapsa run" in (ROOT / "files/entrypoint.sh").read_text()
    assert "uvicorn app:app" in (ROOT / "files/entrypoint.sh").read_text()


def test_files_http_route_returns_answer_and_http_errors(monkeypatch, tmp_path):
    (tmp_path / "people.txt").write_text("Guy lives at 12 Park Drive.")
    model = SimpleNamespace(complete=Mock(side_effect=[
        {"tool_calls": [call("search_files", {"query": "Guy"})]},
        {"content": "12 Park Drive (people.txt:1)"},
    ]), close=Mock())

    for name in ("llm", "runtime", "tools", "workflow"):
        module = load(f"demo_files_http_{name}", f"files/{name}.py")
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.setenv("DEMO_FILES_DIRECTORY", str(tmp_path))
    app = load("demo_files_http_app", "files/app.py")
    monkeypatch.setattr(app, "ModelClient", lambda *_, **__: model)
    monkeypatch.setattr(app, "litellm_key", lambda: "test-key")
    with TestClient(app.app) as client:
        response = client.post("/files", json={"question": "Guy's address?"})
        assert response.status_code == 200
        assert "12 Park Drive" in response.json()["answer"]
        assert client.get("/files").status_code == 405
        assert client.post("/other", json={"question": "hello"}).status_code == 404
        for body in ({"question": ""}, {"question": 7}, [], {}):
            assert client.post("/files", json=body).status_code == 400
        assert client.post("/files", content=b"not-json").status_code == 400

        def model_failure(*_):
            raise app.ModelError(429)

        monkeypatch.setattr(app, "answer_question", model_failure)
        unavailable = client.post("/files", json={"question": "Guy?"})
        assert unavailable.status_code == 503
        assert "test-key" not in unavailable.text

        def unexpected_failure(*_):
            raise RuntimeError("private model details")

        monkeypatch.setattr(app, "answer_question", unexpected_failure)
        failed = client.post("/files", json={"question": "Guy?"})
        assert failed.status_code == 502
        assert "private model details" not in failed.text
    model.close.assert_called_once()
