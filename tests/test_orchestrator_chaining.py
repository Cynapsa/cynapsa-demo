"""Real graph/checkpoints with deterministic model and remote-agent fixtures."""
from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("demo_chaining_workflow", ROOT / "orchestrator/workflow.py")
workflow = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = workflow
spec.loader.exec_module(workflow)


def tool(name, question, id):
    return {"id": id, "function": {"name": name, "arguments": json.dumps({"question": question})}}


def test_files_then_maps_completes_one_question_with_both_sources(tmp_path):
    async def run():
        files_result = {"answer": "Guy lives in Fort Lauderdale.",
                        "sources": [{"path": "file1.txt", "line": 2, "excerpt": "City: Fort Lauderdale"}]}
        maps_result = {"answer": "Cafe A is highly rated in Fort Lauderdale.",
                       "sources": [{"name": "Cafe A", "google_maps_url": "https://maps.google.com/?q=Cafe+A"}]}
        model = SimpleNamespace(complete=Mock(side_effect=[
            {"tool_calls": [tool("ask_files_agent", "Which city does Guy live in?", "files-1")]},
            {"tool_calls": [tool("ask_maps_agent", "Find the best cafes in Fort Lauderdale", "maps-1")]},
            {"content": "Guy lives in Fort Lauderdale (file1.txt:2). Cafe A is highly rated there."},
            {"content": "Guy lives in Fort Lauderdale."},
        ]))
        files = AsyncMock(return_value=files_result)
        maps = AsyncMock(return_value=maps_result)
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "chat.sqlite")) as saver:
            agent = workflow.ConversationAgent(model, maps, saver, ask_files=files)
            result = await agent.answer("Look up Guy's city and find the best cafes there", "chat")
            files.assert_awaited_once_with("Which city does Guy live in?")
            maps.assert_awaited_once_with("Find the best cafes in Fort Lauderdale")
            assert result["files"] == files_result
            assert result["maps"] == maps_result
            second = model.complete.call_args_list[1]
            assert second.kwargs["tool_choice"] == "auto"
            assert {t["function"]["name"] for t in second.kwargs["tools"]} == {"ask_maps_agent", "ask_files_agent"}
            assert "Fort Lauderdale" in second.args[0][-1]["content"]
            final_messages = model.complete.call_args_list[2].args[0]
            assert [m["tool_call_id"] for m in final_messages if m["role"] == "tool"] == ["files-1", "maps-1"]
            assert "Cafe A" in result["answer"]
            await agent.answer("Remind me of his city", "chat")
            assert "Cafe A" in json.dumps(model.complete.call_args.args[0])
    asyncio.run(run())


def test_call_budget_forces_final_answer_and_resets_on_next_turn(tmp_path):
    async def run():
        model = SimpleNamespace(complete=Mock(side_effect=[
            *[{"tool_calls": [tool("ask_maps_agent", f"lookup {i}", f"call-{i}")]} for i in range(workflow.MAX_TOOL_CALLS)],
            {"content": "Verified summary; further lookups could not be completed."},
            {"tool_calls": [tool("ask_maps_agent", "new turn", "call-0")]},
            {"content": "New answer"},
        ]))
        maps = AsyncMock(return_value={"answer": "Evidence", "sources": [{"name": "Place"}]})
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "chat.sqlite")) as saver:
            agent = workflow.ConversationAgent(model, maps, saver)
            result = await agent.answer("Lookups", "chat")
            assert maps.await_count == workflow.MAX_TOOL_CALLS
            assert model.complete.call_args.kwargs["tool_choice"] == "none"
            assert len(result["maps"]["sources"]) == workflow.MAX_TOOL_CALLS
            await agent.answer("New question", "chat")
            assert maps.await_count == workflow.MAX_TOOL_CALLS + 1
    asyncio.run(run())


def test_model_cannot_ignore_exhausted_budget(tmp_path):
    async def run():
        model = SimpleNamespace(complete=Mock(side_effect=[
            {"tool_calls": [tool("ask_maps_agent", "query", f"call-{i}")]}
            for i in range(workflow.MAX_TOOL_CALLS + 1)
        ]))
        maps = AsyncMock(return_value={"answer": "Evidence"})
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "chat.sqlite")) as saver:
            agent = workflow.ConversationAgent(model, maps, saver)
            with pytest.raises(RuntimeError, match="tool-call limit"):
                await agent.answer("Keep looking", "chat")
            assert maps.await_count == workflow.MAX_TOOL_CALLS
    asyncio.run(run())


@pytest.mark.parametrize("calls", [
    [tool("ask_maps_agent", "query", f"call-{i}") for i in range(workflow.MAX_TOOL_CALLS + 1)],
    [tool("ask_maps_agent", "query", "same"), tool("ask_maps_agent", "query", "same")],
    [tool("ask_maps_agent", "query", "valid"), tool("unknown_agent", "query", "invalid")],
])
def test_invalid_batch_is_rejected_before_any_rpc(tmp_path, calls):
    async def run():
        model = SimpleNamespace(complete=Mock(return_value={"tool_calls": calls}))
        maps = AsyncMock()
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "chat.sqlite")) as saver:
            agent = workflow.ConversationAgent(model, maps, saver)
            with pytest.raises(RuntimeError):
                await agent.answer("Invalid batch", "chat")
            maps.assert_not_awaited()
    asyncio.run(run())


def test_valid_batch_provides_feedback_for_every_call(tmp_path):
    async def run():
        model = SimpleNamespace(complete=Mock(side_effect=[
            {"tool_calls": [tool("ask_maps_agent", "first", "a"), tool("ask_maps_agent", "second", "b")]},
            {"content": "Combined answer"},
        ]))
        maps = AsyncMock(side_effect=[{"answer": "First", "sources": [{"name": "A"}]},
                                     {"answer": "Second", "sources": [{"name": "B"}]}])
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "chat.sqlite")) as saver:
            result = await workflow.ConversationAgent(model, maps, saver).answer("Compare", "chat")
            assert maps.await_count == 2
            assert result["maps"]["sources"] == [{"name": "A"}, {"name": "B"}]
            assert [m["tool_call_id"] for m in model.complete.call_args.args[0] if m["role"] == "tool"] == ["a", "b"]
    asyncio.run(run())


def test_second_agent_failure_does_not_save_partial_turn_or_replay_it(tmp_path):
    async def run():
        model = SimpleNamespace(complete=Mock(side_effect=[
            {"content": "First successful answer"},
            {"tool_calls": [tool("ask_files_agent", "Guy's city", "files-1")]},
            {"tool_calls": [tool("ask_maps_agent", "cafes in Fort Lauderdale", "maps-1")]},
            {"content": "Recovered"},
        ]))
        files = AsyncMock(return_value={"answer": "Fort Lauderdale", "sources": []})
        maps = AsyncMock(side_effect=RuntimeError("maps offline"))
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "chat.sqlite")) as saver:
            agent = workflow.ConversationAgent(model, maps, saver, ask_files=files)
            await agent.answer("Initial prompt", "chat")
            with pytest.raises(RuntimeError, match="maps offline"):
                await agent.answer("Failed chained prompt", "chat")
            await agent.answer("New prompt", "chat")
            context = json.dumps(model.complete.call_args.args[0])
            assert "Failed chained prompt" not in context
            assert "Initial prompt" in context
            files.assert_awaited_once()
            maps.assert_awaited_once()
    asyncio.run(run())
