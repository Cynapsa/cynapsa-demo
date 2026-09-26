"""Real LangGraph/SQLite, fake model and RPC: no credentials or cloud calls."""

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
SPEC = importlib.util.spec_from_file_location("demo_orchestrator_workflow", ROOT / "orchestrator/workflow.py")
workflow = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = workflow
SPEC.loader.exec_module(workflow)


def model(*answers):
    return SimpleNamespace(complete=Mock(side_effect=[{"content": answer} for answer in answers]))


def test_conversation_survives_orchestrator_restart(tmp_path):
    async def run():
        path = str(tmp_path / "memory.sqlite")
        key = workflow.thread_key("orchestrator", "mesh", "client-a", "chat-1")
        first = model("I will remember your address.")
        async with AsyncSqliteSaver.from_conn_string(path) as saver:
            agent = workflow.ConversationAgent(first, AsyncMock(), saver)
            await agent.answer("My address is 12201 Park Drive.", key)
        second = model("Your address is 12201 Park Drive.")
        async with AsyncSqliteSaver.from_conn_string(path) as saver:
            agent = workflow.ConversationAgent(second, AsyncMock(), saver)
            result = await agent.answer("What is my address?", key)
            assert result["answer"] == "Your address is 12201 Park Drive."
            history = second.complete.call_args.args[0]
            assert [item["role"] for item in history] == ["developer", "user", "assistant", "user"]
            assert history[1]["content"] == "My address is 12201 Park Drive."
    asyncio.run(run())


@pytest.mark.parametrize("other", [
    ("orchestrator", "mesh", "client-b", "chat-1"),
    ("orchestrator", "other-mesh", "client-a", "chat-1"),
    ("orchestrator", "mesh", "client-a", "new-chat"),
    ("different-orchestrator", "mesh", "client-a", "chat-1"),
])
def test_memory_is_scoped_to_authenticated_identity_and_conversation(tmp_path, other):
    async def run():
        client = model("Remembered", "New conversation")
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "memory.sqlite")) as saver:
            agent = workflow.ConversationAgent(client, AsyncMock(), saver)
            await agent.answer("private-address-sentinel", workflow.thread_key("orchestrator", "mesh", "client-a", "chat-1"))
            await agent.answer("hello", workflow.thread_key(*other))
            assert "private-address-sentinel" not in json.dumps(client.complete.call_args.args[0])
    asyncio.run(run())


def test_graph_calls_remote_tool_and_composes_answer(tmp_path):
    async def run():
        client = SimpleNamespace(complete=Mock(side_effect=[
            {"content": None, "tool_calls": [{
                "id": "call-1", "function": {
                    "name": "ask_maps_agent", "arguments": '{"question":"gyms near Park Drive"}',
                },
            }]},
            {"content": "Here is a gym."},
        ]))
        remote = AsyncMock(return_value={"answer": "Gym A", "sources": []})
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "memory.sqlite")) as saver:
            agent = workflow.ConversationAgent(client, remote, saver)
            result = await agent.answer("Find a gym", "thread")
            remote.assert_awaited_once_with("gyms near Park Drive")
            assert result == {"answer": "Here is a gym.", "maps": {"answer": "Gym A", "sources": []}}
            assert client.complete.call_args.args[0][-1]["role"] == "tool"
            assert set(agent.graph.nodes) >= {"reason", "maps_tool", "compose", "finish"}
    asyncio.run(run())


def test_failed_turn_does_not_poison_follow_up_or_retry_remote_rpc(tmp_path):
    async def run():
        client = SimpleNamespace(complete=Mock(side_effect=[
            {"content": "First answer"},
            {"tool_calls": [{"id": "call-1", "function": {
                "name": "ask_maps_agent", "arguments": '{"question":"test"}',
            }}]},
            {"content": "Recovered"},
        ]))
        remote = AsyncMock(side_effect=RuntimeError("remote failed"))
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "memory.sqlite")) as saver:
            agent = workflow.ConversationAgent(client, remote, saver)
            await agent.answer("successful prompt", "thread")
            with pytest.raises(RuntimeError, match="remote failed"):
                await agent.answer("failed prompt", "thread")
            await agent.answer("new prompt", "thread")
            history = client.complete.call_args.args[0]
            assert "failed prompt" not in json.dumps(history)
            assert history[1]["content"] == "successful prompt"
            remote.assert_awaited_once()
    asyncio.run(run())


def test_context_is_bounded_and_same_chat_turns_are_serialized(tmp_path):
    async def run():
        client = model(*["a" * 5_000 for _ in range(11)])
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "memory.sqlite")) as saver:
            agent = workflow.ConversationAgent(client, AsyncMock(), saver)
            await asyncio.gather(*[agent.answer(f"turn-{i}", "thread") for i in range(10)])
            saved = await agent.graph.aget_state({"configurable": {"thread_id": "thread"}})
            assert len(saved.values["history"]) == 16
            assert all(len(message["content"]) <= 4_000 for message in saved.values["history"])
            await agent.answer("last turn", "thread")
            assert len(client.complete.call_args.args[0]) == 18  # developer + 8 pairs + new prompt
    asyncio.run(run())


def test_blank_identity_cannot_create_a_shared_memory_namespace():
    with pytest.raises(ValueError):
        workflow.thread_key("orchestrator", "mesh", "", "chat")


def test_failed_final_checkpoint_is_not_recovered_as_successful_history(tmp_path):
    async def run():
        path = str(tmp_path / "memory.sqlite")
        async with AsyncSqliteSaver.from_conn_string(path) as saver:
            client = model("First answer", "Uncommitted answer")
            agent = workflow.ConversationAgent(client, AsyncMock(), saver)
            await agent.answer("successful prompt", "thread")
            original = saver.aput
            failures = []

            async def fail_final(config, checkpoint, metadata, new_versions):
                history = checkpoint["channel_values"].get("history", [])
                if any(message.get("content") == "failed-checkpoint-prompt" for message in history):
                    failures.append(True)
                    raise RuntimeError("injected checkpoint save failure")
                return await original(config, checkpoint, metadata, new_versions)

            saver.aput = fail_final
            with pytest.raises(RuntimeError, match="injected checkpoint save failure"):
                await agent.answer("failed-checkpoint-prompt", "thread")
            assert failures

        # LangGraph may overlay pending finish writes in get_state after a
        # restart. The application must read durable checkpoint channels only.
        recovered = model("Recovered")
        async with AsyncSqliteSaver.from_conn_string(path) as saver:
            agent = workflow.ConversationAgent(recovered, AsyncMock(), saver)
            await agent.answer("new prompt", "thread")
            context = json.dumps(recovered.complete.call_args.args[0])
            assert "successful prompt" in context
            assert "failed-checkpoint-prompt" not in context
            assert "Uncommitted answer" not in context
    asyncio.run(run())
