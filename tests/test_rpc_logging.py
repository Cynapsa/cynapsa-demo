"""No-network checks for standalone entities' traffic logs and HTTP quieting."""

import asyncio
import importlib.util
import inspect
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest


ROOT = Path(__file__).resolve().parents[1]
ENTITIES = ("client", "orchestrator", "maps", "files")


def helper(entity):
    spec = importlib.util.spec_from_file_location(f"{entity}_rpc_logging", ROOT / entity / "rpc_logging.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def events(caplog):
    return [json.loads(record.getMessage().removeprefix("CYNAPSA "))
            for record in caplog.records if record.name == "demo.cynapsa"]


@pytest.mark.parametrize("entity", ENTITIES)
def test_outbound_preserves_call_and_result_without_payloads(entity, caplog):
    logs = helper(entity)
    payload = {"prompt": "private-prompt-sentinel"}
    result = SimpleNamespace(status_code=200, body=b"private-response-sentinel")
    session = SimpleNamespace(request=Mock(return_value=result))
    with caplog.at_level(logging.INFO):
        assert logs.request_sync(session, entity, "peer@example.test", payload, path="/ask", ttl_ms=123) is result
    session.request.assert_called_once_with("peer@example.test", payload, path="/ask", ttl_ms=123)
    sent, received = events(caplog)
    assert sent["event"] == "request.started"
    assert received["event"] == "response.received"
    assert sent["log_id"] == received["log_id"]
    assert sent["peer"] == "peer@example.test" and sent["path"] == "/ask"
    assert received["status"] == 200 and received["elapsed_ms"] >= 0
    assert "private-" not in caplog.text


@pytest.mark.parametrize("entity", ENTITIES)
def test_inbound_async_dispatch_metadata_and_result(entity, caplog):
    logs = helper(entity)
    request = SimpleNamespace(from_agent_id="sender@example.test", path="/maps",
                              message_id="message-1", mode="rpc",
                              body=b"private-body", headers=(("Authorization", "private-token"),))
    result = {"answer": "private-result"}

    @logs.log_handler(entity)
    async def receive(incoming):
        assert incoming is request
        return result

    assert inspect.iscoroutinefunction(receive)
    with caplog.at_level(logging.INFO):
        assert asyncio.run(receive(request)) is result
    arrived, prepared = events(caplog)
    assert arrived["event"] == "request.received"
    assert arrived["message_id"] == "message-1"
    assert prepared["event"] == "handler.returned" and prepared["status"] is None
    assert "private-" not in caplog.text


@pytest.mark.parametrize("entity", ENTITIES)
@pytest.mark.parametrize("asynchronous", [False, True])
def test_failed_request_is_logged_and_original_error_propagates(entity, asynchronous, caplog):
    logs = helper(entity)
    failure = RuntimeError("private-error-body")
    failure.code = "forbidden"
    failure.response = SimpleNamespace(status_code=403)
    session = SimpleNamespace(request=(AsyncMock if asynchronous else Mock)(side_effect=failure))
    with caplog.at_level(logging.INFO), pytest.raises(RuntimeError) as caught:
        if asynchronous:
            asyncio.run(logs.request_async(session, entity, "peer@example.test", {}, path="/maps"))
        else:
            logs.request_sync(session, entity, "peer@example.test", {}, path="/maps")
    assert caught.value is failure
    assert [e["event"] for e in events(caplog)] == ["request.started", "request.failed"]
    assert events(caplog)[-1]["code"] == "forbidden"
    assert events(caplog)[-1]["status"] == 403
    assert "private-error-body" not in caplog.text


@pytest.mark.parametrize("entity", ENTITIES)
def test_handler_error_and_cancellation_are_not_swallowed(entity, caplog):
    logs = helper(entity)
    failure = RuntimeError("private-detail")
    failure.status_code = 404
    failure.code = "not_found"

    @logs.log_handler(entity)
    def receive(_):
        raise failure

    with caplog.at_level(logging.INFO), pytest.raises(RuntimeError) as caught:
        receive(SimpleNamespace(mode="rpc"))
    assert caught.value is failure
    assert events(caplog)[-1]["event"] == "handler.failed"
    assert events(caplog)[-1]["status"] == 404
    assert "private-detail" not in caplog.text

    @logs.log_handler(entity)
    async def cancelled(_):
        raise asyncio.CancelledError

    caplog.clear()
    with caplog.at_level(logging.INFO), pytest.raises(asyncio.CancelledError):
        asyncio.run(cancelled(SimpleNamespace()))
    assert events(caplog)[-1]["error_type"] == "CancelledError"


@pytest.mark.parametrize("entity", ENTITIES)
def test_msg_handler_never_claims_a_reply_and_metadata_is_single_line(entity, caplog):
    logs = helper(entity)

    @logs.log_handler(entity)
    def receive(_):
        return None

    with caplog.at_level(logging.INFO):
        receive(SimpleNamespace(mode="msg", path="/files\nforged-log", from_agent_id="x" * 300))
    assert events(caplog)[-1]["event"] == "handler.completed"
    assert events(caplog)[-1]["status"] is None
    assert len(events(caplog)[0]["peer"]) == 256
    assert all("\n" not in record.getMessage() for record in caplog.records)


@pytest.mark.parametrize("entity", ENTITIES)
def test_real_httpx_access_logs_quiet_but_warnings_and_rpc_visible(entity, caplog, monkeypatch):
    logs = helper(entity)
    with caplog.at_level(logging.INFO):
        for name in ("httpx", "httpcore", "urllib3", "requests"):
            monkeypatch.setattr(logging.getLogger(name), "level", logging.NOTSET)
        logs.configure_logging()
        # Actual httpx request logging uses these paths, no network or keys.
        with httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200))) as client:
            client.post("https://model.example.test/v1/chat/completions", json={"prompt": "private"})
            client.post("https://places.example.test/search", json={"query": "private"})
        logging.getLogger("httpx").warning("safe warning retained")
        logs.request_sync(SimpleNamespace(request=lambda *_, **__: SimpleNamespace(status_code=200)),
                          entity, "peer@example.test", {}, path="/ask")
    assert "HTTP Request:" not in caplog.text
    assert "safe warning retained" in caplog.text
    assert "request.started" in caplog.text


def test_helpers_stay_identical_and_are_packaged_in_each_directory():
    sources = {(ROOT / entity / "rpc_logging.py").read_bytes() for entity in ENTITIES}
    assert len(sources) == 1
    for entity in ENTITIES:
        assert "rpc_logging.py" in (ROOT / entity / "Dockerfile").read_text()


@pytest.mark.parametrize("entity", ENTITIES)
@pytest.mark.parametrize("invalid", ["oversized", "unserializable"])
def test_return_log_never_promises_success_before_sdk_normalization(entity, invalid, caplog):
    sdk = pytest.importorskip("cynapsa")
    logs = helper(entity)
    value = {"answer": "x" * 300_000} if invalid == "oversized" else {"answer": object()}

    @logs.log_handler(entity)
    def receive(_):
        return value

    with caplog.at_level(logging.INFO):
        returned = receive(SimpleNamespace(mode="rpc"))
    assert returned is value
    assert events(caplog)[-1]["event"] == "handler.returned"
    assert events(caplog)[-1]["status"] is None
    with pytest.raises((TypeError, ValueError)):
        sdk.CynapsaResponse.from_value(returned)
    assert "response.prepared" not in caplog.text


@pytest.mark.parametrize("entity", ENTITIES)
def test_real_sdk_errors_keep_canonical_status_and_original_exception(entity, caplog):
    sdk = pytest.importorskip("cynapsa")
    logs = helper(entity)
    failure = sdk.RPCException(404, code="not_found", detail="private-error-detail")

    @logs.log_handler(entity)
    def receive(_):
        raise failure

    with caplog.at_level(logging.INFO), pytest.raises(sdk.RPCException) as caught:
        receive(SimpleNamespace(mode="rpc"))
    assert caught.value is failure
    assert events(caplog)[-1]["status"] == 404
    caplog.clear()
    response = sdk.CynapsaResponse.from_rpc_exception(failure)
    remote = sdk.RemoteNativeError(0, response)
    session = SimpleNamespace(request=Mock(side_effect=remote))
    with caplog.at_level(logging.INFO), pytest.raises(sdk.RemoteNativeError) as caught:
        logs.request_sync(session, entity, "peer@example.test", {}, path="/ask")
    assert caught.value is remote
    assert events(caplog)[-1]["status"] == 404
    assert events(caplog)[-1]["code"] == "not_found"
    assert "private-error-detail" not in caplog.text


@pytest.mark.parametrize("entity", ENTITIES)
def test_closed_session_failure_is_attempt_not_sent(entity, caplog):
    logs = helper(entity)
    session = SimpleNamespace(request=Mock(side_effect=RuntimeError("session closed")))
    with caplog.at_level(logging.INFO), pytest.raises(RuntimeError):
        logs.request_sync(session, entity, "peer@example.test", {}, path="/ask")
    assert [e["event"] for e in events(caplog)] == ["request.started", "request.failed"]
    assert "request.sent" not in caplog.text and "response.received" not in caplog.text
