"""No-network checks for simple Cynapsa traffic logs and HTTP quieting."""

import asyncio
import importlib.util
import inspect
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


def lines(caplog):
    return [record.getMessage() for record in caplog.records if record.name == "demo.cynapsa"]


@pytest.mark.parametrize("entity", ENTITIES)
def test_outbound_logs_names_ids_and_full_request_and_response(entity, caplog, monkeypatch):
    logs = helper(entity)
    target = "maps@example.test"
    monkeypatch.setenv("DEMO_MAPS_AGENT_ID", target)
    payload = {"prompt": "A very long prompt " + "x" * 5000}
    result = SimpleNamespace(status_code=200, body=b'{"answer":"Gym A"}')
    session = SimpleNamespace(request=Mock(return_value=result))
    with caplog.at_level(logging.INFO):
        assert logs.request_sync(session, entity, target, payload, path="/ask", ttl_ms=123) is result
    session.request.assert_called_once_with(target, payload, path="/ask", ttl_ms=123)
    sent, received = lines(caplog)
    assert sent.startswith(f"[{entity}] request sent to maps ({target}) /ask: ")
    assert payload["prompt"] in sent
    assert received == f'[{entity}] response received from maps ({target}) /ask: {{"answer": "Gym A"}}'


@pytest.mark.parametrize("entity", ENTITIES)
def test_inbound_async_logs_body_and_handler_return(entity, caplog, monkeypatch):
    logs = helper(entity)
    sender = "client@example.test"
    monkeypatch.setenv("DEMO_CLIENT_AGENT_ID", sender)
    request = SimpleNamespace(from_agent_id=sender, path="/maps", mode="rpc",
                              body=b'{"question":"private prompt"}')
    result = {"answer": "private result"}

    @logs.log_handler(entity)
    async def receive(incoming):
        assert incoming is request
        return result

    assert inspect.iscoroutinefunction(receive)
    with caplog.at_level(logging.INFO):
        assert asyncio.run(receive(request)) is result
    arrived, returned = lines(caplog)
    assert arrived == f'[{entity}] request received from client ({sender}) /maps: {{"question": "private prompt"}}'
    assert returned == f'[{entity}] response returned to client ({sender}) /maps: {{"answer": "private result"}}'


@pytest.mark.parametrize("entity", ENTITIES)
@pytest.mark.parametrize("asynchronous", [False, True])
def test_failed_request_logs_error_and_preserves_exception(entity, asynchronous, caplog):
    logs = helper(entity)
    failure = RuntimeError("remote refused")
    session = SimpleNamespace(request=(AsyncMock if asynchronous else Mock)(side_effect=failure))
    with caplog.at_level(logging.INFO), pytest.raises(RuntimeError) as caught:
        if asynchronous:
            asyncio.run(logs.request_async(session, entity, "peer@example.test", {}, path="/maps"))
        else:
            logs.request_sync(session, entity, "peer@example.test", {}, path="/maps")
    assert caught.value is failure
    assert lines(caplog) == [
        f"[{entity}] request sent to peer@example.test /maps: {{}}",
        f"[{entity}] request failed to peer@example.test /maps: RuntimeError: remote refused",
    ]


@pytest.mark.parametrize("entity", ENTITIES)
def test_handler_error_and_cancellation_are_not_swallowed(entity, caplog):
    logs = helper(entity)
    failure = RuntimeError("handler failed")

    @logs.log_handler(entity)
    def receive(_):
        raise failure

    with caplog.at_level(logging.INFO), pytest.raises(RuntimeError) as caught:
        receive(SimpleNamespace(mode="rpc", body=b"hello"))
    assert caught.value is failure
    assert lines(caplog)[-1] == f"[{entity}] request failed from unknown /: RuntimeError: handler failed"

    @logs.log_handler(entity)
    async def cancelled(_):
        raise asyncio.CancelledError

    caplog.clear()
    with caplog.at_level(logging.INFO), pytest.raises(asyncio.CancelledError):
        asyncio.run(cancelled(SimpleNamespace()))
    assert "CancelledError" in lines(caplog)[-1]


@pytest.mark.parametrize("entity", ENTITIES)
def test_msg_logs_only_request_and_unknown_peer_keeps_id(entity, caplog):
    logs = helper(entity)

    @logs.log_handler(entity)
    def receive(_):
        return None

    with caplog.at_level(logging.INFO):
        receive(SimpleNamespace(mode="msg", path="/files", from_agent_id="other@example.test",
                                body=b"plain text"))
    assert lines(caplog) == [f'[{entity}] request received from other@example.test /files: "plain text"']


@pytest.mark.parametrize("entity", ENTITIES)
def test_http_access_logs_quiet_but_warnings_and_cynapsa_visible(entity, caplog, monkeypatch):
    logs = helper(entity)
    with caplog.at_level(logging.INFO):
        for name in ("httpx", "httpcore", "urllib3", "requests"):
            monkeypatch.setattr(logging.getLogger(name), "level", logging.NOTSET)
        logs.configure_logging()
        with httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200))) as client:
            client.post("https://model.example.test/v1/chat/completions", json={"prompt": "private"})
        logging.getLogger("httpx").warning("warning retained")
        logs.request_sync(SimpleNamespace(request=lambda *_, **__: SimpleNamespace(body=b"ok")),
                          entity, "peer@example.test", {"question": "hello"}, path="/ask")
    assert "HTTP Request:" not in caplog.text
    assert "warning retained" in caplog.text
    assert "request sent to" in caplog.text


def test_helpers_stay_identical_and_are_packaged_in_each_directory():
    sources = {(ROOT / entity / "rpc_logging.py").read_bytes() for entity in ENTITIES}
    assert len(sources) == 1
    for entity in ENTITIES:
        assert "rpc_logging.py" in (ROOT / entity / "Dockerfile").read_text()


@pytest.mark.parametrize("entity", ENTITIES)
def test_return_log_does_not_claim_sdk_delivery(entity, caplog):
    sdk = pytest.importorskip("cynapsa")
    logs = helper(entity)
    value = {"answer": object()}

    @logs.log_handler(entity)
    def receive(_):
        return value

    with caplog.at_level(logging.INFO):
        returned = receive(SimpleNamespace(mode="rpc"))
    assert returned is value
    assert "response returned to" in lines(caplog)[-1]
    assert "response sent" not in caplog.text
    with pytest.raises((TypeError, ValueError)):
        sdk.CynapsaResponse.from_value(returned)


@pytest.mark.parametrize("entity", ENTITIES)
def test_real_sdk_error_preserves_original_exception(entity, caplog):
    sdk = pytest.importorskip("cynapsa")
    logs = helper(entity)
    failure = sdk.RPCException(404, code="not_found", detail="missing endpoint")

    @logs.log_handler(entity)
    def receive(_):
        raise failure

    with caplog.at_level(logging.INFO), pytest.raises(sdk.RPCException) as caught:
        receive(SimpleNamespace(mode="rpc"))
    assert caught.value is failure
    assert "request failed" in lines(caplog)[-1]
    response = sdk.CynapsaResponse.from_rpc_exception(failure)
    remote = sdk.RemoteNativeError(0, response)
    session = SimpleNamespace(request=Mock(side_effect=remote))
    with caplog.at_level(logging.INFO), pytest.raises(sdk.RemoteNativeError) as caught:
        logs.request_sync(session, entity, "peer@example.test", {}, path="/ask")
    assert caught.value is remote
    assert "request failed" in lines(caplog)[-1]
