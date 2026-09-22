from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import math
import threading
import time
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any

import pytest

import cynapsa
from cynapsa import session as session_module
from cynapsa.exceptions import NativeError
from cynapsa.native.abi import CYNAPSA_CALLBACK_V1_COMPLETION
from cynapsa.native.command import (
    MAX_INLINE_PAYLOAD_BYTES,
    PUBLIC_ERROR_MESSAGES,
    decode_completion,
    encode_command,
)
from cynapsa.native.core import MAX_TIMEOUT_MS, NativeCore

from conftest import FakeLibrary
from test_session import AUTH, CompletionDriver, _failure


VALID_REQUEST_HANDLE = "reqh_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
SEND_RESULT = {
    "message_id": "message-one",
    "conversation_id": "conversation-one",
    "accepted": True,
}
RESPONSE_RESULT = {
    "message_id": "response-one",
    "conversation_id": "conversation-one",
    "from_agent_id": "agent-b@example.test",
    "mesh_id": "mesh-one",
    "payload": {
        "http_response": {
            "status_code": 200,
            "reason": "OK",
            "headers": [{"name": "content-type", "value": "application/json"}],
            "body": base64.b64encode(b'{"ok":true}').decode(),
        }
    },
}
HTTP_RESPONSE_RESULT = {
    "message_id": "response-http",
    "conversation_id": "conversation-http",
    "from_agent_id": "agent-b@example.test",
    "mesh_id": "mesh-one",
    "payload": {
        "http_response": {
            "status_code": 207,
            "reason": "Multi-Status",
            "headers": [
                {"name": "content-type", "value": "application/json"},
                {"name": "x-dup", "value": "one"},
                {"name": "x-dup", "value": "two"},
            ],
            "body": base64.b64encode(b'{"ok":true}').decode(),
        }
    },
}


@pytest.fixture
def native_factory(monkeypatch: pytest.MonkeyPatch, fake_library: FakeLibrary) -> None:
    monkeypatch.setattr(
        session_module,
        "_default_core_factory",
        lambda config: NativeCore.create(config.core_create(), library=fake_library),
    )


def _connect(
    fake_library: FakeLibrary,
    native_factory: None,
    *,
    overrides: dict[str, tuple[str, dict[str, Any]]] | None = None,
    auth: dict[str, Any] | None = None,
) -> tuple[cynapsa.AztmSession, CompletionDriver]:
    del native_factory
    driver = CompletionDriver(fake_library)
    if overrides:
        driver.overrides.update(overrides)
    driver.start()
    return cynapsa.connect(**(AUTH if auth is None else auth)), driver


def test_native_payload_and_message_commands_match_frozen_vectors_exactly() -> None:
    vectors = Path(__file__).parents[1] / "src/cynapsa/_vendor/conformance/v1"
    payload_vector = next(
        item["json"]
        for item in json.loads((vectors / "payloads.json").read_text())
        if item["name"] == "native"
    )
    payload = cynapsa.NativePayload(
        "application/octet-stream", "/native", b"\x00\xff"
    )
    expected_payload = json.dumps(
        payload_vector, ensure_ascii=False, separators=(",", ":")
    ).encode()
    assert payload.canonical_json() == expected_payload

    commands = {
        item["name"]: item["json"]
        for item in json.loads((vectors / "commands.json").read_text())
    }
    vector_payload = cynapsa.NativePayload("application/json", "/orders", b"x")
    for name, args in (
        (
            "message.send",
            {"to": "agent-b@example.test", "payload": vector_payload._wire()},
        ),
        (
            "message.request",
            {
                "to": "agent-b@example.test",
                "payload": vector_payload._wire(),
                "ttl_ms": 1000,
            },
        ),
        (
            "message.reply",
            {"request_handle": VALID_REQUEST_HANDLE, "payload": vector_payload._wire()},
        ),
    ):
        _, encoded = encode_command(
            name, "session-1", args, command_id="command-1"
        )
        expected = json.dumps(
            commands[name], ensure_ascii=False, separators=(",", ":")
        ).encode()
        assert encoded == expected

    results = {
        item["name"]: item["json"]
        for item in json.loads((vectors / "results.json").read_text())
    }
    send = decode_completion(json.dumps(results["send"], separators=(",", ":")).encode())
    response = decode_completion(
        json.dumps(results["response"], separators=(",", ":")).encode()
    )
    assert send.result_type == "send" and send.result == {
        "message_id": "m",
        "conversation_id": "c",
        "accepted": True,
    }
    assert response.result_type == "response"
    assert response.result is not None
    assert response.result["payload"]["native"]["body"] == "dmFsdWU="


def test_payload_value_conversions_are_deterministic_and_unambiguous() -> None:
    class PayloadSubclass(cynapsa.NativePayload):
        pass

    raw = cynapsa.NativePayload.from_value(b"\x00\xff", path="/bytes")
    text = cynapsa.NativePayload.from_value("שלום", path="/text")
    structured = cynapsa.NativePayload.from_value(
        {"z": [1, True, None], "a": "é"}, path="/json"
    )

    assert (raw.content_type, raw.body) == ("application/octet-stream", b"\x00\xff")
    assert text.content_type == "text/plain; charset=utf-8"
    assert text.body.decode() == "שלום"
    assert structured.content_type == "application/json"
    assert structured.body == '{"a":"é","z":[1,true,null]}'.encode()
    assert cynapsa.AztmResponse("m", "c", "a", "mesh", structured).json() == {
        "a": "é",
        "z": [1, True, None],
    }

    for value in (
        (1, 2),
        bytearray(b"x"),
        {1, 2},
        object(),
        PayloadSubclass("", "/", b"x"),
    ):
        with pytest.raises(TypeError):
            cynapsa.NativePayload.from_value(value)
    for value in (math.nan, math.inf, -math.inf, {"x": [math.nan]}):
        with pytest.raises(ValueError):
            cynapsa.NativePayload.from_value(value)
    with pytest.raises(TypeError):
        cynapsa.NativePayload.from_json({1: "not a JSON object key"})
    with pytest.raises(ValueError):
        cynapsa.NativePayload.from_value(raw, path="/override")


def test_payload_utf8_path_content_type_inline_bounds_and_immutability() -> None:
    exact_path = "/" + "é" * 1023 + "a"
    exact_content_type = "é" * 256
    payload = cynapsa.NativePayload(exact_content_type, exact_path, b"")
    assert len(payload.path.encode()) == 2048
    assert len(payload.content_type.encode()) == 512
    with pytest.raises(FrozenInstanceError):
        payload.body = b"changed"  # type: ignore[misc]

    for path in ("", "relative", "/query?x=1", "/fragment#x", exact_path + "b", "/\ud800"):
        with pytest.raises(ValueError):
            cynapsa.NativePayload("", path, b"")
    for content_type in (exact_content_type + "a", "text/plain\r", "text/plain\n", "x/\ud800"):
        with pytest.raises(ValueError):
            cynapsa.NativePayload(content_type, "/", b"")
    with pytest.raises(TypeError):
        cynapsa.NativePayload("", "/", bytearray(b"mutable"))  # type: ignore[arg-type]

    at_limit = cynapsa.NativePayload("", "/", b"x" * (MAX_INLINE_PAYLOAD_BYTES - 1))
    assert len(at_limit.body) == MAX_INLINE_PAYLOAD_BYTES - 1
    with pytest.raises(ValueError, match="payload handles"):
        cynapsa.NativePayload("", "/", b"x" * MAX_INLINE_PAYLOAD_BYTES)


def test_response_body_text_json_strictness_and_no_correlation_id() -> None:
    payload = cynapsa.NativePayload("text/plain; charset=iso-8859-1", "/", b"caf\xe9")
    response = cynapsa.AztmResponse("m", "c", "a", "mesh", payload)
    assert response.body == b"caf\xe9"
    assert response.text() == "café"
    assert not hasattr(response, "correlation_id")

    assert cynapsa.AztmResponse(
        "m", "c", "a", "mesh", cynapsa.NativePayload("", "/", b"value")
    ).text() == "value"
    with pytest.raises(ValueError):
        cynapsa.AztmResponse(
            "m",
            "c",
            "a",
            "mesh",
            cynapsa.NativePayload("application/octet-stream", "/", b"value"),
        ).text()
    for body in (
        b"\xff",
        b'{"x":NaN}',
        b'{"x":1,"x":2}',
        b'{"x":1e999}',
        b'{"x":"\\ud800"}',
        b"\xef\xbb\xbf{}",
    ):
        with pytest.raises((UnicodeDecodeError, ValueError, json.JSONDecodeError)):
            cynapsa.AztmResponse(
                "m", "c", "a", "mesh", cynapsa.NativePayload("application/json", "/", body)
            ).json()

    with pytest.raises(ValueError):
        cynapsa.AztmResponse(
            "m",
            "c",
            "a",
            "mesh",
            cynapsa.NativePayload("text/plain; charset*=utf-8''utf-8", "/", b"value"),
        ).text()


def test_http_response_model_text_json_immutability_and_content_type() -> None:
    payload = cynapsa.HTTPResponsePayload(
        200,
        "OK",
        (("content-type", "text/plain; charset=iso-8859-1"),),
        b"caf\xe9",
    )
    response = cynapsa.AztmResponse("m", "c", "a", "mesh", payload)
    assert response.body == b"caf\xe9"
    assert response.text() == "café"
    with pytest.raises(FrozenInstanceError):
        response.payload = cynapsa.NativePayload("", "/", b"")  # type: ignore[misc]
    with pytest.raises(TypeError):
        cynapsa.AztmResponse(  # type: ignore[arg-type]
            "m",
            "c",
            "a",
            "mesh",
            cynapsa.HTTPRequestPayload("GET", "/"),
        )
    assert cynapsa.AztmResponse(
        "m",
        "c",
        "a",
        "mesh",
        cynapsa.HTTPResponsePayload(200, "OK", (), b'{"default":"utf8"}'),
    ).json() == {"default": "utf8"}
    ambiguous = cynapsa.AztmResponse(
        "m",
        "c",
        "a",
        "mesh",
        cynapsa.HTTPResponsePayload(
            200,
            "OK",
            (
                ("content-type", "application/json"),
                ("Content-Type", "text/plain"),
            ),
            b"{}",
        ),
    )
    with pytest.raises(ValueError, match="ambiguous"):
        ambiguous.text()
    with pytest.raises(ValueError, match="ambiguous"):
        ambiguous.json()
    empty = cynapsa.AztmResponse(
        "m",
        "c",
        "a",
        "mesh",
        cynapsa.HTTPResponsePayload(200, "OK", (("content-type", ""),), b"value"),
    )
    with pytest.raises(ValueError, match="empty"):
        empty.text()


def test_json_text_is_always_utf8_and_content_type_controls_fail_closed() -> None:
    for media_type in ("application/json", "application/problem+json"):
        response = cynapsa.AztmResponse(
            "m",
            "c",
            "a",
            "mesh",
            cynapsa.HTTPResponsePayload(
                200,
                "OK",
                (("content-type", f"{media_type}; charset=iso-8859-1"),),
                '"café"'.encode("utf-8"),
            ),
        )
        assert response.text() == '"café"'
        assert response.json() == "café"

    for control in ("\x00", "\x1f", "\x7f"):
        response = cynapsa.AztmResponse(
            "m",
            "c",
            "a",
            "mesh",
            cynapsa.HTTPResponsePayload(
                200,
                "OK",
                (("content-type", f'text/plain; note="a{control}b"'),),
                b"value",
            ),
        )
        with pytest.raises(ValueError, match="malformed"):
            response.text()

    # Horizontal tab is valid quoted-string content and remains accepted.
    tabbed = cynapsa.AztmResponse(
        "m",
        "c",
        "a",
        "mesh",
        cynapsa.HTTPResponsePayload(
            200,
            "OK",
            (("content-type", 'text/plain; note="a\tb"'),),
            b"value",
        ),
    )
    assert tabbed.text() == "value"

    # HTTP header values use the public 8,192-byte field limit, not the
    # native-payload content_type identifier limit of 512 bytes.
    long_but_valid = cynapsa.AztmResponse(
        "m",
        "c",
        "a",
        "mesh",
        cynapsa.HTTPResponsePayload(
            200,
            "OK",
            (("content-type", f'text/plain; note="{"a" * 600}"'),),
            b"value",
        ),
    )
    assert long_but_valid.text() == "value"


@pytest.mark.parametrize(
    "content_type",
    [
        "text/plain; =bad",
        "text/plain; bad",
        'text/plain; foo="unterminated',
        "text/plain;",
        "text/plain; charset*=utf-8''utf-8",
        'text/plain; charset=""',
    ],
)
def test_http_content_type_malformed_parameters_fail_closed_for_text_and_json(
    content_type: str,
) -> None:
    response = cynapsa.AztmResponse(
        "m",
        "c",
        "a",
        "mesh",
        cynapsa.HTTPResponsePayload(
            200,
            "OK",
            (("content-type", content_type),),
            b"{}",
        ),
    )
    with pytest.raises(ValueError, match="malformed|invalid|unknown"):
        response.text()
    with pytest.raises(ValueError, match="malformed"):
        response.json()

    request = cynapsa.CynapsaRequest(
        "POST",
        "/",
        "",
        (("content-type", content_type),),
        b"{}",
        message_id="m",
        conversation_id="c",
        from_agent_id="a",
        mesh_id="mesh",
        mode="rpc",
    )
    with pytest.raises(ValueError, match="malformed|invalid|unknown"):
        request.text()
    with pytest.raises(ValueError, match="malformed"):
        request.json()


def test_payload_base64_decoder_is_strict_and_copies_exact_bytes() -> None:
    source = b"\x00value\xff"
    payload = session_module._native_payload_from_wire(
        {
            "native": {
                "content_type": "application/octet-stream",
                "path": "/",
                "body": base64.b64encode(source).decode(),
            }
        }
    )
    assert payload.body == source
    assert payload.body is not source
    for body in ("dmFsdWU", "dmFsdWU===", "dmFsdW$=", "_w=="):
        with pytest.raises(ValueError):
            session_module._native_payload_from_wire(
                {"native": {"content_type": "", "path": "/", "body": body}}
            )


def test_send_is_local_acceptance_not_remote_delivery(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    session, driver = _connect(
        fake_library,
        native_factory,
        overrides={"message.send": ("send", SEND_RESULT)},
    )
    try:
        result = session.send(
            "agent-b@example.test", {"event": "started"}, path="/events/started"
        )
        assert result == cynapsa.AztmSendResult(
            "message-one", "conversation-one", True
        )
        assert result.accepted_by_core is True
        assert not hasattr(result, "seq")
        command = next(c for c in driver.commands if c["command_name"] == "message.send")
        assert set(command["args"]) == {"to", "payload"}
        wire = command["args"]["payload"]["http_request"]
        assert wire["method"] == "POST"
        assert wire["path"] == "/events/started"
        assert wire["headers"] == [
            {"name": "content-type", "value": "application/json"}
        ]
        assert wire["body"] == base64.b64encode(
            b'{"event":"started"}'
        ).decode()
        assert not any(c["command_name"] == "delivery.accept" for c in driver.commands)
    finally:
        try:
            session.close()
        except NativeError:
            pass
        driver.stop()


def test_request_returns_canonical_application_response_and_maps_ttl(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    session, driver = _connect(
        fake_library,
        native_factory,
        overrides={"message.request": ("response", RESPONSE_RESULT)},
    )
    try:
        response = session.request(
            "agent-b@example.test", {"op": "ping"}, path="/ping", ttl_ms=1250
        )
        assert type(response) is cynapsa.CynapsaResponse
        assert response.status_code == 200
        assert response.reason == "OK"
        assert response.json() == {"ok": True}
        command = next(c for c in driver.commands if c["command_name"] == "message.request")
        assert command["args"]["ttl_ms"] == 1250
        assert "timeout_ms" not in command["args"]
        assert "correlation_id" not in command["args"]
    finally:
        try:
            session.close()
        except NativeError:
            pass
        driver.stop()


def test_remote_application_error_is_a_response_and_local_authorization_is_an_exception(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    application_response = {
        **RESPONSE_RESULT,
        "payload": {
            "http_response": {
                "status_code": 403,
                "reason": "Forbidden",
                "headers": [
                    {"name": "x-cynapsa-error", "value": "ordinary application header"}
                ],
                "body": base64.b64encode(b'{"denied":true}').decode(),
                "error": {
                    "code": "forbidden",
                    "detail": "Access denied",
                    "details_json": '{"safe":true}',
                },
            }
        },
    }
    session, driver = _connect(
        fake_library,
        native_factory,
        overrides={"message.request": ("response", application_response)},
    )
    try:
        response = session.request("agent-b@example.test", "request")
        assert response.status_code == 403
        assert response.headers == (
            ("x-cynapsa-error", "ordinary application header"),
        )
        assert response.error == cynapsa.CynapsaApplicationError(
            "forbidden", "Access denied", {"safe": True}
        )

        authorization = {
            "abi_version": 1,
            "command_id": "$COMMAND_ID",
            "ok": False,
            "error": {
                "code": "authorization_rejected",
                "message": PUBLIC_ERROR_MESSAGES["authorization_rejected"],
                "retryable": False,
                "stage": "policy",
                "local_or_remote": "local",
            },
        }
        driver.overrides["message.send"] = json.dumps(
            authorization, separators=(",", ":")
        ).encode()
        with pytest.raises(NativeError) as raised:
            session.send("agent-b@example.test", "denied")
        assert raised.value.code == "authorization_rejected"
        assert raised.value.details == {
            "retryable": False,
            "stage": "policy",
            "local_or_remote": "local",
        }
    finally:
        session.close()
        driver.stop()


def test_send_and_request_accept_http_request_payload_and_parse_http_response(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    session, driver = _connect(
        fake_library,
        native_factory,
        overrides={
            "message.send": ("send", SEND_RESULT),
            "message.request": ("response", HTTP_RESPONSE_RESULT),
        },
    )
    request_payload = cynapsa.HTTPRequestPayload(
        "POST",
        "/orders",
        "a=1&a=2",
        (("x-dup", "one"), ("x-dup", "two")),
        b"body\x00",
    )
    try:
        assert session.send("agent-b@example.test", request_payload).accepted
        response = session.request(
            "agent-b@example.test", request_payload, ttl_ms=1000
        )
        assert type(response) is cynapsa.CynapsaResponse
        assert response.status_code == 207
        assert response.reason == "Multi-Status"
        assert response.headers == (
            ("content-type", "application/json"),
            ("x-dup", "one"),
            ("x-dup", "two"),
        )
        assert response.body == b'{"ok":true}'
        assert response.json() == {"ok": True}

        commands = [
            c for c in driver.commands if c["command_name"] in {"message.send", "message.request"}
        ]
        assert commands[0]["args"]["payload"]["http_request"]["headers"] == [
            {"name": "x-dup", "value": "one"},
            {"name": "x-dup", "value": "two"},
        ]
        assert base64.b64decode(commands[0]["args"]["payload"]["http_request"]["body"]) == b"body\x00"
        with pytest.raises(ValueError):
            session.send("agent-b@example.test", request_payload, path="/override")
        with pytest.raises(TypeError):
            session.request(
                "agent-b@example.test",
                cynapsa.HTTPResponsePayload(200, "OK"),
            )
    finally:
        session.close()
        driver.stop()


@pytest.mark.asyncio
async def test_async_request_accepts_http_request_payload(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library)
    driver.overrides["message.request"] = ("response", HTTP_RESPONSE_RESULT)
    driver.start()
    session = await cynapsa.connect_async(**AUTH)
    try:
        response = await session.request(
            "agent-b@example.test",
            cynapsa.HTTPRequestPayload(
                "GET",
                "/async",
                "",
                (("content-type", "application/json"),),
                b"{}",
            ),
        )
        assert type(response) is cynapsa.CynapsaResponse
        assert response.status_code == 207
        command = next(c for c in driver.commands if c["command_name"] == "message.request")
        assert set(command["args"]["payload"]) == {"http_request"}
    finally:
        await session.close()
        driver.stop()


@pytest.mark.asyncio
async def test_async_send_and_request_reject_response_as_outbound_request(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library)
    driver.overrides["message.request"] = ("response", RESPONSE_RESULT)
    driver.start()
    session = await cynapsa.connect_async(**AUTH)
    try:
        before = len(driver.commands)
        with pytest.raises(TypeError):
            await session.send(
                "agent-b@example.test",
                cynapsa.HTTPResponsePayload(200, "OK"),
            )
        with pytest.raises(TypeError):
            await session.request(
                "agent-b@example.test",
                cynapsa.HTTPResponsePayload(200, "OK"),
            )
        assert len(driver.commands) == before

        response = await session.request(
            "agent-b@example.test",
            cynapsa.CynapsaRequest("GET", "/"),
        )
        assert type(response) is cynapsa.CynapsaResponse
        assert response.json() == {"ok": True}
    finally:
        await session.close()
        driver.stop()


def test_request_rejects_obsolete_native_response_variant(
    fake_library: FakeLibrary,
    native_factory: None,
) -> None:
    completion_payload = {
        "native": {
            "content_type": "application/json",
            "path": "/",
            "body": base64.b64encode(b'{"ok":true}').decode(),
        }
    }
    session, driver = _connect(
        fake_library,
        native_factory,
        overrides={
            "message.request": (
                "response",
                {**RESPONSE_RESULT, "payload": completion_payload},
            )
        },
    )
    try:
        with pytest.raises(NativeError) as raised:
            session.request("agent-b@example.test", cynapsa.CynapsaRequest("GET", "/"))
        assert raised.value.code == "core_error"
    finally:
        try:
            session.close()
        except NativeError:
            pass
        driver.stop()


@pytest.mark.parametrize("ttl_ms", [-1, True, MAX_TIMEOUT_MS + 1])
def test_request_rejects_invalid_ttl_before_submit(
    ttl_ms: Any, fake_library: FakeLibrary, native_factory: None
) -> None:
    session, driver = _connect(fake_library, native_factory)
    try:
        before = len(driver.commands)
        with pytest.raises(ValueError):
            session.request("agent-b@example.test", b"x", ttl_ms=ttl_ms)
        assert len(driver.commands) == before
    finally:
        session.close()
        driver.stop()


def test_request_zero_preserves_wire_zero_and_uses_sdk_safety_timeout(
    fake_library: FakeLibrary,
    native_factory: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session_module, "_RPC_COMPLETION_SAFETY_WINDOW_MS", 5)
    driver = CompletionDriver(fake_library)
    driver.stalled.add("message.request")
    driver.start()
    session = cynapsa.connect(**{**AUTH, "rpc_timeout_ms": 20})
    try:
        with pytest.raises(cynapsa.SdkSafetyTimeout, match="safety deadline"):
            session.request("agent-b@example.test", b"x", ttl_ms=0)
        command = next(c for c in driver.commands if c["command_name"] == "message.request")
        assert command["args"]["ttl_ms"] == 0
        assert set(command["args"]) == {"to", "payload", "ttl_ms"}
        assert fake_library.calls["core_cancel"] == 1
        assert json.loads(fake_library.cancel_inputs[-1])["handle"].startswith("cmdh_")
        assert session._owner.core is not None
        assert session._owner.core.pending_command_count == 1
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_COMPLETION,
            _failure(command, "request_cancelled"),
        )
        deadline = time.monotonic() + 1
        while session._owner.core.pending_command_count:
            assert time.monotonic() < deadline
            time.sleep(0.001)
        assert session._owner.core.pending_command_count == 0
    finally:
        session.close()
        driver.stop()


def test_request_safety_timeout_is_drained_by_close_when_core_never_completes(
    fake_library: FakeLibrary,
    native_factory: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session_module, "_RPC_COMPLETION_SAFETY_WINDOW_MS", 5)
    driver = CompletionDriver(fake_library)
    driver.stalled.add("message.request")
    driver.start()
    session = cynapsa.connect(**{**AUTH, "rpc_timeout_ms": 20})

    with pytest.raises(cynapsa.SdkSafetyTimeout):
        session.request("agent-b@example.test", b"x")
    core = session._owner.core
    assert core is not None
    assert core.pending_command_count == 1

    session.close()
    driver.stop()
    assert core.pending_command_count == 0
    assert session._owner.core is None
    assert fake_library.calls["core_destroy"] == 1


def test_concurrent_close_waits_for_request_safety_cancel_and_drains_pending(
    fake_library: FakeLibrary,
    native_factory: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session_module, "_RPC_COMPLETION_SAFETY_WINDOW_MS", 5)
    driver = CompletionDriver(fake_library)
    driver.stalled.add("message.request")
    driver.start()
    session = cynapsa.connect(**{**AUTH, "rpc_timeout_ms": 20})
    request_errors: list[BaseException] = []
    close_errors: list[BaseException] = []

    def requester() -> None:
        try:
            session.request("agent-b@example.test", b"x")
        except BaseException as exc:
            request_errors.append(exc)

    def closer() -> None:
        try:
            session.close()
        except BaseException as exc:
            close_errors.append(exc)

    request_thread = threading.Thread(target=requester)
    request_thread.start()
    deadline = time.monotonic() + 1
    while not any(
        command["command_name"] == "message.request"
        for command in driver.commands
    ):
        assert time.monotonic() < deadline
        time.sleep(0.001)
    core = session._owner.core
    assert core is not None

    close_thread = threading.Thread(target=closer)
    close_thread.start()
    request_thread.join(1)
    close_thread.join(1)
    driver.stop()

    assert len(request_errors) == 1
    assert isinstance(request_errors[0], cynapsa.SdkSafetyTimeout)
    assert not close_errors
    assert fake_library.calls["core_cancel"] == 1
    assert core.pending_command_count == 0
    assert session._owner.core is None


@pytest.mark.asyncio
async def test_async_request_zero_preserves_wire_zero_and_uses_sdk_safety_timeout(
    fake_library: FakeLibrary,
    native_factory: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session_module, "_RPC_COMPLETION_SAFETY_WINDOW_MS", 5)
    driver = CompletionDriver(fake_library)
    driver.stalled.add("message.request")
    driver.start()
    session = await cynapsa.connect_async(**{**AUTH, "rpc_timeout_ms": 20})
    try:
        with pytest.raises(cynapsa.SdkSafetyTimeout, match="safety deadline"):
            await session.request("agent-b@example.test", b"x", ttl_ms=0)
        command = next(c for c in driver.commands if c["command_name"] == "message.request")
        assert command["args"]["ttl_ms"] == 0
        assert set(command["args"]) == {"to", "payload", "ttl_ms"}
        assert fake_library.calls["core_cancel"] == 1
        assert session._owner.core is not None
        assert session._owner.core.pending_command_count == 1
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_COMPLETION,
            _failure(command, "request_cancelled"),
        )
        deadline = time.monotonic() + 1
        while session._owner.core.pending_command_count:
            assert time.monotonic() < deadline
            await asyncio.sleep(0.001)
        assert session._owner.core.pending_command_count == 0
    finally:
        await session.close()
        driver.stop()


def test_request_accepts_delayed_core_rpc_timeout_within_safety_window(
    fake_library: FakeLibrary,
    native_factory: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session_module, "_RPC_COMPLETION_SAFETY_WINDOW_MS", 25)
    driver = CompletionDriver(fake_library)
    driver.stalled.add("message.request")
    driver.start()
    session = cynapsa.connect(**AUTH)
    errors: list[BaseException] = []

    def invoke() -> None:
        try:
            session.request("agent-b@example.test", b"x", ttl_ms=10)
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=invoke)
    thread.start()
    deadline = time.monotonic() + 1
    while not any(c["command_name"] == "message.request" for c in driver.commands):
        assert time.monotonic() < deadline
        time.sleep(0.001)
    command = next(c for c in driver.commands if c["command_name"] == "message.request")
    assert command["args"]["ttl_ms"] == 10
    time.sleep(0.01)
    fake_library.emit_callback(
        CYNAPSA_CALLBACK_V1_COMPLETION,
        _failure(command, "rpc_timeout"),
    )
    thread.join(1)
    try:
        assert len(errors) == 1
        assert isinstance(errors[0], NativeError)
        assert errors[0].code == "rpc_timeout"  # type: ignore[attr-defined]
        assert fake_library.calls["core_cancel"] == 0
    finally:
        session.close()
        driver.stop()


@pytest.mark.asyncio
async def test_async_request_accepts_delayed_core_rpc_timeout_within_safety_window(
    fake_library: FakeLibrary,
    native_factory: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session_module, "_RPC_COMPLETION_SAFETY_WINDOW_MS", 25)
    driver = CompletionDriver(fake_library)
    driver.stalled.add("message.request")
    driver.start()
    session = await cynapsa.connect_async(**AUTH)
    try:
        task = asyncio.create_task(
            session.request("agent-b@example.test", b"x", ttl_ms=10)
        )
        deadline = time.monotonic() + 1
        while not any(c["command_name"] == "message.request" for c in driver.commands):
            assert time.monotonic() < deadline
            await asyncio.sleep(0.001)
        command = next(
            c for c in driver.commands if c["command_name"] == "message.request"
        )
        assert command["args"]["ttl_ms"] == 10
        await asyncio.sleep(0.01)
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_COMPLETION,
            _failure(command, "rpc_timeout"),
        )
        with pytest.raises(NativeError) as raised:
            await task
        assert raised.value.code == "rpc_timeout"
        assert fake_library.calls["core_cancel"] == 0
    finally:
        await session.close()
        driver.stop()


def test_maximum_request_ttl_is_forwarded_without_a_second_field(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    session, driver = _connect(
        fake_library,
        native_factory,
        overrides={"message.request": ("response", RESPONSE_RESULT)},
    )
    try:
        session.request("agent-b@example.test", b"x", ttl_ms=MAX_TIMEOUT_MS)
        args = next(c for c in driver.commands if c["command_name"] == "message.request")["args"]
        assert args["ttl_ms"] == MAX_TIMEOUT_MS
        assert set(args) == {"to", "payload", "ttl_ms"}
    finally:
        session.close()
        driver.stop()


def test_request_wait_timeout_adds_exact_safety_window_without_wire_overflow() -> None:
    assert session_module._request_wait_timeout_seconds(1) == 5.001
    assert session_module._request_wait_timeout_seconds(30_000) == 35.0
    assert session_module._request_wait_timeout_seconds(MAX_TIMEOUT_MS) == (
        MAX_TIMEOUT_MS + 5_000
    ) / 1_000


def test_configured_payload_limit_fails_closed_without_handle_or_chunking(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    session, driver = _connect(
        fake_library,
        native_factory,
        auth={**AUTH, "payload_limit": 8},
    )
    try:
        before = len(driver.commands)
        with pytest.raises(NativeError) as raised:
            session.send("agent-b@example.test", b"12345678")
        assert raised.value.code == "payload_too_large"
        assert raised.value.details["stage"] == "payload"
        assert len(driver.commands) == before
        assert not any(c["command_name"].startswith("payload.") for c in driver.commands)
    finally:
        session.close()
        driver.stop()


def test_configured_payload_limit_counts_public_application_fields(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    session, driver = _connect(
        fake_library,
        native_factory,
        auth={**AUTH, "payload_limit": 13},
        overrides={"message.send": ("send", SEND_RESULT)},
    )
    try:
        assert session.send(
            "agent-b@example.test",
            cynapsa.NativePayload("", "/", b"12345678"),
        ).accepted
        before = len(driver.commands)
        with pytest.raises(NativeError) as raised:
            session.send(
                "agent-b@example.test",
                cynapsa.NativePayload("", "/", b"123456789"),
            )
        assert raised.value.code == "payload_too_large"
        assert len(driver.commands) == before
    finally:
        session.close()
        driver.stop()


def test_response_payload_must_obey_configured_application_limit(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    response = {
        **RESPONSE_RESULT,
        "payload": {
            "native": {
                "content_type": "",
                "path": "/",
                "body": base64.b64encode(b"12345678").decode(),
            }
        },
    }
    session, driver = _connect(
        fake_library,
        native_factory,
        auth={**AUTH, "payload_limit": 8},
        overrides={"message.request": ("response", response)},
    )
    try:
        with pytest.raises(NativeError) as raised:
            session.request("agent-b@example.test", cynapsa.NativePayload("", "/", b""))
        assert raised.value.code == "core_error"
    finally:
        session.close()
        driver.stop()


@pytest.mark.asyncio
async def test_async_request_cancellation_calls_direct_core_cancel(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library)
    driver.stalled.add("message.request")
    driver.start()
    session = await cynapsa.connect_async(**AUTH)
    task = asyncio.create_task(
        session.request("agent-b@example.test", b"request", ttl_ms=10_000)
    )
    try:
        deadline = time.monotonic() + 1
        while not any(c["command_name"] == "message.request" for c in driver.commands):
            assert time.monotonic() < deadline
            await asyncio.sleep(0.001)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert fake_library.calls["core_cancel"] == 1
    finally:
        await session.close()
        driver.stop()


@pytest.mark.parametrize(
    ("result_type", "result"),
    [
        ("empty", {}),
        ("send", {**SEND_RESULT, "accepted": False}),
        ("send", {**SEND_RESULT, "secret": "must-not-escape"}),
        ("send", {**SEND_RESULT, "message_id": ""}),
        ("future_send", SEND_RESULT),
    ],
)
def test_malformed_send_results_fail_closed_without_secret_or_internal_terms(
    result_type: str,
    result: dict[str, Any],
    fake_library: FakeLibrary,
    native_factory: None,
) -> None:
    session, driver = _connect(
        fake_library,
        native_factory,
        overrides={"message.send": (result_type, result)},
    )
    try:
        with pytest.raises(NativeError) as raised:
            session.send("agent-b@example.test", b"x")
        assert raised.value.code == "core_error"
        assert "secret" not in str(raised.value).lower()
        assert "result_type" not in str(raised.value).lower()
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None
    finally:
        try:
            session.close()
        except NativeError:
            pass
        driver.stop()


def test_non_native_or_correlated_response_result_fails_closed(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    correlated = {**RESPONSE_RESULT, "correlation_id": "private"}
    session, driver = _connect(
        fake_library,
        native_factory,
        overrides={"message.request": ("response", correlated)},
    )
    try:
        with pytest.raises(NativeError) as raised:
            session.request("agent-b@example.test", b"x", ttl_ms=100)
        assert raised.value.code == "core_error"
        assert "correlation" not in str(raised.value).lower()
    finally:
        try:
            session.close()
        except NativeError:
            pass
        driver.stop()


def test_concurrent_send_close_and_method_after_close_are_safe(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library)
    driver.stalled.add("message.send")
    driver.start()
    session = cynapsa.connect(**{**AUTH, "command_timeout_ms": 20})
    send_errors: list[BaseException] = []
    close_errors: list[BaseException] = []

    def sender() -> None:
        try:
            session.send("agent-b@example.test", b"x")
        except BaseException as exc:
            send_errors.append(exc)

    def closer() -> None:
        try:
            session.close()
        except BaseException as exc:
            close_errors.append(exc)

    send_thread = threading.Thread(target=sender)
    send_thread.start()
    deadline = time.monotonic() + 1
    while not any(c["command_name"] == "message.send" for c in driver.commands):
        assert time.monotonic() < deadline
        time.sleep(0.001)
    close_thread = threading.Thread(target=closer)
    close_thread.start()
    send_thread.join(2)
    close_thread.join(2)
    driver.stop()
    assert len(send_errors) == 1 and isinstance(send_errors[0], TimeoutError)
    assert not close_errors
    assert fake_library.calls["core_cancel"] == 1
    with pytest.raises(NativeError) as raised:
        session.send("agent-b@example.test", b"after close")
    assert raised.value.code == "shutdown_in_progress"
