from __future__ import annotations

import asyncio
import base64
import dataclasses
import importlib
import inspect
import io
import json
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any

import aiohttp
import httpx
import pytest
import requests
import urllib3
from fastapi import FastAPI, Request

import cynapsa
import cynapsa._http_bridge as bridge_module
import cynapsa.http as http_module
import cynapsa.session as session_module
from conftest import FakeLibrary
from cynapsa.exceptions import NativeError
from cynapsa.native.abi import (
    CYNAPSA_CALLBACK_V1_COMPLETION,
    CYNAPSA_CALLBACK_V1_EVENT,
)
from cynapsa.native.command import (
    MAX_ABI_INPUT_BYTES,
    MAX_INLINE_PAYLOAD_BYTES,
    PUBLIC_ERROR_MESSAGES,
)
from cynapsa.native.core import NativeCore


AUTH = {
    "mesh_endpoint": "Mesh.Example.TEST:05222",
    "username": "input-agent@example.test",
    "password": "credential-do-not-retain",
    "mesh_id": "mesh-one",
    "command_timeout_ms": 500,
    "queue_limit": 8,
    "payload_limit": 1_048_576,
}

ENROLLMENT_TOKEN = (
    "cpsa_e1.01234567-89ab-4def-8123-456789abcdef."
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
)


def _success(command: dict[str, Any], result_type: str, result: dict[str, Any]) -> bytes:
    return json.dumps(
        {
            "abi_version": 1,
            "command_id": command["command_id"],
            "ok": True,
            "result_type": result_type,
            "result": result,
        },
        separators=(",", ":"),
    ).encode()


def _failure(command: dict[str, Any], code: str) -> bytes:
    return json.dumps(
        {
            "abi_version": 1,
            "command_id": command["command_id"],
            "ok": False,
            "error": {
                "code": code,
                "message": PUBLIC_ERROR_MESSAGES[code],
                "retryable": False,
                "stage": "command",
                "local_or_remote": "local",
            },
        },
        separators=(",", ":"),
    ).encode()


class BridgeDriver:
    def __init__(self, library: FakeLibrary) -> None:
        self.library = library
        self.commands: list[dict[str, Any]] = []
        self.stalled: set[str] = set()
        self.failures: dict[str, str] = {}
        self.responses: dict[str, tuple[str, dict[str, Any]]] = {}
        self.responder: Callable[[dict[str, Any]], bytes | None] | None = None
        self._seen = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> BridgeDriver:
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(2)

    def _document(self, command: dict[str, Any]) -> bytes:
        name = command["command_name"]
        if self.responder is not None:
            response = self.responder(command)
            if response is not None:
                return response
        if name in self.failures:
            return _failure(command, self.failures[name])
        if name in self.responses:
            kind, result = self.responses[name]
            return _success(command, kind, result)
        if name == "core.init":
            return _success(
                command, "core_init", {"sdk_session_id": command["sdk_session_id"]}
            )
        if name == "auth.login":
            args = command["args"]
            return _success(
                command,
                "auth",
                {
                    "agent_id": "bridge-agent@example.test",
                    "mesh_id": args["mesh_id"],
                    "agent_instance_id": args["agent_instance_id"],
                    "personality": "http_bridge",
                },
            )
        if name in {"auth.token_login", "auth.installation_login"}:
            args = command["args"]
            return _success(
                command,
                "auth",
                {
                    "agent_id": "01234567-89ab-4def-8123-456789abcdef",
                    "mesh_id": args["mesh_id"],
                    "agent_instance_id": "11234567-89ab-4def-8123-456789abcdef",
                    "personality": "http_bridge",
                    "profile_id": args.get("profile_id", "default"),
                    "credential_expires_at": "2026-09-12T12:00:00Z",
                    "offline_start_deadline": "2026-09-12T11:55:00Z",
                    "offline_cold_start_target_seconds": 300,
                    "offline_target_satisfied": True,
                    "policy_revision": 7,
                    "session_expiry_mode": "continue",
                    "preparation_status": "ready",
                },
            )
        if name == "message.send" or name == "message.reply":
            return _success(
                command,
                "send",
                {"message_id": "m", "conversation_id": "c", "accepted": True},
            )
        if name == "core.status":
            return _success(
                command,
                "status",
                {
                    "lifecycle": "ready",
                    "connectivity": "available",
                    "personality": "http_bridge",
                    "agent_id": "bridge-agent@example.test",
                    "mesh_id": "mesh-one",
                    "mesh_endpoint": "mesh.example.test:5222",
                    "queued_message_count": 0,
                },
            )
        if name == "address.map.list":
            mappings: dict[str, str] = {}
            for submitted in self.commands:
                if submitted["command_name"] == "address.map.put":
                    args = submitted["args"]
                    mappings[args["virtual_origin"]] = args["recipient"]
                elif submitted["command_name"] == "address.map.remove":
                    mappings.pop(submitted["args"]["virtual_origin"], None)
            return _success(
                command,
                "address_mappings",
                {
                    "mappings": [
                        {
                            "virtual_origin": origin,
                            "recipient": recipient,
                        }
                        for origin, recipient in mappings.items()
                    ]
                },
            )
        return _success(command, "empty", {})

    def _run(self) -> None:
        while not self._stop.is_set():
            if self._seen >= len(self.library.submit_inputs):
                self._stop.wait(0.001)
                continue
            command = json.loads(self.library.submit_inputs[self._seen])
            self._seen += 1
            self.commands.append(command)
            if command["command_name"] in self.stalled:
                continue
            deadline = time.monotonic() + 1
            while self.library.callback is None and not self._stop.is_set():
                if time.monotonic() >= deadline:
                    return
                self._stop.wait(0.001)
            if not self._stop.is_set():
                self.library.emit_callback(
                    CYNAPSA_CALLBACK_V1_COMPLETION, self._document(command)
                )


@pytest.fixture
def bridge_factory(
    monkeypatch: pytest.MonkeyPatch, fake_library: FakeLibrary
) -> BridgeDriver:
    monkeypatch.setattr(
        session_module,
        "_default_core_factory",
        lambda config: NativeCore.create(config.core_create(), library=fake_library),
    )
    driver = BridgeDriver(fake_library).start()
    yield driver
    driver.stop()


def _rpc_response(
    *,
    status: int = 201,
    reason: str = "Created",
    body: bytes = b"ok",
    headers: list[dict[str, str]] | None = None,
) -> tuple[str, dict[str, Any]]:
    return (
        "response",
        {
            "message_id": "response-message",
            "conversation_id": "conversation",
            "from_agent_id": "peer@example.test",
            "mesh_id": "mesh-one",
            "payload": {
                "http_response": {
                    "status_code": status,
                    "reason": reason,
                    "headers": headers
                    if headers is not None
                    else [
                        {"name": "set-cookie", "value": "a=1"},
                        {"name": "set-cookie", "value": "b=2"},
                    ],
                    "body": base64.b64encode(body).decode(),
                }
            },
        },
    )


def test_canonical_http_payload_roundtrip_and_exact_limits() -> None:
    request = cynapsa.HTTPRequestPayload(
        "POST",
        "/orders/%2Fraw",
        "a=1&a=2",
        (("x-a", "one"), ("x-a", "two")),
        b"\x00\xff",
    )
    assert request.canonical_json() == (
        b'{"http_request":{"method":"POST","path":"/orders/%2Fraw",'
        b'"query":"a=1&a=2","headers":[{"name":"x-a","value":"one"},'
        b'{"name":"x-a","value":"two"}],"body":"AP8="}}'
    )
    assert cynapsa.HTTPRequestPayload.from_canonical_json(request.canonical_json()) == request
    assert request.headers == (("x-a", "one"), ("x-a", "two"))
    assert request.inline_size == 39

    response = cynapsa.HTTPResponsePayload(
        201,
        "Created",
        (("set-cookie", "a=1"), ("set-cookie", "b=2")),
        b"\x00\xff",
    )
    assert cynapsa.HTTPResponsePayload.from_canonical_json(response.canonical_json()) == response
    assert response.inline_size == 35

    exact = cynapsa.HTTPRequestPayload(
        "G", "/", "", (), b"x" * (MAX_INLINE_PAYLOAD_BYTES - 2)
    )
    assert exact.inline_size == MAX_INLINE_PAYLOAD_BYTES
    with pytest.raises(ValueError):
        cynapsa.HTTPRequestPayload(
            "G", "/", "", (), b"x" * (MAX_INLINE_PAYLOAD_BYTES - 1)
        )
    for kwargs in (
        {"method": "GET bad", "path": "/"},
        {"method": "GET", "path": "relative"},
        {"method": "GET", "path": "/bad?x"},
        {"method": "GET", "path": "/", "query": "x" * 8193},
        {"method": "GET", "path": "/", "headers": (("x", "bad\r\n"),)},
    ):
        with pytest.raises(ValueError):
            cynapsa.HTTPRequestPayload(**kwargs)
    with pytest.raises(ValueError):
        cynapsa.HTTPRequestPayload.from_canonical_json(
            b'{"http_request":{"method":"GET","path":"/","query":"",'
            b'"headers":[],"body":"YQ"}}'
        )


def test_canonical_http_all_metadata_byte_boundaries_and_immutability() -> None:
    max_path = "/" + "é" * 1023 + "a"
    max_query = "é" * 4096
    max_value = "é" * 4096
    max_reason = "é" * 256
    headers = [("x" * 512, max_value)] + [("x", "") for _ in range(4095)]
    request = cynapsa.HTTPRequestPayload(
        "M" * 32, max_path, max_query, headers, b""
    )
    response = cynapsa.HTTPResponsePayload(599, max_reason, headers, b"")
    assert len(request.path.encode()) == 2048
    assert len(request.query.encode()) == 8192
    assert len(response.reason.encode()) == 512
    assert len(request.headers) == 4096

    body = b"immutable"
    payload = cynapsa.HTTPRequestPayload("GET", "/", body=body)
    assert payload.body == body
    with pytest.raises(dataclasses.FrozenInstanceError):
        payload.path = "/changed"  # type: ignore[misc]

    invalid_requests = (
        {"method": "M" * 33, "path": "/"},
        {"method": "GéT", "path": "/"},
        {"method": "GET", "path": max_path + "b"},
        {"method": "GET", "path": "/", "query": max_query + "q"},
        {"method": "GET", "path": "/", "headers": (("x" * 513, ""),)},
        {"method": "GET", "path": "/", "headers": (("x", max_value + "v"),)},
        {
            "method": "GET",
            "path": "/",
            "headers": tuple(("x", "") for _ in range(4097)),
        },
    )
    for fields in invalid_requests:
        with pytest.raises(ValueError):
            cynapsa.HTTPRequestPayload(**fields)

    for status in (True, 99, 600, 200.0):
        with pytest.raises(ValueError):
            cynapsa.HTTPResponsePayload(status)  # type: ignore[arg-type]
    for reason in (max_reason + "r", "ok\r", "ok\n"):
        with pytest.raises(ValueError):
            cynapsa.HTTPResponsePayload(200, reason)


@pytest.mark.parametrize(
    "body",
    ["YQ", "YQ=", "YQ===", "Y Q==", "YQ==\n", "-Q==", "====", 1],
)
def test_canonical_http_rejects_noncanonical_base64(body: object) -> None:
    document = {
        "http_response": {
            "status_code": 200,
            "reason": "OK",
            "headers": [],
            "body": body,
        }
    }
    raw = json.dumps(document, separators=(",", ":")).encode()
    with pytest.raises((TypeError, ValueError)):
        cynapsa.HTTPResponsePayload.from_canonical_json(raw)


def test_canonical_http_rejects_duplicate_unknown_and_multiple_variants() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        cynapsa.HTTPRequestPayload.from_canonical_json(
            b'{"http_request":{"method":"GET","method":"POST","path":"/",'
            b'"query":"","headers":[],"body":""}}'
        )
    with pytest.raises(ValueError):
        cynapsa.HTTPResponsePayload.from_canonical_json(
            b'{"http_response":{"status_code":200,"reason":"OK",'
            b'"headers":[],"body":"","private":1}}'
        )
    for wire in ({}, {"native": {}, "http_request": {}}, {"unknown": {}}):
        with pytest.raises(ValueError):
            http_module._http_payload_from_wire(wire)


def test_canonical_http_json_decoder_rejects_oversized_documents_before_parsing() -> None:
    with pytest.raises(ValueError, match="ABI input limit"):
        cynapsa.HTTPRequestPayload.from_canonical_json(
            b" " * (MAX_ABI_INPUT_BYTES + 1)
        )
    with pytest.raises(ValueError):
        cynapsa.HTTPRequestPayload.from_canonical_json(
            b'{"http_request":' + (b"[" * 2_000) + (b"]" * 2_000) + b"}"
        )


def test_public_payload_limit_accounting_uses_application_fields_only() -> None:
    for size in (0, 1, 23, 24, 255, 256, 65_535, 65_536):
        request = cynapsa.HTTPRequestPayload("G", "/", body=b"x" * size)
        assert request.inline_size == 2 + size
        request.validate_payload_limit(request.inline_size)
        with pytest.raises(ValueError):
            request.validate_payload_limit(request.inline_size - 1)


def test_url_and_origin_parsing_matches_go_address_rules() -> None:
    accepted = {
        "HTTP://Agent.Example:080": ("http://agent.example", "/", ""),
        "https://agent.example:8443/a%2fb": (
            "https://agent.example:8443",
            "/a%2fb",
            "",
        ),
        "http://127.0.0.1/pé?x=1&x=%2f": (
            "http://127.0.0.1",
            "/p%C3%A9",
            "x=1&x=%2f",
        ),
    }
    for raw, expected in accepted.items():
        parsed = bridge_module._parse_address(raw, origin_only=False)
        assert (parsed.key, parsed.path, parsed.query) == expected
    for raw in (
        "ftp://agent.example",
        "https://user@agent.example",
        "https://agent.example./",
        "https://mésh.example/",
        "https://[2001:db8::1]/",
        "https://agent.example:0/",
        "https://agent.example:65536/",
        "https://agent.example/%zz",
        "https://agent.example/path#fragment",
        "https://agent.exa\nmple/path",
    ):
        with pytest.raises(ValueError):
            bridge_module._parse_address(raw, origin_only=False)
    for raw in (
        "https://agent.example/",
        "https://agent.example?x=1",
        "https://agent.example#fragment",
    ):
        with pytest.raises(ValueError):
            bridge_module._parse_address(raw, origin_only=True)


def test_selective_requests_and_httpx_rpc_and_restoration(
    bridge_factory: BridgeDriver, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge_factory.responses["message.request"] = _rpc_response()
    original = requests.sessions.Session.send
    bypassed: list[str] = []

    def bypass(session: Any, request: Any, **kwargs: Any) -> requests.Response:
        bypassed.append(request.url)
        response = requests.Response()
        response.status_code = 299
        response.url = request.url
        response._content = b"bypass"
        return response

    monkeypatch.setattr(requests.sessions.Session, "send", bypass)
    handle = cynapsa.login(
        **AUTH,
        address_map={
            "HTTP://Agent.Example:80": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        mapped = requests.post(
            "http://AGENT.example/orders?a=1&a=2",
            data=b"request-body",
            headers={"x-a": "one"},
        )
        assert isinstance(mapped, requests.Response)
        assert (mapped.status_code, mapped.reason, mapped.content) == (201, "Created", b"ok")
        assert mapped.raw.headers.getlist("set-cookie") == ["a=1", "b=2"]

        unmapped = requests.get("http://unmapped.example/path")
        assert unmapped.status_code == 299
        assert bypassed == ["http://unmapped.example/path"]

        transport_calls: list[httpx.Request] = []

        def transport(request: httpx.Request) -> httpx.Response:
            transport_calls.append(request)
            return httpx.Response(298, content=b"original")

        with httpx.Client(transport=httpx.MockTransport(transport)) as client:
            response = client.post(
                "http://agent.example/hx?q=1",
                content=b"hx",
                headers=[("x-a", "1"), ("x-a", "2")],
            )
            assert isinstance(response, httpx.Response)
            assert response.status_code == 201
            assert response.headers.get_list("set-cookie") == ["a=1", "b=2"]
            assert client.get("http://elsewhere.example/").status_code == 298
        assert len(transport_calls) == 1

        pool = urllib3.PoolManager()
        direct = pool.request(
            "POST",
            "http://agent.example/urllib3?x=1",
            body=b"u3",
            headers=urllib3._collections.HTTPHeaderDict(
                [("x-a", "one"), ("x-a", "two")]
            ),
        )
        assert isinstance(direct, urllib3.HTTPResponse)
        assert (direct.status, direct.reason, direct.data) == (201, "Created", b"ok")
        assert direct.headers.getlist("set-cookie") == ["a=1", "b=2"]

        status = handle.status()
        assert status.personality == "http_bridge"
        assert handle.mappings() == (
            cynapsa.AddressMapping(
                "HTTP://Agent.Example:80", "peer@example.test", "rpc"
            ),
        )

        messages = [
            item for item in bridge_factory.commands if item["command_name"] == "message.request"
        ]
        assert len(messages) == 3
        request_payload = messages[0]["args"]["payload"]["http_request"]
        assert request_payload["path"] == "/orders"
        assert request_payload["query"] == "a=1&a=2"
        assert base64.b64decode(request_payload["body"]) == b"request-body"
        assert not [
            item
            for item in bridge_factory.commands
            if item.get("args", {}).get("to") == "unmapped.example"
        ]
    finally:
        handle.close()
    assert requests.sessions.Session.send is bypass
    monkeypatch.setattr(requests.sessions.Session, "send", original)


def test_sync_bridge_mixes_rpc_and_msg_per_origin_for_all_clients(
    bridge_factory: BridgeDriver,
) -> None:
    bridge_factory.responses["message.request"] = _rpc_response(
        status=207, reason="Multi-Status", body=b"remote"
    )
    handle = cynapsa.login(
        **AUTH,
        address_map={
            "http://rpc.example": {
                "recipient": "rpc-peer@example.test",
                "mode": "rpc",
            },
            "http://msg.example": {
                "recipient": "msg-peer@example.test",
                "mode": "msg",
            },
        },
    )
    try:
        rpc_requests = requests.get("http://rpc.example/requests")
        msg_requests = requests.get("http://msg.example/requests")
        assert isinstance(rpc_requests, requests.Response)
        assert isinstance(msg_requests, requests.Response)
        assert (rpc_requests.status_code, rpc_requests.content) == (207, b"remote")
        assert (msg_requests.status_code, msg_requests.content) == (200, b"")

        with httpx.Client() as client:
            rpc_httpx = client.get("http://rpc.example/httpx")
            msg_httpx = client.get("http://msg.example/httpx")
        assert isinstance(rpc_httpx, httpx.Response)
        assert isinstance(msg_httpx, httpx.Response)
        assert (rpc_httpx.status_code, rpc_httpx.content) == (207, b"remote")
        assert (msg_httpx.status_code, msg_httpx.content) == (200, b"")

        pool = urllib3.PoolManager()
        rpc_urllib3 = pool.request("GET", "http://rpc.example/urllib3")
        msg_urllib3 = pool.request("GET", "http://msg.example/urllib3")
        assert isinstance(rpc_urllib3, urllib3.HTTPResponse)
        assert isinstance(msg_urllib3, urllib3.HTTPResponse)
        assert (rpc_urllib3.status, rpc_urllib3.data) == (207, b"remote")
        assert msg_urllib3.status == 200
        assert msg_urllib3.data in (b"", None)

        assert handle.status().personality == "http_bridge"
        assert handle.mappings() == (
            cynapsa.AddressMapping(
                "http://rpc.example", "rpc-peer@example.test", "rpc"
            ),
            cynapsa.AddressMapping(
                "http://msg.example", "msg-peer@example.test", "msg"
            ),
        )
        routed = [
            command
            for command in bridge_factory.commands
            if command["command_name"] in {"message.request", "message.send"}
        ]
        assert [command["command_name"] for command in routed] == [
            "message.request",
            "message.send",
        ] * 3
        assert [command["args"]["to"] for command in routed] == [
            "rpc-peer@example.test",
            "msg-peer@example.test",
        ] * 3
        puts = [
            command["args"]
            for command in bridge_factory.commands
            if command["command_name"] == "address.map.put"
        ]
        assert puts == [
            {
                "virtual_origin": "http://rpc.example",
                "recipient": "rpc-peer@example.test",
            },
            {
                "virtual_origin": "http://msg.example",
                "recipient": "msg-peer@example.test",
            },
        ]
    finally:
        handle.close()


def test_native_client_response_metadata_cookies_json_and_versions(
    bridge_factory: BridgeDriver,
) -> None:
    bridge_factory.responses["message.request"] = _rpc_response(
        status=203,
        reason="Non-Authoritative Information",
        body=b'{"ok":true}',
        headers=[
            {"name": "content-type", "value": "application/json; charset=utf-8"},
            {"name": "set-cookie", "value": "a=1; Path=/"},
            {"name": "set-cookie", "value": "b=2; Path=/"},
            {"name": "x-dup", "value": "one"},
            {"name": "x-dup", "value": "two"},
        ],
    )
    handle = cynapsa.login(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        with requests.Session() as session:
            response = session.get("http://agent.example/data?q=%2f")
            assert response.request.url == response.url
            assert response.json() == {"ok": True}
            assert response.encoding == "utf-8"
            assert response.cookies.get_dict() == {"a": "1", "b": "2"}
            assert session.cookies.get_dict() == {"a": "1", "b": "2"}
            assert response.raw.version == 11
            assert response.raw.headers.getlist("x-dup") == ["one", "two"]
            assert response.headers["x-dup"] == "one, two"

        with httpx.Client() as client:
            hx = client.get("http://agent.example/hx")
            assert hx.request.url == httpx.URL("http://agent.example/hx")
            assert hx.json() == {"ok": True}
            assert hx.http_version == "HTTP/1.1"
            assert hx.headers.get_list("x-dup") == ["one", "two"]
            assert dict(hx.cookies) == {"a": "1", "b": "2"}

        pool = urllib3.HTTPConnectionPool("agent.example", port=80)
        u3 = pool.urlopen("GET", "/pool?q=1", preload_content=False)
        assert u3.version == 11
        assert u3.url == "http://agent.example/pool?q=1"
        assert u3.read() == b'{"ok":true}'
    finally:
        handle.close()


def test_streaming_bodies_fail_before_core_submission(
    bridge_factory: BridgeDriver,
) -> None:
    handle = cynapsa.login(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        before = len(bridge_factory.commands)
        with pytest.raises(TypeError):
            requests.post("http://agent.example/stream", data=iter([b"x"]))
        request = httpx.Request(
            "POST", "http://agent.example/stream", content=iter([b"x"])
        )
        with httpx.Client() as client, pytest.raises(TypeError):
            client.send(request)
        pool = urllib3.HTTPConnectionPool("agent.example")
        with pytest.raises(TypeError):
            pool.urlopen("POST", "/stream", body=iter([b"x"]))
        assert len(bridge_factory.commands) == before
    finally:
        handle.close()


def test_requests_rpc_timeout_uses_native_timeout_exception_and_configured_ttl(
    bridge_factory: BridgeDriver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session_module, "_RPC_COMPLETION_SAFETY_WINDOW_MS", 25)
    bridge_factory.stalled.add("message.request")
    handle = cynapsa.login(
        **{**AUTH, "rpc_timeout_ms": 20},
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    errors: list[BaseException] = []

    def invoke() -> None:
        try:
            requests.get("http://agent.example/requests-timeout")
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=invoke)
    thread.start()
    deadline = time.monotonic() + 1
    while not any(
        item["command_name"] == "message.request" for item in bridge_factory.commands
    ):
        assert time.monotonic() < deadline
        time.sleep(0.001)
    command = next(
        item for item in bridge_factory.commands if item["command_name"] == "message.request"
    )
    bridge_factory.library.emit_callback(
        CYNAPSA_CALLBACK_V1_COMPLETION,
        _failure(command, "rpc_timeout"),
    )
    thread.join(1)
    try:
        assert len(errors) == 1
        assert isinstance(errors[0], requests.exceptions.ReadTimeout)
        assert errors[0].request is not None  # type: ignore[attr-defined]
        assert errors[0].request.url == "http://agent.example/requests-timeout"  # type: ignore[attr-defined]
        assert errors[0].__cause__ is None
        assert errors[0].__context__ is None
        assert command["args"]["ttl_ms"] == 20
        assert set(command["args"]) == {"to", "payload", "ttl_ms"}
        assert bridge_factory.library.calls["core_cancel"] == 0
    finally:
        handle.close()


def test_httpx_sync_rpc_timeout_uses_native_timeout_exception_and_request_ttl(
    bridge_factory: BridgeDriver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session_module, "_RPC_COMPLETION_SAFETY_WINDOW_MS", 25)
    bridge_factory.stalled.add("message.request")
    handle = cynapsa.login(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    errors: list[BaseException] = []

    def invoke() -> None:
        try:
            with httpx.Client() as client:
                client.get("http://agent.example/httpx-timeout", timeout=0.021)
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=invoke)
    thread.start()
    deadline = time.monotonic() + 1
    while not any(
        item["command_name"] == "message.request" for item in bridge_factory.commands
    ):
        assert time.monotonic() < deadline
        time.sleep(0.001)
    command = next(
        item for item in bridge_factory.commands if item["command_name"] == "message.request"
    )
    assert command["args"]["ttl_ms"] == 21
    time.sleep(0.01)
    bridge_factory.library.emit_callback(
        CYNAPSA_CALLBACK_V1_COMPLETION,
        _failure(command, "rpc_timeout"),
    )
    thread.join(1)
    try:
        assert len(errors) == 1
        assert isinstance(errors[0], httpx.ReadTimeout)
        assert errors[0].request.url == "http://agent.example/httpx-timeout"  # type: ignore[attr-defined]
        assert errors[0].__cause__ is None
        assert errors[0].__context__ is None
        assert bridge_factory.library.calls["core_cancel"] == 0
    finally:
        handle.close()


@pytest.mark.asyncio
async def test_httpx_async_rpc_timeout_uses_native_timeout_exception_and_request_ttl(
    bridge_factory: BridgeDriver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session_module, "_RPC_COMPLETION_SAFETY_WINDOW_MS", 25)
    bridge_factory.stalled.add("message.request")
    handle = await cynapsa.login_async(
        **AUTH,
        address_map={
            "https://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        async with httpx.AsyncClient() as client:
            task = asyncio.create_task(
                client.get("https://agent.example/httpx-async-timeout", timeout=0.021)
            )
            deadline = time.monotonic() + 1
            while not any(
                item["command_name"] == "message.request"
                for item in bridge_factory.commands
            ):
                assert time.monotonic() < deadline
                await asyncio.sleep(0.001)
            command = next(
                item
                for item in bridge_factory.commands
                if item["command_name"] == "message.request"
            )
            assert command["args"]["ttl_ms"] == 21
            await asyncio.sleep(0.01)
            bridge_factory.library.emit_callback(
                CYNAPSA_CALLBACK_V1_COMPLETION,
                _failure(command, "rpc_timeout"),
            )
            with pytest.raises(httpx.ReadTimeout) as raised:
                await task
            assert raised.value.request.url == (
                "https://agent.example/httpx-async-timeout"
            )
            assert raised.value.__cause__ is None
            assert raised.value.__context__ is None
            assert bridge_factory.library.calls["core_cancel"] == 0
    finally:
        await handle.close()


def test_urllib3_rpc_timeout_uses_native_timeout_exception_and_request_ttl(
    bridge_factory: BridgeDriver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session_module, "_RPC_COMPLETION_SAFETY_WINDOW_MS", 25)
    bridge_factory.stalled.add("message.request")
    handle = cynapsa.login(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    errors: list[BaseException] = []

    def invoke() -> None:
        try:
            pool = urllib3.PoolManager()
            pool.request("GET", "http://agent.example/urllib3-timeout", timeout=0.02)
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=invoke)
    thread.start()
    deadline = time.monotonic() + 1
    while not any(
        item["command_name"] == "message.request" for item in bridge_factory.commands
    ):
        assert time.monotonic() < deadline
        time.sleep(0.001)
    command = next(
        item for item in bridge_factory.commands if item["command_name"] == "message.request"
    )
    bridge_factory.library.emit_callback(
        CYNAPSA_CALLBACK_V1_COMPLETION,
        _failure(command, "rpc_timeout"),
    )
    thread.join(1)
    try:
        assert len(errors) == 1
        assert isinstance(errors[0], urllib3.exceptions.ReadTimeoutError)
        assert errors[0].url == "http://agent.example/urllib3-timeout"  # type: ignore[attr-defined]
        assert errors[0].__cause__ is None
        assert errors[0].__context__ is None
        assert command["args"]["ttl_ms"] == 20
        assert bridge_factory.library.calls["core_cancel"] == 0
    finally:
        handle.close()


def test_sync_http_hooks_translate_sdk_safety_timeout_to_native_exceptions(
    bridge_factory: BridgeDriver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session_module, "_RPC_COMPLETION_SAFETY_WINDOW_MS", 5)
    bridge_factory.stalled.add("message.request")
    handle = cynapsa.login(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        with pytest.raises(requests.exceptions.ReadTimeout) as requests_error:
            requests.get("http://agent.example/requests-safety", timeout=0.005)
        assert requests_error.value.request.url == (
            "http://agent.example/requests-safety"
        )
        assert requests_error.value.__cause__ is None
        assert requests_error.value.__context__ is None

        with httpx.Client() as client:
            with pytest.raises(httpx.ReadTimeout) as httpx_error:
                client.get("http://agent.example/httpx-safety", timeout=0.005)
        assert httpx_error.value.request.url == "http://agent.example/httpx-safety"
        assert httpx_error.value.__cause__ is None
        assert httpx_error.value.__context__ is None

        pool = urllib3.PoolManager()
        with pytest.raises(urllib3.exceptions.ReadTimeoutError) as urllib3_error:
            pool.request(
                "GET", "http://agent.example/urllib3-safety", timeout=0.005
            )
        assert urllib3_error.value.url == "http://agent.example/urllib3-safety"
        assert urllib3_error.value.__cause__ is None
        assert urllib3_error.value.__context__ is None

        requests_sent = [
            command
            for command in bridge_factory.commands
            if command["command_name"] == "message.request"
        ]
        assert [command["args"]["ttl_ms"] for command in requests_sent] == [5, 5, 5]
        assert bridge_factory.library.calls["core_cancel"] == 3
    finally:
        handle.close()


@pytest.mark.asyncio
async def test_httpx_async_translates_sdk_safety_timeout_to_read_timeout(
    bridge_factory: BridgeDriver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session_module, "_RPC_COMPLETION_SAFETY_WINDOW_MS", 5)
    bridge_factory.stalled.add("message.request")
    handle = await cynapsa.login_async(
        **AUTH,
        address_map={
            "https://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        async with httpx.AsyncClient() as client:
            with pytest.raises(httpx.ReadTimeout) as raised:
                await client.get(
                    "https://agent.example/httpx-async-safety", timeout=0.005
                )
        assert raised.value.request.url == (
            "https://agent.example/httpx-async-safety"
        )
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None
        command = next(
            command
            for command in bridge_factory.commands
            if command["command_name"] == "message.request"
        )
        assert command["args"]["ttl_ms"] == 5
        assert bridge_factory.library.calls["core_cancel"] == 1
    finally:
        await handle.close()


def test_urllib_request_string_rpc_response_has_native_file_interface(
    bridge_factory: BridgeDriver,
) -> None:
    bridge_factory.responses["message.request"] = _rpc_response(
        status=206,
        reason="Partial Content",
        body=b"first\nsecond\n",
        headers=[
            {"name": "content-type", "value": "text/plain"},
            {"name": "x-duplicate", "value": "one"},
            {"name": "x-duplicate", "value": "two"},
        ],
    )
    original = urllib.request.urlopen
    handle = cynapsa.login(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        assert urllib.request.urlopen is not original
        response = urllib.request.urlopen("http://agent.example/items?q=1&q=2")
        with response as opened:
            assert opened is response
            assert (opened.status, opened.code, opened.reason) == (
                206,
                206,
                "Partial Content",
            )
            assert opened.url == "http://agent.example/items?q=1&q=2"
            assert opened.geturl() == opened.url
            assert opened.getcode() == 206
            assert opened.info() is opened.headers
            assert opened.msg == "Partial Content"
            assert opened.headers.get_all("x-duplicate") == ["one", "two"]
            target = bytearray(5)
            assert opened.readinto(target) == 5
            assert bytes(target) == b"first"
            assert opened.readline() == b"\n"
            assert opened.readlines() == [b"second\n"]
            with pytest.raises(io.UnsupportedOperation):
                opened.fileno()
        assert response.closed

        with urllib.request.urlopen("http://agent.example/iter") as iterable:
            assert list(iterable) == [b"first\n", b"second\n"]

        commands = [
            command
            for command in bridge_factory.commands
            if command["command_name"] == "message.request"
        ]
        assert len(commands) == 2
        wire = commands[0]["args"]["payload"]["http_request"]
        assert wire["method"] == "GET"
        assert wire["path"] == "/items"
        assert wire["query"] == "q=1&q=2"
        assert base64.b64decode(wire["body"]) == b""
    finally:
        handle.close()
    assert urllib.request.urlopen is original


def test_urllib_request_request_object_preserves_method_headers_body_and_timeout(
    bridge_factory: BridgeDriver,
) -> None:
    bridge_factory.responses["message.request"] = _rpc_response()
    handle = cynapsa.login(
        **AUTH,
        address_map={
            "https://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        request = urllib.request.Request(
            "https://agent.example/custom/path?encoded=%2F",
            data=b"original",
            headers={"X-Normal": "one"},
            method="PATCH",
        )
        request.add_unredirected_header("X-Unredirected", "two")
        with urllib.request.urlopen(request, b"override", 0.031) as response:
            assert response.read() == b"ok"

        retained = urllib.request.Request(
            "https://agent.example/retained",
            data=b"retained",
            headers={"X-Retained": "yes"},
        )
        with urllib.request.urlopen(retained, timeout=0.042) as response:
            assert response.read() == b"ok"

        commands = [
            command
            for command in bridge_factory.commands
            if command["command_name"] == "message.request"
        ]
        assert len(commands) == 2
        command = commands[0]
        assert command["args"]["ttl_ms"] == 31
        wire = command["args"]["payload"]["http_request"]
        assert wire["method"] == "PATCH"
        assert wire["path"] == "/custom/path"
        assert wire["query"] == "encoded=%2F"
        assert base64.b64decode(wire["body"]) == b"override"
        headers = {item["name"].lower(): item["value"] for item in wire["headers"]}
        assert headers["x-normal"] == "one"
        assert headers["x-unredirected"] == "two"
        assert headers["content-length"] == str(len(b"override"))
        assert headers["content-type"] == "application/x-www-form-urlencoded"
        assert headers["host"] == "agent.example"
        assert headers["user-agent"].startswith("Python-urllib/")
        assert headers["connection"] == "close"
        assert request.data == b"override"

        retained_command = commands[1]
        assert retained_command["args"]["ttl_ms"] == 42
        retained_wire = retained_command["args"]["payload"]["http_request"]
        assert retained_wire["method"] == "POST"
        assert base64.b64decode(retained_wire["body"]) == b"retained"
    finally:
        handle.close()


def test_urllib_request_uses_wire_header_precedence_and_context_defaults(
    bridge_factory: BridgeDriver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bridge_factory.responses["message.request"] = _rpc_response()

    class CustomOpener:
        addheaders = [("User-agent", "custom-global"), ("X-Global", "present")]

    monkeypatch.setattr(urllib.request, "_opener", CustomOpener())
    handle = cynapsa.login(
        **AUTH,
        address_map={
            "https://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        request = urllib.request.Request(
            "https://agent.example/headers",
            headers={"X-Same": "normal"},
        )
        request.add_unredirected_header("X-Same", "unredirected")
        with urllib.request.urlopen(request, timeout=0.1):
            pass
        with urllib.request.urlopen(
            "https://agent.example/context", timeout=0.1, context=object()
        ):
            pass

        commands = [
            item
            for item in bridge_factory.commands
            if item["command_name"] == "message.request"
        ]
        first_headers = {
            item["name"].lower(): item["value"]
            for item in commands[0]["args"]["payload"]["http_request"]["headers"]
        }
        assert first_headers["x-same"] == "unredirected"
        assert first_headers["user-agent"] == "custom-global"
        assert first_headers["x-global"] == "present"
        assert first_headers["connection"] == "close"

        context_headers = {
            item["name"].lower(): item["value"]
            for item in commands[1]["args"]["payload"]["http_request"]["headers"]
        }
        assert context_headers["user-agent"].startswith("Python-urllib/")
        assert "x-global" not in context_headers
    finally:
        handle.close()


def test_urllib_request_rejects_invalid_owned_urls_without_network_escape(
    bridge_factory: BridgeDriver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delegated: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def original(*args: Any, **kwargs: Any) -> object:
        delegated.append((args, kwargs))
        return object()

    monkeypatch.setattr(urllib.request, "urlopen", original)
    handle = cynapsa.login(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        for value in (
            "http://agent.example/path#fragment",
            "http://user:secret@agent.example/path",
        ):
            with pytest.raises(ValueError, match="url is invalid"):
                urllib.request.urlopen(value)
        assert delegated == []
        assert not any(
            item["command_name"] in {"message.request", "message.send"}
            for item in bridge_factory.commands
        )
    finally:
        handle.close()


def test_urllib_request_argument_and_timeout_validation_matches_runtime(
    bridge_factory: BridgeDriver,
) -> None:
    bridge_factory.responses["message.request"] = _rpc_response()
    handle = cynapsa.login(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        with pytest.raises(TypeError, match="timeout must be a number or None"):
            urllib.request.urlopen("http://agent.example/bad-timeout", timeout="1")
        with pytest.raises(TypeError):
            urllib.request.urlopen("http://agent.example/duplicate", None, data=b"x")
        if "cafile" in inspect.signature(urllib.request.urlopen).parameters:
            with urllib.request.urlopen(
                "http://agent.example/legacy", cafile=None
            ) as response:
                assert response.read() == b"ok"
        else:
            with pytest.raises(TypeError):
                urllib.request.urlopen("http://agent.example/legacy", cafile=None)
    finally:
        handle.close()


def test_urllib_request_returns_http_error_status_as_buffered_response(
    bridge_factory: BridgeDriver,
) -> None:
    bridge_factory.responses["message.request"] = _rpc_response(
        status=404,
        reason="Not Found",
        body=b"missing",
        headers=[{"name": "content-type", "value": "text/plain"}],
    )
    handle = cynapsa.login(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        with urllib.request.urlopen("http://agent.example/missing") as response:
            assert not isinstance(response, urllib.error.HTTPError)
            assert (response.status, response.reason, response.msg) == (
                404,
                "Not Found",
                "Not Found",
            )
            assert response.read() == b"missing"
    finally:
        handle.close()


def test_urllib_request_string_data_selects_post_and_msg_is_fixed_success(
    bridge_factory: BridgeDriver,
) -> None:
    handle = cynapsa.login(
        **AUTH,
        address_map={
            "http://msg.example": {
                "recipient": "peer@example.test",
                "mode": "msg",
            }
        },
    )
    try:
        with urllib.request.urlopen(
            "http://msg.example/submit?x=1", data=bytearray(b"body"), timeout=0.2
        ) as response:
            assert (response.status, response.code, response.reason) == (200, 200, "OK")
            assert response.info().items() == []
            assert response.read() == b""

        command = next(
            command
            for command in bridge_factory.commands
            if command["command_name"] == "message.send"
        )
        assert set(command["args"]) == {"to", "payload"}
        wire = command["args"]["payload"]["http_request"]
        assert (wire["method"], wire["path"], wire["query"]) == (
            "POST",
            "/submit",
            "x=1",
        )
        assert base64.b64decode(wire["body"]) == b"body"
    finally:
        handle.close()


def test_urllib_request_unmapped_call_delegates_exact_arguments(
    bridge_factory: BridgeDriver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    marker = object()

    def original(*args: Any, **kwargs: Any) -> object:
        calls.append((args, kwargs))
        return marker

    monkeypatch.setattr(urllib.request, "urlopen", original)
    handle = cynapsa.login(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    context = object()
    try:
        arguments = ("http://unmapped.example/path", b"body", 1.25)
        assert urllib.request.urlopen(*arguments, context=context) is marker
        assert calls == [(arguments, {"context": context})]

        bridge_factory.responses["message.request"] = _rpc_response(body=b"mapped")
        with urllib.request.urlopen(
            "http://agent.example/mapped", timeout=0.25
        ) as response:
            assert response.read() == b"mapped"
        assert calls == [(arguments, {"context": context})]
    finally:
        handle.close()
    assert urllib.request.urlopen is original


def test_urllib_request_maps_core_and_sdk_timeouts_to_urlerror(
    bridge_factory: BridgeDriver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bridge_factory.failures["message.request"] = "rpc_timeout"
    handle = cynapsa.login(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        with pytest.raises(urllib.error.URLError) as raised:
            urllib.request.urlopen("http://agent.example/core-timeout", None, 0.021)
        assert isinstance(raised.value.reason, TimeoutError)
        assert str(raised.value.reason) == "timed out"
        assert "rpc_timeout" not in str(raised.value)
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None
        command = next(
            command
            for command in bridge_factory.commands
            if command["command_name"] == "message.request"
        )
        assert command["args"]["ttl_ms"] == 21

        bridge_factory.failures.clear()
        bridge_factory.stalled.add("message.request")
        monkeypatch.setattr(session_module, "_RPC_COMPLETION_SAFETY_WINDOW_MS", 5)
        handle._bridge.owner.rpc_timeout_ms = 5
        with pytest.raises(urllib.error.URLError) as safety:
            urllib.request.urlopen("http://agent.example/sdk-timeout")
        assert isinstance(safety.value.reason, TimeoutError)
        assert str(safety.value.reason) == "timed out"
        assert "SDK" not in str(safety.value)
        assert safety.value.__cause__ is None
        assert safety.value.__context__ is None
    finally:
        handle.close()


@pytest.mark.parametrize(
    "body",
    ["text", io.BytesIO(b"file"), iter([b"chunk"])],
)
def test_urllib_request_rejects_unsupported_bodies_before_core_submission(
    bridge_factory: BridgeDriver,
    body: object,
) -> None:
    handle = cynapsa.login(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        before = len(bridge_factory.commands)
        request = urllib.request.Request("http://agent.example/body", data=body)
        with pytest.raises(TypeError, match="data must be bytes-like"):
            urllib.request.urlopen(request)
        assert len(bridge_factory.commands) == before
    finally:
        handle.close()


def test_urllib_request_memoryview_body_is_snapshotted(
    bridge_factory: BridgeDriver,
) -> None:
    source = bytearray(b"buffered")
    handle = cynapsa.login(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "msg",
            }
        },
    )
    try:
        with urllib.request.urlopen(
            "http://agent.example/body", data=memoryview(source)
        ) as response:
            assert response.read() == b""
        source[:] = b"XXXXXXXX"
        command = next(
            item
            for item in bridge_factory.commands
            if item["command_name"] == "message.send"
        )
        wire = command["args"]["payload"]["http_request"]
        assert base64.b64decode(wire["body"]) == b"buffered"
    finally:
        handle.close()


def test_urllib_request_patch_is_nested_safe_and_conflicts_fail_cleanly() -> None:
    manager = bridge_module._PatchManager()
    original = urllib.request.urlopen

    class Owner:
        def __init__(self, *origins: str) -> None:
            self.origin_keys = origins
            self._registered = False

    first = Owner("http://one.example")
    second = Owner("http://two.example")
    conflicting = Owner("http://one.example")
    manager.register(first)  # type: ignore[arg-type]
    installed = urllib.request.urlopen
    manager.register(second)  # type: ignore[arg-type]
    try:
        assert urllib.request.urlopen is installed
        with pytest.raises(ValueError, match="already owns"):
            manager.register(conflicting)  # type: ignore[arg-type]
        manager.unregister(first)  # type: ignore[arg-type]
        assert urllib.request.urlopen is installed
    finally:
        manager.unregister(first)  # type: ignore[arg-type]
        manager.unregister(second)  # type: ignore[arg-type]
    assert urllib.request.urlopen is original


def test_urllib_request_patch_rolls_back_when_later_installation_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = bridge_module._PatchManager()
    original = urllib.request.urlopen
    real_import = importlib.import_module

    def failing_import(name: str) -> Any:
        if name == "requests":
            raise RuntimeError("broken requests installation")
        return real_import(name)

    monkeypatch.setattr(importlib, "import_module", failing_import)
    with pytest.raises(RuntimeError, match="broken requests installation"):
        manager._install()
    assert urllib.request.urlopen is original
    assert manager._originals == []


def test_urllib_request_mapped_calls_are_thread_safe(
    bridge_factory: BridgeDriver,
) -> None:
    bridge_factory.responses["message.request"] = _rpc_response(body=b"parallel")
    handle = cynapsa.login(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    barrier = threading.Barrier(9)
    results: list[bytes] = []
    errors: list[BaseException] = []

    def invoke(index: int) -> None:
        try:
            barrier.wait()
            with urllib.request.urlopen(
                f"http://agent.example/concurrent/{index}", timeout=0.5
            ) as response:
                results.append(response.read())
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=invoke, args=(index,)) for index in range(8)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(2)
    try:
        assert not any(thread.is_alive() for thread in threads)
        assert errors == []
        assert results == [b"parallel"] * 8
        commands = [
            command
            for command in bridge_factory.commands
            if command["command_name"] == "message.request"
        ]
        assert len(commands) == 8
        assert all(command["args"]["ttl_ms"] == 500 for command in commands)
    finally:
        handle.close()


def test_urllib_request_concurrent_mapped_and_unmapped_calls_stay_separate(
    bridge_factory: BridgeDriver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = object()
    delegated: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def original(*args: Any, **kwargs: Any) -> object:
        delegated.append((args, kwargs))
        return marker

    monkeypatch.setattr(urllib.request, "urlopen", original)
    bridge_factory.responses["message.request"] = _rpc_response(body=b"mapped")
    handle = cynapsa.login(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    barrier = threading.Barrier(9)
    mapped: list[bytes] = []
    unmapped: list[object] = []
    errors: list[BaseException] = []

    def invoke(index: int, is_mapped: bool) -> None:
        try:
            barrier.wait()
            if is_mapped:
                with urllib.request.urlopen(
                    f"http://agent.example/mixed/{index}", timeout=0.5
                ) as response:
                    mapped.append(response.read())
            else:
                unmapped.append(
                    urllib.request.urlopen(
                        f"http://outside.example/mixed/{index}",
                        None,
                        0.75,
                        context=marker,
                    )
                )
        except BaseException as exc:
            errors.append(exc)

    threads = [
        threading.Thread(target=invoke, args=(index, index % 2 == 0))
        for index in range(8)
    ]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(2)
    try:
        assert not any(thread.is_alive() for thread in threads)
        assert errors == []
        assert mapped == [b"mapped"] * 4
        assert unmapped == [marker] * 4
        assert len(delegated) == 4
        assert all(args[2] == 0.75 for args, _kwargs in delegated)
        assert all(kwargs == {"context": marker} for _args, kwargs in delegated)
        commands = [
            item
            for item in bridge_factory.commands
            if item["command_name"] == "message.request"
        ]
        assert len(commands) == 4
    finally:
        handle.close()


def test_non_hooked_urllib_seam_remains_unpatched(bridge_factory: BridgeDriver) -> None:
    urllib_open = urllib.request.OpenerDirector.open
    handle = cynapsa.login(**AUTH, address_map={})
    try:
        assert urllib.request.OpenerDirector.open is urllib_open
    finally:
        handle.close()


@pytest.mark.asyncio
async def test_aiohttp_request_methods_params_headers_and_buffered_bodies(
    bridge_factory: BridgeDriver,
) -> None:
    bridge_factory.responses["message.request"] = _rpc_response(
        status=202,
        reason="Accepted",
        body=b"aiohttp",
        headers=[
            {"name": "content-type", "value": "text/plain; charset=utf-8"},
            {"name": "x-result", "value": "one"},
            {"name": "x-result", "value": "two"},
            {"name": "content-length", "value": "7"},
        ],
    )
    handle = await cynapsa.login_async(
        **AUTH,
        address_map={
            "https://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        async with aiohttp.ClientSession(
            headers=[("x-default", "one"), ("x-default", "two")]
        ) as client:
            responses = [
                await client.request(
                    "PATCH",
                    "https://agent.example/items?q=base",
                    params=[("q", "one"), ("q", "two")],
                    headers=[("x-request", "one"), ("x-request", "two")],
                    data=b"bytes",
                ),
                await client.get("https://agent.example/get"),
                await client.post("https://agent.example/text", data="hello"),
                await client.request(
                    "CUSTOM", "https://agent.example/json", json={"ok": True}
                ),
            ]
        assert all(isinstance(response, aiohttp.ClientResponse) for response in responses)
        assert [response.status for response in responses] == [202] * 4

        commands = [
            command
            for command in bridge_factory.commands
            if command["command_name"] == "message.request"
        ]
        assert [command["args"]["payload"]["http_request"]["method"] for command in commands] == [
            "PATCH",
            "GET",
            "POST",
            "CUSTOM",
        ]
        first = commands[0]["args"]["payload"]["http_request"]
        assert first["path"] == "/items"
        assert first["query"] == "q=base&q=one&q=two"
        assert base64.b64decode(first["body"]) == b"bytes"
        assert [
            header["value"]
            for header in first["headers"]
            if header["name"].lower() == "x-default"
        ] == ["one", "two"]
        assert [
            header["value"]
            for header in first["headers"]
            if header["name"].lower() == "x-request"
        ] == ["one", "two"]
        assert base64.b64decode(
            commands[2]["args"]["payload"]["http_request"]["body"]
        ) == b"hello"
        assert json.loads(
            base64.b64decode(
                commands[3]["args"]["payload"]["http_request"]["body"]
            )
        ) == {"ok": True}
    finally:
        await handle.close()


@pytest.mark.asyncio
async def test_aiohttp_response_is_native_buffered_and_supports_async_with(
    bridge_factory: BridgeDriver,
) -> None:
    bridge_factory.responses["message.request"] = _rpc_response(
        status=201,
        reason="Created",
        body=b'{"value": 3}',
        headers=[
            {"name": "content-type", "value": "application/json; charset=utf-8"},
            {"name": "content-length", "value": "12"},
            {"name": "x-duplicate", "value": "a"},
            {"name": "x-duplicate", "value": "b"},
        ],
    )
    handle = await cynapsa.login_async(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        async with aiohttp.ClientSession() as client:
            async with client.get("http://agent.example/data?q=1") as response:
                assert isinstance(response, aiohttp.ClientResponse)
                assert (response.status, response.reason) == (201, "Created")
                assert response.headers.getall("x-duplicate") == ["a", "b"]
                assert response.raw_headers[-2:] == (
                    (b"x-duplicate", b"a"),
                    (b"x-duplicate", b"b"),
                )
                assert str(response.url) == "http://agent.example/data?q=1"
                assert response.request_info.method == "GET"
                assert response.request_info.url == response.url
                assert response.history == ()
                assert not response.cookies
                assert response.content_length == 12
                assert response.content_type == "application/json"
                assert response.charset == "utf-8"
                assert response.content.is_eof()
                assert not response.content.at_eof()
                assert response._session is None
                assert await response.content.read(2) == b'{"'
                assert await response.read() == b'value": 3}'
                assert await response.read() == b'value": 3}'
                assert await response.text() == 'value": 3}'
                assert response.content.at_eof()
                assert response.closed
                response.release()
                await response.wait_for_close()
                response.close()
                assert response._released
                with pytest.raises(aiohttp.ClientConnectionError):
                    await response.read()
            async with client.get("http://agent.example/json") as json_response:
                assert await json_response.read() == b'{"value": 3}'
                assert await json_response.text() == '{"value": 3}'
                assert await json_response.json() == {"value": 3}
            assert response.closed
    finally:
        await handle.close()


@pytest.mark.asyncio
async def test_aiohttp_response_metadata_cookies_and_context_cleanup(
    bridge_factory: BridgeDriver,
) -> None:
    bridge_factory.responses["message.request"] = _rpc_response(
        status=418,
        reason="Teapot",
        body=b"tea",
        headers=[
            {"name": "content-type", "value": "text/plain; charset=latin-1"},
            {"name": "content-length", "value": "3"},
            {"name": "set-cookie", "value": "flavor=earl-grey; Path=/"},
        ],
    )
    handle = await cynapsa.login_async(
        **AUTH,
        address_map={
            "https://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    held: aiohttp.ClientResponse | None = None
    try:
        async with aiohttp.ClientSession() as client:
            with pytest.raises(RuntimeError, match="inside context"):
                async with client.get("https://agent.example/tea?q=1") as response:
                    held = response
                    assert response.url == response.real_url
                    assert response.request_info.real_url == response.real_url
                    assert response.method == "GET"
                    assert response.history == ()
                    assert response.cookies["flavor"].value == "earl-grey"
                    assert response.content_type == "text/plain"
                    assert response.charset == "latin-1"
                    assert response.content_length == 3
                    raise RuntimeError("inside context")
            assert held is not None
            assert held.closed and held._released
            await client.get("https://agent.example/next")
        requests_sent = [
            command
            for command in bridge_factory.commands
            if command["command_name"] == "message.request"
        ]
        second_headers = requests_sent[-1]["args"]["payload"]["http_request"][
            "headers"
        ]
        assert any(
            header["name"].lower() == "cookie"
            and "flavor=earl-grey" in header["value"]
            for header in second_headers
        )
    finally:
        await handle.close()


@pytest.mark.asyncio
async def test_aiohttp_preparation_auth_skip_headers_json_and_proxy_rejection(
    bridge_factory: BridgeDriver,
) -> None:
    bridge_factory.responses["message.request"] = _rpc_response(body=b"ok")
    handle = await cynapsa.login_async(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth("user", "pass"),
            cookies={"session": "abc"},
            json_serialize=lambda _value: '{"custom":true}',
        ) as client:
            await client.post(
                "http://agent.example/items?base=1",
                params=[("q", "one"), ("q", "two")],
                json={"ignored": True},
                skip_auto_headers={"User-Agent"},
            )
            for kwargs in (
                {"proxy": "http://proxy.example"},
                {"proxy_auth": aiohttp.BasicAuth("proxy", "secret")},
                {"proxy_headers": {"x-proxy": "value"}},
            ):
                with pytest.raises(TypeError, match="proxy options"):
                    await client.get("http://agent.example/proxy", **kwargs)
        command = next(
            command
            for command in bridge_factory.commands
            if command["command_name"] == "message.request"
        )
        request = command["args"]["payload"]["http_request"]
        assert request["query"] == "base=1&q=one&q=two"
        assert base64.b64decode(request["body"]) == b'{"custom":true}'
        headers = [(item["name"].lower(), item["value"]) for item in request["headers"]]
        assert ("content-type", "application/json") in headers
        assert any(name == "authorization" and value.startswith("Basic ") for name, value in headers)
        assert any(name == "cookie" and "session=abc" in value for name, value in headers)
        assert not any(name == "user-agent" for name, _value in headers)
        assert len(
            [
                item
                for item in bridge_factory.commands
                if item["command_name"] == "message.request"
            ]
        ) == 1
    finally:
        await handle.close()


@pytest.mark.asyncio
async def test_aiohttp_raise_for_status_callable_failure_releases_response(
    bridge_factory: BridgeDriver,
) -> None:
    bridge_factory.responses["message.request"] = _rpc_response(status=500)
    handle = await cynapsa.login_async(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    seen: list[aiohttp.ClientResponse] = []

    async def fail(response: aiohttp.ClientResponse) -> None:
        seen.append(response)
        raise RuntimeError("status callback")

    try:
        async with aiohttp.ClientSession() as client:
            with pytest.raises(RuntimeError, match="status callback"):
                await client.get(
                    "http://agent.example/fail", raise_for_status=fail
                )
        assert len(seen) == 1
        assert seen[0].closed and seen[0]._released
        assert seen[0]._session is None
    finally:
        await handle.close()


@pytest.mark.asyncio
async def test_aiohttp_msg_response_is_fixed_native_success(
    bridge_factory: BridgeDriver,
) -> None:
    handle = await cynapsa.login_async(
        **AUTH,
        address_map={
            "http://events.example": {
                "recipient": "events@example.test",
                "mode": "msg",
            }
        },
    )
    try:
        async with aiohttp.ClientSession() as client:
            response = await client.post("http://events.example/publish", data=b"event")
            assert isinstance(response, aiohttp.ClientResponse)
            assert (response.status, response.reason, response.raw_headers) == (200, "OK", ())
            assert list(response.headers.items()) == []
            assert await response.read() == b""
            assert response.content_length is None
        routed = [
            command
            for command in bridge_factory.commands
            if command["command_name"] in {"message.send", "message.request"}
        ]
        assert [command["command_name"] for command in routed] == ["message.send"]
    finally:
        await handle.close()


@pytest.mark.asyncio
async def test_aiohttp_raise_for_status_session_and_request_overrides(
    bridge_factory: BridgeDriver,
) -> None:
    bridge_factory.responses["message.request"] = _rpc_response(
        status=503,
        reason="Unavailable",
        body=b"later",
        headers=[],
    )
    handle = await cynapsa.login_async(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    callbacks: list[int] = []

    async def check(response: aiohttp.ClientResponse) -> None:
        callbacks.append(response.status)

    try:
        async with aiohttp.ClientSession(raise_for_status=True) as client:
            with pytest.raises(aiohttp.ClientResponseError) as raised:
                await client.get("http://agent.example/default")
            assert raised.value.status == 503
            response = await client.get(
                "http://agent.example/disabled", raise_for_status=False
            )
            assert response.status == 503
            checked = await client.get(
                "http://agent.example/callback", raise_for_status=check
            )
            assert checked.status == 503
        assert callbacks == [503]
    finally:
        await handle.close()


@pytest.mark.asyncio
async def test_aiohttp_timeout_variants_map_to_core_and_asyncio_timeout(
    bridge_factory: BridgeDriver,
) -> None:
    bridge_factory.failures["message.request"] = "rpc_timeout"
    handle = await cynapsa.login_async(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=0.041)
        ) as client:
            with pytest.raises(asyncio.TimeoutError):
                await client.get("http://agent.example/session-timeout")
            with pytest.raises(asyncio.TimeoutError):
                await client.get("http://agent.example/float-timeout", timeout=0.052)
            with pytest.raises(asyncio.TimeoutError):
                await client.get(
                    "http://agent.example/object-timeout",
                    timeout=aiohttp.ClientTimeout(total=0.063),
                )
        requests_sent = [
            command
            for command in bridge_factory.commands
            if command["command_name"] == "message.request"
        ]
        assert [command["args"]["ttl_ms"] for command in requests_sent] == [41, 52, 63]
    finally:
        await handle.close()


@pytest.mark.asyncio
async def test_aiohttp_sdk_safety_timeout_is_asyncio_timeout(
    bridge_factory: BridgeDriver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session_module, "_RPC_COMPLETION_SAFETY_WINDOW_MS", 20)
    bridge_factory.stalled.add("message.request")
    handle = await cynapsa.login_async(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        async with aiohttp.ClientSession() as client:
            with pytest.raises(asyncio.TimeoutError) as raised:
                await client.get("http://agent.example/safety", timeout=0.01)
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None
        command = next(
            command
            for command in bridge_factory.commands
            if command["command_name"] == "message.request"
        )
        assert command["args"]["ttl_ms"] == 10
    finally:
        await handle.close()


@pytest.mark.asyncio
async def test_aiohttp_cancellation_remains_cancelled_and_cancels_core(
    bridge_factory: BridgeDriver,
) -> None:
    bridge_factory.stalled.add("message.request")
    handle = await cynapsa.login_async(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        async with aiohttp.ClientSession() as client:
            async def use_context() -> None:
                async with client.get("http://agent.example/cancel"):
                    pytest.fail("cancelled request entered its response context")

            task = asyncio.create_task(use_context())
            deadline = time.monotonic() + 1
            while not any(
                item["command_name"] == "message.request"
                for item in bridge_factory.commands
            ) and time.monotonic() < deadline:
                await asyncio.sleep(0.001)
            command = next(
                item
                for item in bridge_factory.commands
                if item["command_name"] == "message.request"
            )
            task.cancel()
            deadline = time.monotonic() + 1
            while (
                not bridge_factory.library.cancel_inputs
                and time.monotonic() < deadline
            ):
                await asyncio.sleep(0.001)
            bridge_factory.library.emit_callback(
                CYNAPSA_CALLBACK_V1_COMPLETION,
                _failure(command, "request_cancelled"),
            )
            with pytest.raises(asyncio.CancelledError):
                await task
        assert len(bridge_factory.library.cancel_inputs) == 1
    finally:
        await handle.close()


@pytest.mark.asyncio
async def test_aiohttp_rejects_conflicting_and_streaming_bodies(
    bridge_factory: BridgeDriver,
) -> None:
    handle = await cynapsa.login_async(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )

    async def chunks() -> Any:
        yield b"chunk"

    try:
        async with aiohttp.ClientSession() as client:
            with pytest.raises(ValueError):
                await client.post(
                    "http://agent.example/conflict", data=b"x", json={"x": 1}
                )
            for kwargs in (
                {"data": io.BytesIO(b"stream")},
                {"data": {"form": "value"}},
                {"data": chunks()},
                {"data": b"x", "chunked": True},
                {"data": b"x", "compress": "gzip"},
            ):
                with pytest.raises(TypeError):
                    await client.post("http://agent.example/unsupported", **kwargs)
        assert not any(
            command["command_name"] == "message.request"
            for command in bridge_factory.commands
        )
    finally:
        await handle.close()


@pytest.mark.asyncio
async def test_aiohttp_owned_invalid_urls_fail_without_network_delegation(
    bridge_factory: BridgeDriver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delegated: list[str] = []
    original = aiohttp.ClientSession._request

    async def delegate(client: Any, method: str, url: Any, **kwargs: Any) -> Any:
        delegated.append(str(url))
        return await original(client, method, url, **kwargs)

    monkeypatch.setattr(aiohttp.ClientSession, "_request", delegate)
    handle = await cynapsa.login_async(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        async with aiohttp.ClientSession() as client:
            for url in (
                "http://user@agent.example/private",
                "http://agent.example/path#fragment",
            ):
                with pytest.raises(ValueError, match="url is invalid"):
                    await client.get(url)
        assert delegated == []
        assert not any(
            command["command_name"] == "message.request"
            for command in bridge_factory.commands
        )
    finally:
        await handle.close()
    monkeypatch.setattr(aiohttp.ClientSession, "_request", original)


@pytest.mark.asyncio
async def test_aiohttp_unmapped_delegates_exactly_and_concurrently(
    bridge_factory: BridgeDriver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bridge_factory.responses["message.request"] = _rpc_response(body=b"mapped")
    original = aiohttp.ClientSession._request
    marker = object()
    delegated: list[tuple[Any, ...]] = []

    async def delegate(client: Any, method: str, url: Any, **kwargs: Any) -> Any:
        delegated.append((client, method, url, kwargs))
        await asyncio.sleep(0)
        return marker

    monkeypatch.setattr(aiohttp.ClientSession, "_request", delegate)
    handle = await cynapsa.login_async(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        async with aiohttp.ClientSession() as client:
            mapped, unmapped = await asyncio.gather(
                client.get("http://agent.example/mapped"),
                client.request(
                    "CUSTOM",
                    "http://unmapped.example/path",
                    params=[("a", "1")],
                    headers=[("x", "one"), ("x", "two")],
                    timeout=0.75,
                ),
            )
        assert isinstance(mapped, aiohttp.ClientResponse)
        assert await mapped.read() == b"mapped"
        assert unmapped is marker
        assert len(delegated) == 1
        assert delegated[0][1:] == (
            "CUSTOM",
            "http://unmapped.example/path",
            {
                "params": [("a", "1")],
                "headers": [("x", "one"), ("x", "two")],
                "timeout": 0.75,
            },
        )
    finally:
        await handle.close()
    assert aiohttp.ClientSession._request is delegate
    monkeypatch.setattr(aiohttp.ClientSession, "_request", original)


def test_aiohttp_patch_is_nested_safe_and_restores_exact_original() -> None:
    manager = bridge_module._PatchManager()
    original = aiohttp.ClientSession._request

    class Owner:
        def __init__(self, *origins: str) -> None:
            self.origin_keys = origins
            self._registered = False

    first = Owner("http://one.example")
    second = Owner("http://two.example")
    manager.register(first)  # type: ignore[arg-type]
    installed = aiohttp.ClientSession._request
    manager.register(second)  # type: ignore[arg-type]
    try:
        assert installed is not original
        assert aiohttp.ClientSession._request is installed
        manager.unregister(first)  # type: ignore[arg-type]
        assert aiohttp.ClientSession._request is installed
    finally:
        manager.unregister(first)  # type: ignore[arg-type]
        manager.unregister(second)  # type: ignore[arg-type]
    assert aiohttp.ClientSession._request is original


def test_aiohttp_patch_installation_rolls_back_on_later_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = bridge_module._PatchManager()
    original = aiohttp.ClientSession._request
    real_import = importlib.import_module

    def failing_import(name: str) -> Any:
        if name == "urllib3":
            raise RuntimeError("broken urllib3 installation")
        return real_import(name)

    monkeypatch.setattr(importlib, "import_module", failing_import)
    with pytest.raises(RuntimeError, match="broken urllib3 installation"):
        manager._install()
    assert aiohttp.ClientSession._request is original


@pytest.mark.asyncio
async def test_httpx_async_is_nonblocking_and_returns_real_response(
    bridge_factory: BridgeDriver,
) -> None:
    bridge_factory.responses["message.request"] = _rpc_response(
        status=202, reason="Accepted", body=b"async"
    )
    handle = await cynapsa.login_async(
        **AUTH,
        address_map={
            "https://Agent.Example:443": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        for _ in range(10):
            ticks += 1
            await asyncio.sleep(0)

    try:
        async with httpx.AsyncClient() as client:
            response, _ = await asyncio.gather(
                client.post("https://agent.example/async", content=b"body"), ticker()
            )
        assert isinstance(response, httpx.Response)
        assert (response.status_code, response.reason_phrase, response.content) == (
            202,
            "Accepted",
            b"async",
        )
        assert ticks == 10
    finally:
        await handle.close()


@pytest.mark.asyncio
async def test_async_bridge_mixes_rpc_and_msg_per_origin(
    bridge_factory: BridgeDriver,
) -> None:
    bridge_factory.responses["message.request"] = _rpc_response(
        status=206, reason="Partial Content", body=b"async-remote"
    )
    handle = await cynapsa.login_async(
        **AUTH,
        address_map={
            "https://rpc.example": {
                "recipient": "rpc-peer@example.test",
                "mode": "rpc",
            },
            "https://msg.example": {
                "recipient": "msg-peer@example.test",
                "mode": "msg",
            },
        },
    )
    try:
        async with httpx.AsyncClient() as client:
            rpc_response = await client.get("https://rpc.example/async")
            msg_response = await client.get("https://msg.example/async")
        assert isinstance(rpc_response, httpx.Response)
        assert isinstance(msg_response, httpx.Response)
        assert (rpc_response.status_code, rpc_response.content) == (
            206,
            b"async-remote",
        )
        assert (msg_response.status_code, msg_response.content) == (200, b"")
        assert await handle.mappings() == (
            cynapsa.AddressMapping(
                "https://rpc.example", "rpc-peer@example.test", "rpc"
            ),
            cynapsa.AddressMapping(
                "https://msg.example", "msg-peer@example.test", "msg"
            ),
        )
        routed = [
            command
            for command in bridge_factory.commands
            if command["command_name"] in {"message.request", "message.send"}
        ]
        assert [command["command_name"] for command in routed] == [
            "message.request",
            "message.send",
        ]
        assert [command["args"]["to"] for command in routed] == [
            "rpc-peer@example.test",
            "msg-peer@example.test",
        ]
    finally:
        await handle.close()


@pytest.mark.asyncio
async def test_async_http_cancellation_uses_the_native_command_handle(
    bridge_factory: BridgeDriver,
) -> None:
    bridge_factory.stalled.add("message.request")
    handle = await cynapsa.login_async(
        **AUTH,
        address_map={
            "https://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        client = httpx.AsyncClient()
        task = asyncio.create_task(client.get("https://agent.example/cancel"))
        deadline = time.monotonic() + 1
        while not any(
            item["command_name"] == "message.request" for item in bridge_factory.commands
        ) and time.monotonic() < deadline:
            await asyncio.sleep(0.001)
        command = next(
            item for item in bridge_factory.commands if item["command_name"] == "message.request"
        )
        task.cancel()

        async def complete_cancel() -> None:
            deadline = time.monotonic() + 1
            while not bridge_factory.library.cancel_inputs and time.monotonic() < deadline:
                await asyncio.sleep(0.001)
            bridge_factory.library.emit_callback(
                CYNAPSA_CALLBACK_V1_COMPLETION,
                _failure(command, "request_cancelled"),
            )

        await complete_cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(bridge_factory.library.cancel_inputs) == 1
        cancellation = json.loads(bridge_factory.library.cancel_inputs[0])
        assert cancellation == {
            "abi_version": 1,
            "handle": "cmdh_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        }
        await client.aclose()
    finally:
        await handle.close()


@pytest.mark.asyncio
async def test_async_msg_cancellation_never_returns_synthetic_success(
    bridge_factory: BridgeDriver,
) -> None:
    bridge_factory.stalled.add("message.send")
    handle = await cynapsa.login_async(
        **AUTH,
        address_map={
            "https://agent.example": {
                "recipient": "peer@example.test",
                "mode": "msg",
            }
        },
    )
    try:
        client = httpx.AsyncClient()
        task = asyncio.create_task(client.get("https://agent.example/cancel"))
        deadline = time.monotonic() + 1
        while not any(
            item["command_name"] == "message.send"
            for item in bridge_factory.commands
        ) and time.monotonic() < deadline:
            await asyncio.sleep(0.001)
        command = next(
            item
            for item in bridge_factory.commands
            if item["command_name"] == "message.send"
        )
        task.cancel()

        deadline = time.monotonic() + 1
        while not bridge_factory.library.cancel_inputs and time.monotonic() < deadline:
            await asyncio.sleep(0.001)
        bridge_factory.library.emit_callback(
            CYNAPSA_CALLBACK_V1_COMPLETION,
            _failure(command, "request_cancelled"),
        )

        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(bridge_factory.library.cancel_inputs) == 1
        await client.aclose()
    finally:
        await handle.close()


@pytest.mark.asyncio
async def test_async_mapping_cancellation_uses_the_native_command_handle(
    bridge_factory: BridgeDriver,
) -> None:
    handle = await cynapsa.login_async(
        **AUTH,
        address_map={
            "https://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    bridge_factory.stalled.add("address.map.list")
    try:
        task = asyncio.create_task(handle.mappings())
        deadline = time.monotonic() + 1
        while not any(
            item["command_name"] == "address.map.list"
            for item in bridge_factory.commands
        ) and time.monotonic() < deadline:
            await asyncio.sleep(0.001)
        command = next(
            item
            for item in bridge_factory.commands
            if item["command_name"] == "address.map.list"
        )
        task.cancel()

        deadline = time.monotonic() + 1
        while not bridge_factory.library.cancel_inputs and time.monotonic() < deadline:
            await asyncio.sleep(0.001)
        bridge_factory.library.emit_callback(
            CYNAPSA_CALLBACK_V1_COMPLETION,
            _failure(command, "request_cancelled"),
        )

        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(bridge_factory.library.cancel_inputs) == 1
    finally:
        await handle.close()


def test_msg_response_is_created_only_after_send_completion(
    bridge_factory: BridgeDriver,
) -> None:
    bridge_factory.stalled.add("message.send")
    handle = cynapsa.login(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "msg",
            }
        },
    )
    outcome: list[requests.Response] = []
    failure: list[BaseException] = []

    def invoke() -> None:
        try:
            outcome.append(requests.post("http://agent.example/msg", data=b"body"))
        except BaseException as exc:
            failure.append(exc)

    thread = threading.Thread(target=invoke)
    thread.start()
    deadline = time.monotonic() + 1
    while not any(
        command["command_name"] == "message.send" for command in bridge_factory.commands
    ) and time.monotonic() < deadline:
        time.sleep(0.001)
    assert thread.is_alive()
    assert outcome == []
    command = next(
        command for command in bridge_factory.commands if command["command_name"] == "message.send"
    )
    bridge_factory.library.emit_callback(
        CYNAPSA_CALLBACK_V1_COMPLETION,
        _success(
            command,
            "send",
            {"message_id": "m", "conversation_id": "c", "accepted": True},
        ),
    )
    thread.join(1)
    try:
        assert failure == []
        assert len(outcome) == 1
        assert (outcome[0].status_code, outcome[0].reason, outcome[0].content) == (
            200,
            "OK",
            b"",
        )
        assert dict(outcome[0].headers) == {}
    finally:
        handle.close()


def test_msg_rejection_never_returns_synthetic_success(
    bridge_factory: BridgeDriver,
) -> None:
    bridge_factory.failures["message.send"] = "peer_unreachable"
    handle = cynapsa.login(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "msg",
            }
        },
    )
    try:
        with pytest.raises(NativeError) as raised:
            requests.get("http://agent.example/rejected")
        assert raised.value.code == "peer_unreachable"
    finally:
        handle.close()


def test_http_bridge_rpc_rejects_non_http_response_variant(
    bridge_factory: BridgeDriver,
) -> None:
    bridge_factory.responses["message.request"] = (
        "response",
        {
            "message_id": "response-message",
            "conversation_id": "conversation",
            "from_agent_id": "peer@example.test",
            "mesh_id": "mesh-one",
            "payload": {
                "native": {
                    "content_type": "application/json",
                    "path": "/",
                    "body": base64.b64encode(b'{"wrong":true}').decode(),
                }
            },
        },
    )
    handle = cynapsa.login(
        **AUTH,
        address_map={
            "http://agent.example": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        with pytest.raises(NativeError) as raised:
            requests.get("http://agent.example/mismatch")
        assert raised.value.code == "core_error"
    finally:
        handle.close()


def test_transactional_mapping_failure_and_origin_conflict_restore_hooks(
    bridge_factory: BridgeDriver,
) -> None:
    original_requests = requests.sessions.Session.send
    original_urllib_request = urllib.request.urlopen
    bridge_factory.failures["address.map.put"] = "command_error"
    with pytest.raises(NativeError) as raised:
        cynapsa.login(
            **AUTH,
            address_map={
                "http://agent.example": {
                    "recipient": "peer@example.test",
                    "mode": "rpc",
                }
            },
        )
    assert raised.value.code == "command_error"
    assert requests.sessions.Session.send is original_requests
    assert urllib.request.urlopen is original_urllib_request
    assert "auth.logout" in [item["command_name"] for item in bridge_factory.commands]

    bridge_factory.failures.clear()
    first = cynapsa.login(
        **AUTH,
        address_map={
            "HTTP://Agent.Example:80": {
                "recipient": "peer@example.test",
                "mode": "rpc",
            }
        },
    )
    try:
        before = len(bridge_factory.commands)
        with pytest.raises(ValueError):
            cynapsa.login(
                **AUTH,
                address_map={
                    "http://agent.example": {
                        "recipient": "other@example.test",
                        "mode": "rpc",
                    }
                },
            )
        assert len(bridge_factory.commands) == before
    finally:
        first.close()
        first.close()
    assert requests.sessions.Session.send is original_requests
    assert urllib.request.urlopen is original_urllib_request


def test_http_mode_is_absent_from_public_login_signatures() -> None:
    for function in (cynapsa.login, cynapsa.login_async):
        parameters = inspect.signature(function).parameters
        assert "http_mode" not in parameters
        assert parameters["mesh_endpoint"].default is None
        assert parameters["username"].default is None
        assert parameters["password"].default is None
        assert parameters["enrollment_token"].default is None
        assert parameters["profile_id"].default == "default"


def _bridge_authentication_cases() -> tuple[
    tuple[dict[str, object], str, dict[str, object]], ...
]:
    return (
        (
            {
                "mesh_id": "mesh-one",
                "mesh_endpoint": "Mesh.Example.TEST:05222",
                "username": "input-agent@example.test",
                "password": "legacy-bridge-secret",
            },
            "auth.login",
            {
                "mesh_endpoint": "mesh.example.test:5222",
                "username": "input-agent@example.test",
                "password": "legacy-bridge-secret",
                "mesh_id": "mesh-one",
            },
        ),
        (
            {
                "mesh_id": "mesh-one",
                "enrollment_token": ENROLLMENT_TOKEN,
            },
            "auth.token_login",
            {
                "token": ENROLLMENT_TOKEN,
                "mesh_id": "mesh-one",
                "profile_id": "default",
            },
        ),
        (
            {"mesh_id": "mesh-one", "profile_id": "bridge-profile"},
            "auth.installation_login",
            {"mesh_id": "mesh-one", "profile_id": "bridge-profile"},
        ),
    )


def _assert_bridge_authentication_command(
    driver: BridgeDriver,
    expected_name: str,
    expected_args: dict[str, object],
) -> None:
    authentication = [
        command
        for command in driver.commands
        if command["command_name"].startswith("auth.")
        and command["command_name"] != "auth.logout"
    ]
    assert len(authentication) == 1
    command = authentication[0]
    assert command["command_name"] == expected_name
    actual_args = dict(command["args"])
    if expected_name == "auth.login":
        assert actual_args.pop("agent_instance_id").startswith("agentinst_")
    assert actual_args == expected_args


@pytest.mark.parametrize(
    ("authentication", "expected_name", "expected_args"),
    _bridge_authentication_cases(),
)
def test_sync_bridge_selects_all_authentication_modes(
    bridge_factory: BridgeDriver,
    authentication: dict[str, object],
    expected_name: str,
    expected_args: dict[str, object],
) -> None:
    handle = cynapsa.login(**authentication, address_map={})
    try:
        _assert_bridge_authentication_command(
            bridge_factory, expected_name, expected_args
        )
        if expected_name == "auth.login":
            assert handle.profile_id is None
            assert handle.installation_id is None
        else:
            assert handle.profile_id == expected_args["profile_id"]
            assert handle.installation_id == "11234567-89ab-4def-8123-456789abcdef"
            assert handle.auth_info.policy_revision == 7
    finally:
        handle.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("authentication", "expected_name", "expected_args"),
    _bridge_authentication_cases(),
)
async def test_async_bridge_selects_all_authentication_modes(
    bridge_factory: BridgeDriver,
    authentication: dict[str, object],
    expected_name: str,
    expected_args: dict[str, object],
) -> None:
    handle = await cynapsa.login_async(**authentication, address_map={})
    try:
        _assert_bridge_authentication_command(
            bridge_factory, expected_name, expected_args
        )
        if expected_name == "auth.login":
            assert handle.profile_id is None
            assert handle.installation_id is None
        else:
            assert handle.profile_id == expected_args["profile_id"]
            assert handle.installation_id == "11234567-89ab-4def-8123-456789abcdef"
            assert handle.auth_info.policy_revision == 7
    finally:
        await handle.close()


@pytest.mark.parametrize(
    "authentication",
    (
        {"mesh_id": "mesh-one", "username": "agent@example.test"},
        {
            "mesh_id": "mesh-one",
            "mesh_endpoint": "mesh.example.test:5222",
            "username": "agent@example.test",
        },
        {
            "mesh_id": "mesh-one",
            "mesh_endpoint": "mesh.example.test:5222",
            "username": "agent@example.test",
            "password": "legacy-secret",
            "enrollment_token": ENROLLMENT_TOKEN,
        },
        {
            "mesh_id": "mesh-one",
            "mesh_endpoint": "mesh.example.test:5222",
            "username": "agent@example.test",
            "password": "legacy-secret",
            "profile_id": "legacy-cannot-use-profiles",
        },
    ),
)
def test_bridge_rejects_partial_or_mixed_authentication_before_core_creation(
    bridge_factory: BridgeDriver, authentication: dict[str, object]
) -> None:
    with pytest.raises(ValueError) as raised:
        cynapsa.login(**authentication, address_map={})
    assert "legacy-secret" not in str(raised.value)
    assert ENROLLMENT_TOKEN not in str(raised.value)
    for secret in ("legacy-secret", ENROLLMENT_TOKEN):
        if secret in authentication.values():
            _assert_secret_absent_from_sdk_traceback(raised.value, secret)
    assert bridge_factory.commands == []
    assert not bridge_module._PATCH_MANAGER._bridges


def _assert_secret_absent_from_sdk_traceback(
    error: BaseException, secret: str
) -> None:
    assert secret not in str(error)
    assert secret not in repr(error)
    traceback = error.__traceback__
    while traceback is not None:
        if "/src/cynapsa/" in traceback.tb_frame.f_code.co_filename:
            assert secret not in repr(dict(traceback.tb_frame.f_locals))
        traceback = traceback.tb_next


@pytest.mark.parametrize(
    ("authentication", "auth_name", "secret"),
    (
        (
            {
                "mesh_id": "mesh-one",
                "mesh_endpoint": "mesh.example.test:5222",
                "username": "agent@example.test",
                "password": "failed-legacy-secret",
            },
            "auth.login",
            "failed-legacy-secret",
        ),
        (
            {
                "mesh_id": "mesh-one",
                "enrollment_token": ENROLLMENT_TOKEN,
                "profile_id": "bridge-profile",
            },
            "auth.token_login",
            ENROLLMENT_TOKEN,
        ),
        (
            {"mesh_id": "mesh-one", "profile_id": "bridge-profile"},
            "auth.installation_login",
            "",
        ),
    ),
)
def test_sync_bridge_authentication_failure_installs_nothing_and_scrubs_secret(
    bridge_factory: BridgeDriver,
    authentication: dict[str, object],
    auth_name: str,
    secret: str,
) -> None:
    original = requests.sessions.Session.send
    bridge_factory.failures[auth_name] = "authentication_failed"
    with pytest.raises(NativeError) as raised:
        cynapsa.login(**authentication, address_map={})
    assert raised.value.code == "authentication_failed"
    if secret:
        _assert_secret_absent_from_sdk_traceback(raised.value, secret)
    assert requests.sessions.Session.send is original
    assert not bridge_module._PATCH_MANAGER._bridges
    assert not any(
        command["command_name"] == "address.map.put"
        for command in bridge_factory.commands
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("authentication", "auth_name", "secret"),
    (
        (
            {
                "mesh_id": "mesh-one",
                "mesh_endpoint": "mesh.example.test:5222",
                "username": "agent@example.test",
                "password": "failed-async-legacy-secret",
            },
            "auth.login",
            "failed-async-legacy-secret",
        ),
        (
            {
                "mesh_id": "mesh-one",
                "enrollment_token": ENROLLMENT_TOKEN,
                "profile_id": "bridge-profile",
            },
            "auth.token_login",
            ENROLLMENT_TOKEN,
        ),
        (
            {"mesh_id": "mesh-one", "profile_id": "bridge-profile"},
            "auth.installation_login",
            "",
        ),
    ),
)
async def test_async_bridge_authentication_failure_installs_nothing_and_scrubs_secret(
    bridge_factory: BridgeDriver,
    authentication: dict[str, object],
    auth_name: str,
    secret: str,
) -> None:
    original = requests.sessions.Session.send
    bridge_factory.failures[auth_name] = "authentication_failed"
    with pytest.raises(NativeError) as raised:
        await cynapsa.login_async(**authentication, address_map={})
    assert raised.value.code == "authentication_failed"
    if secret:
        _assert_secret_absent_from_sdk_traceback(raised.value, secret)
    assert requests.sessions.Session.send is original
    assert not bridge_module._PATCH_MANAGER._bridges
    assert not any(
        command["command_name"] == "address.map.put"
        for command in bridge_factory.commands
    )


@pytest.mark.asyncio
async def test_async_token_authentication_cancellation_rolls_back_before_hooks(
    bridge_factory: BridgeDriver,
) -> None:
    original = requests.sessions.Session.send
    bridge_factory.stalled.add("auth.token_login")
    task = asyncio.create_task(
        cynapsa.login_async(
            mesh_id="mesh-one",
            enrollment_token=ENROLLMENT_TOKEN,
            profile_id="bridge-profile",
            address_map={
                "http://agent.example": {
                    "recipient": "peer@example.test",
                    "mode": "rpc",
                }
            },
        )
    )
    deadline = time.monotonic() + 1
    while not any(
        command["command_name"] == "auth.token_login"
        for command in bridge_factory.commands
    ):
        assert time.monotonic() < deadline
        await asyncio.sleep(0.001)
    command = next(
        command
        for command in bridge_factory.commands
        if command["command_name"] == "auth.token_login"
    )
    task.cancel()
    deadline = time.monotonic() + 1
    while not bridge_factory.library.cancel_inputs:
        assert time.monotonic() < deadline
        await asyncio.sleep(0.001)
    bridge_factory.library.emit_callback(
        CYNAPSA_CALLBACK_V1_COMPLETION,
        _failure(command, "request_cancelled"),
    )
    with pytest.raises(asyncio.CancelledError):
        await task
    assert requests.sessions.Session.send is original
    assert not bridge_module._PATCH_MANAGER._bridges
    assert not any(
        submitted["command_name"] == "address.map.put"
        for submitted in bridge_factory.commands
    )
    assert bridge_factory.library.calls["core_destroy"] == 1


@pytest.mark.parametrize(
    "target",
    [
        {"recipient": "peer@example.test"},
        {"mode": "rpc"},
        {"recipient": "peer@example.test", "mode": "RPC"},
        {"recipient": "peer@example.test", "mode": ""},
        {"recipient": "peer@example.test", "mode": 1},
        {"recipient": "peer@example.test", "mode": "rpc", "extra": True},
    ],
)
def test_address_mapping_requires_exact_recipient_and_mode(
    bridge_factory: BridgeDriver, target: object
) -> None:
    with pytest.raises(ValueError):
        cynapsa.login(
            **AUTH,
            address_map={"http://agent.example": target},  # type: ignore[dict-item]
        )
    assert bridge_factory.commands == []


@pytest.mark.parametrize("recipient", ["a" * 257, "peer\n@example.test", "peer\x00id"])
def test_address_mapping_rejects_core_invalid_agent_ids_before_authentication(
    bridge_factory: BridgeDriver, recipient: str
) -> None:
    with pytest.raises(ValueError):
        cynapsa.login(
            **AUTH,
            address_map={
                "http://agent.example": {"recipient": recipient, "mode": "rpc"}
            },
        )
    assert bridge_factory.commands == []


def test_address_mapping_rejects_duplicate_normalized_origins(
    bridge_factory: BridgeDriver,
) -> None:
    with pytest.raises(ValueError, match="duplicate normalized origins"):
        cynapsa.login(
            **AUTH,
            address_map={
                "HTTP://Agent.Example:80": {
                    "recipient": "peer@example.test",
                    "mode": "rpc",
                },
                "http://agent.example": {
                    "recipient": "peer@example.test",
                    "mode": "rpc",
                },
            },
        )
    assert bridge_factory.commands == []


def test_mapping_list_fails_closed_when_core_and_local_routes_disagree(
    bridge_factory: BridgeDriver,
) -> None:
    returned: list[dict[str, str]] = []

    def respond(command: dict[str, Any]) -> bytes | None:
        if command["command_name"] == "address.map.list":
            return _success(command, "address_mappings", {"mappings": returned})
        return None

    bridge_factory.responder = respond
    handle = cynapsa.login(
        **AUTH,
        address_map={
            "HTTP://Agent.Example:80": {
                "recipient": "peer@example.test",
                "mode": "msg",
            }
        },
    )
    try:
        inconsistent_results = (
            [],
            [
                {
                    "virtual_origin": "http://unknown.example",
                    "recipient": "peer@example.test",
                }
            ],
            [
                {
                    "virtual_origin": "http://agent.example",
                    "recipient": "other@example.test",
                }
            ],
            [
                {
                    "virtual_origin": "http://agent.example",
                    "recipient": "peer@example.test",
                },
                {
                    "virtual_origin": "HTTP://Agent.Example:80",
                    "recipient": "peer@example.test",
                },
            ],
        )
        for result in inconsistent_results:
            returned[:] = result
            with pytest.raises(NativeError) as raised:
                handle.mappings()
            assert raised.value.code == "core_error"

        returned[:] = [
            {
                "virtual_origin": "http://agent.example",
                "recipient": "peer@example.test",
            }
        ]
        assert handle.mappings() == (
            cynapsa.AddressMapping(
                "HTTP://Agent.Example:80", "peer@example.test", "msg"
            ),
        )
    finally:
        handle.close()


@pytest.mark.asyncio
async def test_async_login_validation_scrubs_password_from_public_traceback() -> None:
    secret = "async-validation-secret-do-not-retain"
    with pytest.raises(ValueError) as raised:
        await cynapsa.login_async(
            **{**AUTH, "password": secret},
            address_map={
                "not-an-origin": {
                    "recipient": "peer@example.test",
                    "mode": "rpc",
                }
            },
        )
    assert secret not in str(raised.value)
    assert secret not in repr(raised.value)
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    traceback = raised.value.__traceback__
    while traceback is not None:
        if "/src/cynapsa/" in traceback.tb_frame.f_code.co_filename:
            assert secret not in repr(dict(traceback.tb_frame.f_locals))
        traceback = traceback.tb_next


def test_second_mapping_failure_rolls_back_first_and_auth_owner(
    bridge_factory: BridgeDriver,
) -> None:
    puts = 0

    def respond(command: dict[str, Any]) -> bytes | None:
        nonlocal puts
        if command["command_name"] == "address.map.put":
            puts += 1
            if puts == 2:
                return _failure(command, "command_error")
        return None

    bridge_factory.responder = respond
    with pytest.raises(NativeError):
        cynapsa.login(
            **AUTH,
            address_map={
                "http://one.example": {"recipient": "one@example.test", "mode": "rpc"},
                "http://two.example": {"recipient": "two@example.test", "mode": "rpc"},
            },
        )
    names = [command["command_name"] for command in bridge_factory.commands]
    assert names.count("address.map.put") == 2
    removed = next(
        command for command in bridge_factory.commands
        if command["command_name"] == "address.map.remove"
    )
    assert removed["args"] == {"virtual_origin": "http://one.example"}
    assert "auth.logout" in names
    assert not bridge_module._PATCH_MANAGER._bridges


def test_auth_personality_mismatch_installs_nothing(
    bridge_factory: BridgeDriver,
) -> None:
    bridge_factory.responses["auth.login"] = (
        "auth",
        {
            "agent_id": "bridge-agent@example.test",
            "mesh_id": "mesh-one",
            "agent_instance_id": "wrong-instance",
            "personality": "native",
        },
    )
    original = requests.sessions.Session.send
    with pytest.raises(NativeError) as raised:
        cynapsa.login(**AUTH, address_map={})
    assert raised.value.code == "authentication_failed"
    assert requests.sessions.Session.send is original
    assert not bridge_module._PATCH_MANAGER._bridges
    assert bridge_factory.library.calls["core_destroy"] == 1


def test_patch_manager_preserves_third_party_interference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = bridge_module._PatchManager()

    class Owner:
        origin_keys = ("http://patch-owner.example",)
        _registered = False

    owner = Owner()
    original = requests.sessions.Session.send
    manager.register(owner)  # type: ignore[arg-type]
    installed = requests.sessions.Session.send

    def foreign(session: Any, request: Any, **kwargs: Any) -> Any:
        return original(session, request, **kwargs)

    monkeypatch.setattr(requests.sessions.Session, "send", foreign)
    manager.unregister(owner)  # type: ignore[arg-type]
    assert installed is not original
    assert requests.sessions.Session.send is foreign


def test_patch_installation_tolerates_absent_optional_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = bridge_module._PatchManager()

    def absent(name: str) -> Any:
        error = ModuleNotFoundError(name)
        error.name = name
        raise error

    monkeypatch.setattr(importlib, "import_module", absent)
    manager._install()
    assert manager._originals == []


def _event(
    *, event_id: str, mode: str, path: str = "/orders", body: bytes = b"hello"
) -> bytes:
    return json.dumps(
        {
            "abi_version": 1,
            "event_id": event_id,
            "event_name": "message.received",
            "created_at": "2026-08-26T00:00:00Z",
            "payload": {
                "message_id": f"message-{event_id}",
                "conversation_id": "conversation",
                "from_agent_id": "peer@example.test",
                "mesh_id": "mesh-one",
                "mode": mode,
                "request_handle": (
                    "reqh_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                    if mode == "rpc"
                    else ""
                ),
                "payload": {
                    "http_request": {
                        "method": "POST",
                        "path": path,
                        "query": "x=1&x=2",
                        "headers": [
                            {"name": "x-a", "value": "one"},
                            {"name": "x-a", "value": "two"},
                        ],
                        "body": base64.b64encode(body).decode(),
                    }
                },
            },
        },
        separators=(",", ":"),
    ).encode()


@pytest.mark.asyncio
async def test_explicit_asgi_routing_accepts_before_app_and_rpc_only_replies(
    bridge_factory: BridgeDriver,
) -> None:
    app = FastAPI()
    invoked: list[tuple[list[str], list[str], bytes, bool]] = []

    @app.post("/orders")
    async def orders(request: Request) -> dict[str, Any]:
        accepted = any(
            item["command_name"] == "delivery.accept" for item in bridge_factory.commands
        )
        invoked.append(
            (
                request.query_params.getlist("x"),
                request.headers.getlist("x-a"),
                await request.body(),
                accepted,
            )
        )
        return {"ok": True}

    @app.post("/explode")
    async def explode() -> None:
        raise RuntimeError("secret exception text")

    handle = await cynapsa.login_async(**AUTH, address_map={}, asgi_app=app)
    try:
        bridge_factory.library.emit_callback(
            CYNAPSA_CALLBACK_V1_EVENT, _event(event_id="rpc", mode="rpc")
        )
        deadline = time.monotonic() + 2
        while len(
            [item for item in bridge_factory.commands if item["command_name"] == "message.reply"]
        ) < 1 and time.monotonic() < deadline:
            await asyncio.sleep(0.005)
        replies = [
            item for item in bridge_factory.commands if item["command_name"] == "message.reply"
        ]
        assert invoked == [(["1", "2"], ["one", "two"], b"hello", True)]
        assert replies[0]["args"]["payload"]["http_response"]["status_code"] == 200

        bridge_factory.library.emit_callback(
            CYNAPSA_CALLBACK_V1_EVENT,
            _event(event_id="error", mode="rpc", path="/explode"),
        )
        while len(
            [item for item in bridge_factory.commands if item["command_name"] == "message.reply"]
        ) < 2 and time.monotonic() < deadline:
            await asyncio.sleep(0.005)
        replies = [
            item for item in bridge_factory.commands if item["command_name"] == "message.reply"
        ]
        error = replies[1]["args"]["payload"]["http_response"]
        assert error["status_code"] == 500
        assert error["reason"] == "Internal Server Error"
        assert b"handler_error" in base64.b64decode(error["body"])
        assert b"secret exception text" not in base64.b64decode(error["body"])
        assert error["error"] == {
            "code": "handler_error",
            "detail": "The application handler failed",
            "details_json": "{}",
        }
        assert "secret" not in json.dumps(error)

        before = len(replies)
        bridge_factory.library.emit_callback(
            CYNAPSA_CALLBACK_V1_EVENT, _event(event_id="msg", mode="msg")
        )
        deadline = time.monotonic() + 1
        while len(invoked) < 2 and time.monotonic() < deadline:
            await asyncio.sleep(0.005)
        assert len(
            [item for item in bridge_factory.commands if item["command_name"] == "message.reply"]
        ) == before
    finally:
        await handle.close()


@pytest.mark.asyncio
async def test_asgi_scope_receive_chunks_and_protocol_violations_fail_closed(
    bridge_factory: BridgeDriver,
) -> None:
    observed: list[tuple[dict[str, Any], list[dict[str, Any]]]] = []

    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        received = [await receive(), await receive()]
        observed.append((scope, received))
        if scope["path"] == "/broken":
            await send({"type": "http.response.start", "status": 200})
            await send({"type": "http.response.body", "body": b"partial", "more_body": True})
            return
        await send(
            {
                "type": "http.response.start",
                "status": 206,
                "headers": [(b"x-a", b"one"), (b"x-a", b"two")],
            }
        )
        await send({"type": "http.response.body", "body": b"a", "more_body": True})
        await send({"type": "http.response.body", "body": b"b"})

    handle = await cynapsa.login_async(**AUTH, address_map={}, asgi_app=app)
    runtime = handle._bridge._asgi
    assert runtime is not None
    try:
        bridge_factory.library.emit_callback(
            CYNAPSA_CALLBACK_V1_EVENT, _event(event_id="scope", mode="rpc")
        )
        bridge_factory.library.emit_callback(
            CYNAPSA_CALLBACK_V1_EVENT,
            _event(event_id="broken", mode="rpc", path="/broken"),
        )
        deadline = time.monotonic() + 2
        while len(
            [c for c in bridge_factory.commands if c["command_name"] == "message.reply"]
        ) < 2 and time.monotonic() < deadline:
            await asyncio.sleep(0.005)
        replies = [
            c for c in bridge_factory.commands if c["command_name"] == "message.reply"
        ]
        assert replies[0]["args"]["payload"]["http_response"] == {
            "status_code": 206,
            "reason": "Partial Content",
            "headers": [
                {"name": "x-a", "value": "one"},
                {"name": "x-a", "value": "two"},
            ],
            "body": "YWI=",
        }
        failed = replies[1]["args"]["payload"]["http_response"]
        assert failed["status_code"] == 500
        assert failed["reason"] == "Internal Server Error"
        assert b"handler_error" in base64.b64decode(failed["body"])
        scope, received = observed[0]
        assert scope["scheme"] == "http"
        assert scope["server"] is None and scope["root_path"] == ""
        assert scope["path"] == "/orders" and scope["raw_path"] == b"/orders"
        assert scope["query_string"] == b"x=1&x=2"
        assert scope["headers"] == [(b"x-a", b"one"), (b"x-a", b"two")]
        assert received == [
            {"type": "http.request", "body": b"hello", "more_body": False},
            {"type": "http.disconnect"},
        ]
    finally:
        await handle.close()
    assert not runtime.consumer.is_alive()
    assert not any(worker.is_alive() for worker in runtime.workers)
