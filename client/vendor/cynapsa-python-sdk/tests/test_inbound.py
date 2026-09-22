from __future__ import annotations

import asyncio
import base64
import json
import queue
import threading
import time
from typing import Any

import pytest

import cynapsa
from cynapsa import session as session_module
from cynapsa.native.abi import (
    CYNAPSA_CALLBACK_V1_DIAGNOSTIC,
    CYNAPSA_CALLBACK_V1_EVENT,
)
from cynapsa.native.core import NativeCore

from conftest import FakeLibrary
from test_messaging import SEND_RESULT, VALID_REQUEST_HANDLE
from test_session import AUTH, CompletionDriver, _failure


def _event(
    event_id: str,
    *,
    mode: str = "rpc",
    path: str = "/route",
    request_handle: str | None = None,
    body: bytes = b'{"value":1}',
) -> bytes:
    handle = VALID_REQUEST_HANDLE if request_handle is None and mode == "rpc" else request_handle
    return json.dumps(
        {
            "abi_version": 1,
            "event_id": event_id,
            "event_name": "message.received",
            "created_at": "2026-08-26T12:00:00.123456789Z",
            "payload": {
                "message_id": "message-" + event_id,
                "conversation_id": "conversation-" + event_id,
                "from_agent_id": "agent-b@example.test",
                "mesh_id": "mesh-one",
                "mode": mode,
                "request_handle": "" if handle is None else handle,
                "payload": {
                    "native": {
                        "content_type": "application/json",
                        "path": path,
                        "body": base64.b64encode(body).decode(),
                    }
                },
            },
        },
        separators=(",", ":"),
    ).encode()


def _http_event(
    event_id: str,
    *,
    mode: str = "rpc",
    path: str = "/http",
    request_handle: str | None = None,
    body: bytes = b'{"value":1}',
    headers: list[dict[str, str]] | None = None,
) -> bytes:
    handle = VALID_REQUEST_HANDLE if request_handle is None and mode == "rpc" else request_handle
    return json.dumps(
        {
            "abi_version": 1,
            "event_id": event_id,
            "event_name": "message.received",
            "created_at": "2026-08-26T12:00:00.123456789Z",
            "payload": {
                "message_id": "message-" + event_id,
                "conversation_id": "conversation-" + event_id,
                "from_agent_id": "agent-b@example.test",
                "mesh_id": "mesh-one",
                "mode": mode,
                "request_handle": "" if handle is None else handle,
                "payload": {
                    "http_request": {
                        "method": "POST",
                        "path": path,
                        "query": "x=1&x=2",
                        "headers": headers
                        if headers is not None
                        else [
                            {"name": "content-type", "value": "application/json"},
                            {"name": "x-dup", "value": "one"},
                            {"name": "x-dup", "value": "two"},
                        ],
                        "body": base64.b64encode(body).decode(),
                    }
                },
            },
        },
        separators=(",", ":"),
    ).encode()


def _wait(predicate: Any, timeout: float = 2) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline
        time.sleep(0.001)


@pytest.fixture
def native_factory(monkeypatch: pytest.MonkeyPatch, fake_library: FakeLibrary) -> None:
    monkeypatch.setattr(
        session_module,
        "_default_core_factory",
        lambda config: NativeCore.create(config.core_create(), library=fake_library),
    )


def _connect(
    fake_library: FakeLibrary,
    *,
    auth: dict[str, Any] = AUTH,
    overrides: dict[str, Any] | None = None,
) -> tuple[cynapsa.AztmSession, CompletionDriver]:
    driver = CompletionDriver(fake_library)
    if overrides:
        driver.overrides.update(overrides)
    driver.start()
    return cynapsa.connect(**auth), driver


def test_one_handler_serves_rpc_and_msg_and_unregisters_by_path(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    session, driver = _connect(
        fake_library, overrides={"message.reply": ("send", SEND_RESULT)}
    )
    seen: list[str] = []

    @session.on("*")
    def rpc_wildcard(req: cynapsa.AztmRequest) -> None:
        seen.append("rpc-wildcard")

    def exact(req: cynapsa.AztmRequest) -> None:
        seen.append("exact")

    session.on("/route", exact)
    session.on("/both", lambda req: seen.append("both"))
    with pytest.raises(ValueError):
        session.on("/route", exact)
    with pytest.raises(TypeError):
        session.on("/invalid", exact, mode="invalid")
    try:
        fake_library.emit_callback(CYNAPSA_CALLBACK_V1_EVENT, _event("one"))
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_EVENT,
            _event("two", mode="msg", request_handle=None),
        )
        fake_library.emit_callback(CYNAPSA_CALLBACK_V1_EVENT, _event("three", path="/other"))
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_EVENT, _event("four", path="/both")
        )
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_EVENT,
            _event("five", mode="msg", path="/both", request_handle=None),
        )
        _wait(lambda: len(seen) == 5)
        assert seen.count("exact") == 2
        assert seen.count("both") == 2
        assert seen.count("rpc-wildcard") == 1
        assert session.off("/route", exact) is True
        assert session.off("/route", exact) is False
        assert not any(
            item["command_name"].startswith("handler.") for item in driver.commands
        )
    finally:
        session.close()
        driver.stop()


def test_accept_is_after_enqueue_before_handler_exactly_once_and_failure_skips_dispatch(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    session, driver = _connect(
        fake_library, overrides={"message.reply": ("send", SEND_RESULT)}
    )
    observations: list[tuple[bool, str]] = []

    def handler(req: cynapsa.AztmRequest) -> None:
        observations.append(
            (
                any(
                    item["command_name"] == "delivery.accept"
                    and item["args"]["event_id"] == "accepted"
                    for item in driver.commands
                ),
                threading.current_thread().name,
            )
        )

    session.on("/route", handler)
    try:
        fake_library.emit_callback(CYNAPSA_CALLBACK_V1_EVENT, _event("accepted"))
        fake_library.emit_callback(CYNAPSA_CALLBACK_V1_EVENT, _event("accepted"))
        _wait(lambda: observations)
        assert observations[0][0] is True
        assert "callback" not in observations[0][1]
        assert len(
            [
                item
                for item in driver.commands
                if item["command_name"] == "delivery.accept"
                and item["args"]["event_id"] == "accepted"
            ]
        ) == 1

        driver.overrides["delivery.accept"] = _failure(
            {"command_id": "$COMMAND_ID"}, "queue_full"
        )
        fake_library.emit_callback(CYNAPSA_CALLBACK_V1_EVENT, _event("failed"))
        diagnostic = session.next_local_diagnostic(2)
        while diagnostic.code == "duplicate_event":
            diagnostic = session.next_local_diagnostic(2)
        assert diagnostic.code == "delivery_accept_failed"
        fake_library.emit_callback(CYNAPSA_CALLBACK_V1_EVENT, _event("failed"))
        duplicate = session.next_local_diagnostic(2)
        assert duplicate.code == "duplicate_event"
        assert len(
            [
                item
                for item in driver.commands
                if item["command_name"] == "delivery.accept"
                and item["args"]["event_id"] == "failed"
            ]
        ) == 1
        time.sleep(0.02)
        assert len(observations) == 1
    finally:
        session.close()
        driver.stop()


def test_rpc_return_none_msg_discard_and_sanitized_exceptions(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    session, driver = _connect(
        fake_library, overrides={"message.reply": ("send", SEND_RESULT)}
    )

    session.on("/none", lambda req: None)
    session.on("/auto", lambda req: {"automatic": True})
    session.on("/msg", lambda req: {"ignored": True})

    def intentional(req: cynapsa.CynapsaRequest) -> None:
        raise cynapsa.RPCException(404, code="not_found", detail="safe detail")

    session.on("/intentional", intentional)

    def failure(req: cynapsa.AztmRequest) -> None:
        raise RuntimeError("secret implementation detail")

    session.on("/failure", failure)
    try:
        for index, path in enumerate(
            ("/none", "/auto", "/msg", "/failure", "/intentional")
        ):
            mode = "msg" if path == "/msg" else "rpc"
            fake_library.emit_callback(
                CYNAPSA_CALLBACK_V1_EVENT,
                _event(str(index), mode=mode, path=path, request_handle=None),
            )
        _wait(
            lambda: len(
                [item for item in driver.commands if item["command_name"] == "delivery.accept"]
            )
            == 5
        )
        _wait(
            lambda: len(
                [item for item in driver.commands if item["command_name"] == "message.reply"]
            )
            == 4
        )
        replies = [
            item for item in driver.commands if item["command_name"] == "message.reply"
        ]
        payloads = [item["args"]["payload"]["http_response"] for item in replies]
        bodies = [base64.b64decode(item["body"]) for item in payloads]
        assert b'{"automatic":true}' in bodies
        assert b"null" in bodies
        assert any(b"handler_error" in body for body in bodies)
        assert any(b"not_found" in body and b"safe detail" in body for body in bodies)
        assert not any(b"secret implementation detail" in body for body in bodies)
        assert session.next_local_diagnostic(2).code == "handler_error"
    finally:
        session.close()
        driver.stop()


def test_native_inbound_route_receives_http_request_and_replies_http_response(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    session, driver = _connect(
        fake_library, overrides={"message.reply": ("send", SEND_RESULT)}
    )
    observed: list[tuple[str, tuple[tuple[str, str], ...], bytes]] = []

    @session.on("/http")
    def explicit(req: cynapsa.CynapsaRequest) -> cynapsa.CynapsaResponse:
        assert type(req) is cynapsa.CynapsaRequest
        observed.append((req.path, req.headers, req.body))
        return cynapsa.CynapsaResponse(
            202,
            "Accepted",
            (("x-dup", "one"), ("x-dup", "two")),
            b"explicit",
        )

    session.on(
        "/auto",
        lambda req: cynapsa.CynapsaResponse(
            203,
            "Non-Authoritative Information",
            (("content-type", "application/json"),),
            b'{"auto":true}',
        ),
    )
    try:
        fake_library.emit_callback(CYNAPSA_CALLBACK_V1_EVENT, _http_event("explicit"))
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_EVENT, _http_event("auto", path="/auto")
        )
        _wait(
            lambda: len(
                [item for item in driver.commands if item["command_name"] == "message.reply"]
            )
            == 2
        )
        assert observed == [
            (
                "/http",
                (
                    ("content-type", "application/json"),
                    ("x-dup", "one"),
                    ("x-dup", "two"),
                ),
                b'{"value":1}',
            )
        ]
        replies = [
            item["args"]["payload"]["http_response"]
            for item in driver.commands
            if item["command_name"] == "message.reply"
        ]
        assert len(replies) == 2
        replies_by_status = {reply["status_code"]: reply for reply in replies}
        assert replies_by_status == {
            202: {
                "status_code": 202,
                "reason": "Accepted",
                "headers": [
                    {"name": "x-dup", "value": "one"},
                    {"name": "x-dup", "value": "two"},
                ],
                "body": base64.b64encode(b"explicit").decode("ascii"),
            },
            203: {
                "status_code": 203,
                "reason": "Non-Authoritative Information",
                "headers": [
                    {"name": "content-type", "value": "application/json"}
                ],
                "body": base64.b64encode(b'{"auto":true}').decode("ascii"),
            },
        }
        with pytest.raises(queue.Empty):
            session.next_local_diagnostic(0.01)
    finally:
        session.close()
        driver.stop()


@pytest.mark.parametrize("returned", [{"invented": True}, "invented"])
def test_http_inbound_handler_normalizes_native_shorthand(
    returned: object, fake_library: FakeLibrary, native_factory: None
) -> None:
    session, driver = _connect(
        fake_library, overrides={"message.reply": ("send", SEND_RESULT)}
    )
    session.on("/http", lambda req: returned)
    try:
        fake_library.emit_callback(CYNAPSA_CALLBACK_V1_EVENT, _http_event("bad-return"))
        _wait(
            lambda: len(
                [item for item in driver.commands if item["command_name"] == "message.reply"]
            )
            == 1
        )
        reply = next(
            item for item in driver.commands if item["command_name"] == "message.reply"
        )
        assert reply["args"]["payload"]["http_response"]["status_code"] == 200
    finally:
        session.close()
        driver.stop()


def test_http_inbound_msg_ignores_return_value_without_reply(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    session, driver = _connect(
        fake_library, overrides={"message.reply": ("send", SEND_RESULT)}
    )
    seen: list[cynapsa.CynapsaRequest] = []

    def handler(req: cynapsa.CynapsaRequest) -> cynapsa.CynapsaResponse:
        assert type(req) is cynapsa.CynapsaRequest
        seen.append(req)
        return cynapsa.CynapsaResponse(200, "OK", (), b"ignored")

    session.on("/http", handler)
    try:
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_EVENT,
            _http_event("msg-http", mode="msg", request_handle=None),
        )
        _wait(lambda: len(seen) == 1)
        time.sleep(0.02)
        assert not any(
            item["command_name"] == "message.reply" for item in driver.commands
        )
    finally:
        session.close()
        driver.stop()


@pytest.mark.asyncio
async def test_async_session_runs_sync_and_async_handlers_off_callback_and_event_loop(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library)
    driver.overrides["message.reply"] = ("send", SEND_RESULT)
    driver.start()
    session = await cynapsa.connect_async(**AUTH)
    caller_thread = threading.get_ident()
    seen: list[tuple[str, int]] = []

    async def asynchronous(req: cynapsa.CynapsaRequest) -> dict[str, bool]:
        await asyncio.sleep(0)
        seen.append(("async", threading.get_ident()))
        return {"ok": True}

    def synchronous(req: cynapsa.CynapsaRequest) -> None:
        seen.append(("sync", threading.get_ident()))

    session.on("/async", asynchronous)
    session.on("/sync", synchronous)
    try:
        fake_library.emit_callback(CYNAPSA_CALLBACK_V1_EVENT, _event("async", path="/async"))
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_EVENT,
            _event("sync", mode="msg", path="/sync", request_handle=None),
        )
        await asyncio.to_thread(_wait, lambda: len(seen) == 2)
        assert {name for name, _ in seen} == {"async", "sync"}
        assert all(thread_id != caller_thread for _, thread_id in seen)
    finally:
        await session.close()
        driver.stop()


@pytest.mark.asyncio
async def test_async_native_inbound_route_replies_http_response(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library)
    driver.overrides["message.reply"] = ("send", SEND_RESULT)
    driver.start()
    session = await cynapsa.connect_async(**AUTH)
    seen: list[tuple[str, bytes]] = []

    @session.on("/async-http")
    async def asynchronous(req: cynapsa.CynapsaRequest) -> cynapsa.CynapsaResponse:
        assert type(req) is cynapsa.CynapsaRequest
        seen.append((req.path, req.body))
        await asyncio.sleep(0)
        return cynapsa.CynapsaResponse(204, "No Content", (), b"")

    try:
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_EVENT,
            _http_event("async-http", path="/async-http", body=b"async"),
        )
        await asyncio.to_thread(
            _wait,
            lambda: len(
                [c for c in driver.commands if c["command_name"] == "message.reply"]
            )
            == 1,
        )
        assert seen == [("/async-http", b"async")]
        reply = next(c for c in driver.commands if c["command_name"] == "message.reply")
        assert reply["args"]["payload"]["http_response"]["status_code"] == 204
    finally:
        await session.close()
        driver.stop()


@pytest.mark.asyncio
async def test_async_handler_return_is_the_only_reply_mechanism(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library)
    driver.overrides["message.reply"] = ("send", SEND_RESULT)
    driver.start()
    session = await cynapsa.connect_async(**AUTH)

    @session.on("/explicit-http")
    async def handler(
        request: cynapsa.CynapsaRequest,
    ) -> cynapsa.CynapsaResponse:
        assert not hasattr(request, "reply")
        return cynapsa.CynapsaResponse(203, "Returned", (), b"automatic")

    try:
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_EVENT,
            _http_event("explicit-http", path="/explicit-http"),
        )
        await asyncio.to_thread(
            _wait,
            lambda: len(
                [c for c in driver.commands if c["command_name"] == "message.reply"]
            )
            == 1,
        )
        await asyncio.sleep(0.02)
        replies = [
            c for c in driver.commands if c["command_name"] == "message.reply"
        ]
        assert len(replies) == 1
        wire = replies[0]["args"]["payload"]["http_response"]
        assert wire["status_code"] == 203
        assert base64.b64decode(wire["body"]) == b"automatic"
    finally:
        await session.close()
        driver.stop()


def test_sync_handler_accepts_a_generic_awaitable_result(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    session, driver = _connect(fake_library)
    completed = threading.Event()

    class HandlerAwaitable:
        def __await__(self) -> Any:
            async def finish() -> None:
                await asyncio.sleep(0)
                completed.set()

            return finish().__await__()

    session.on(
        "/awaitable",
        lambda request: HandlerAwaitable(),
    )
    try:
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_EVENT,
            _event(
                "generic-awaitable",
                mode="msg",
                path="/awaitable",
                request_handle=None,
            ),
        )
        assert completed.wait(2)
        with pytest.raises(queue.Empty):
            session.next_local_diagnostic(0.01)
    finally:
        session.close()
        driver.stop()


def test_native_event_and_diagnostic_streams_remain_separate(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    session, driver = _connect(fake_library)
    ordinary = {
        "abi_version": 1,
        "event_id": "ordinary",
        "event_name": "peer.reachable",
        "created_at": "2026-08-26T12:00:00Z",
        "payload": {"peer": "agent-b", "reachable": True},
    }
    diagnostic = {
        "abi_version": 1,
        "event_id": "diagnostic",
        "event_name": "diagnostics.log",
        "created_at": "2026-08-26T12:00:00Z",
        "payload": {
            "level": "info",
            "code": "core_state",
            "message": "Core state changed",
        },
    }
    try:
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_DIAGNOSTIC,
            json.dumps(diagnostic, separators=(",", ":")).encode(),
        )
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_EVENT,
            json.dumps(ordinary, separators=(",", ":")).encode(),
        )
        assert session.next_event(2).event_id == "ordinary"
        assert session.next_diagnostic(2).event_id == "diagnostic"
        with pytest.raises(queue.Empty):
            session.next_event(0.01)
    finally:
        session.close()
        driver.stop()


@pytest.mark.parametrize(
    "retired_event_name",
    ["delivery.ordering_blocked", "delivery.unknown"],
)
def test_retired_core_event_becomes_local_diagnostic_and_callback_processing_continues(
    retired_event_name: str,
    fake_library: FakeLibrary,
    native_factory: None,
) -> None:
    session, driver = _connect(fake_library)
    retired = {
        "abi_version": 1,
        "event_id": "retired",
        "event_name": retired_event_name,
        "created_at": "2026-08-26T12:00:00Z",
        "payload": {},
    }
    following = {
        "abi_version": 1,
        "event_id": "following",
        "event_name": "peer.reachable",
        "created_at": "2026-08-26T12:00:00Z",
        "payload": {"peer": "agent-b", "reachable": True},
    }
    try:
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_EVENT,
            json.dumps(retired, separators=(",", ":")).encode(),
        )
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_EVENT,
            json.dumps(following, separators=(",", ":")).encode(),
        )

        assert session.next_local_diagnostic(2).code == "event_decode_failed"
        assert session.next_event(2).event_id == "following"
        assert session._owner.core is not None
        assert session._owner.core.callback_dispatch_error is None
    finally:
        session.close()
        driver.stop()


def test_canonical_request_has_no_explicit_reply_state() -> None:
    request = cynapsa.CynapsaRequest("POST", "/route", body=b"request")
    assert not hasattr(request, "reply")
    assert not hasattr(request, "reply_claimed")
    assert not hasattr(request, "replied")


def test_handler_queue_saturation_does_not_accept_unretained_event(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    session, driver = _connect(
        fake_library, auth={**AUTH, "queue_limit": 1}
    )
    entered = threading.Event()
    release = threading.Event()
    handled: list[str] = []

    def blocked(req: cynapsa.AztmRequest) -> None:
        entered.set()
        assert release.wait(2)
        handled.append(req.message_id)

    session.on("/route", blocked)
    try:
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_EVENT,
            _event("first", mode="msg", request_handle=None),
        )
        assert entered.wait(1)
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_EVENT,
            _event("second", mode="msg", request_handle=None),
        )
        _wait(
            lambda: any(
                item["command_name"] == "delivery.accept"
                and item["args"]["event_id"] == "second"
                for item in driver.commands
            )
        )
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_EVENT,
            _event("third", mode="msg", request_handle=None),
        )
        diagnostic = session.next_local_diagnostic(2)
        while diagnostic.code != "handler_queue_full":
            diagnostic = session.next_local_diagnostic(2)
        assert not any(
            item["command_name"] == "delivery.accept"
            and item["args"]["event_id"] == "third"
            for item in driver.commands
        )
        release.set()
        _wait(
            lambda: any(
                item["command_name"] == "delivery.accept"
                and item["args"]["event_id"] == "third"
                for item in driver.commands
            )
        )
        _wait(lambda: len(handled) == 3)
        assert session._owner.core.callback_dispatch_error is None
    finally:
        release.set()
        _wait(lambda: session._inbound._handler_queue.unfinished_tasks == 0)
        session.close()
        driver.stop()


def test_handler_concurrency_is_bounded_by_owned_worker_pool(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    session, driver = _connect(
        fake_library, auth={**AUTH, "queue_limit": 12}
    )
    lock = threading.Lock()
    release = threading.Event()
    active = 0
    maximum = 0

    def blocked(req: cynapsa.AztmRequest) -> None:
        nonlocal active, maximum
        with lock:
            active += 1
            maximum = max(maximum, active)
        release.wait(2)
        with lock:
            active -= 1

    session.on("/route", blocked)
    try:
        for index in range(12):
            fake_library.emit_callback(
                CYNAPSA_CALLBACK_V1_EVENT,
                _event(f"bounded-{index}", mode="msg", request_handle=None),
            )
        _wait(lambda: maximum == 8)
        assert maximum == 8
    finally:
        release.set()
        session.close()
        driver.stop()


def test_close_race_either_rejects_new_event_or_drains_accepted_handler(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    session, driver = _connect(fake_library)
    handled = threading.Event()
    session.on("/route", lambda req: handled.set())
    close_errors: list[BaseException] = []

    def close() -> None:
        try:
            session.close()
        except BaseException as exc:
            close_errors.append(exc)

    thread = threading.Thread(target=close)
    thread.start()
    try:
        try:
            fake_library.emit_callback(
                CYNAPSA_CALLBACK_V1_EVENT,
                _event("close-race", mode="msg", request_handle=None),
            )
        except RuntimeError:
            pass
        thread.join(2)
        assert not thread.is_alive()
        assert not close_errors
        accepted = any(
            item["command_name"] == "delivery.accept"
            and item["args"]["event_id"] == "close-race"
            for item in driver.commands
        )
        assert not accepted or handled.is_set()
        with pytest.raises(Exception):
            session.on("/later", lambda req: None)
        with pytest.raises(Exception):
            session.next_event(0)
    finally:
        driver.stop()


def test_unowned_delivery_remains_unaccepted_until_a_route_owns_it(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    session, driver = _connect(fake_library)
    handled = threading.Event()
    try:
        fake_library.emit_callback(CYNAPSA_CALLBACK_V1_EVENT, _event("late-owner"))
        diagnostic = session.next_local_diagnostic(2)
        assert diagnostic.code == "handler_not_found"
        assert not any(
            item["command_name"] == "delivery.accept"
            and item["args"]["event_id"] == "late-owner"
            for item in driver.commands
        )
        session.on("/route", lambda req: handled.set())
        assert handled.wait(2)
        assert len(
            [
                item
                for item in driver.commands
                if item["command_name"] == "delivery.accept"
                and item["args"]["event_id"] == "late-owner"
            ]
        ) == 1
    finally:
        session.close()
        driver.stop()


def test_hostile_event_id_reuse_is_tombstoned_without_reaccept_or_dispatch(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    session, driver = _connect(
        fake_library, overrides={"message.reply": ("send", SEND_RESULT)}
    )
    bodies: list[bytes] = []
    session.on("/route", lambda req: bodies.append(req.body))
    try:
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_EVENT, _event("reused", body=b"first")
        )
        _wait(lambda: bodies == [b"first"])
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_EVENT, _event("reused", body=b"hostile")
        )
        diagnostic = session.next_local_diagnostic(2)
        assert diagnostic.code == "event_id_reused"
        time.sleep(0.02)
        assert bodies == [b"first"]
        assert len(
            [
                item
                for item in driver.commands
                if item["command_name"] == "delivery.accept"
                and item["args"]["event_id"] == "reused"
            ]
        ) == 1
    finally:
        session.close()
        driver.stop()


def test_local_diagnostic_schema_is_closed_normalized_and_bounded() -> None:
    with pytest.raises(ValueError):
        cynapsa.AztmLocalDiagnostic("private", "raw implementation text")
    with pytest.raises(ValueError):
        cynapsa.AztmLocalDiagnostic("handler_error", "raw implementation text")
