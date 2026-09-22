from __future__ import annotations

import base64
import json
from dataclasses import FrozenInstanceError
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest

import cynapsa
from cynapsa.events import EVENT_NAMES, MessageReceived


VECTORS = Path(__file__).parents[1] / "src/cynapsa/_vendor/conformance/v1/events.json"


def _raw(value: dict[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


def test_public_catalog_excludes_retired_vendored_events() -> None:
    vectors = json.loads(VECTORS.read_text())
    public = [item for item in vectors if item["name"] in EVENT_NAMES]
    assert {item["name"] for item in public} == EVENT_NAMES
    incompatible = [item for item in vectors if item["name"] not in EVENT_NAMES]
    assert incompatible == []
    for item in public:
        event = cynapsa.decode_event(_raw(item["json"]))
        assert event.event_name == item["name"]
        assert event.created_at.tzinfo is timezone.utc
        assert not isinstance(event.payload, dict)
        with pytest.raises(FrozenInstanceError):
            event.event_id = "changed"  # type: ignore[misc]


def test_unknown_malformed_duplicate_and_cross_stream_events_fail_closed() -> None:
    base = json.loads(VECTORS.read_text())[0]["json"]
    cases = [
        {**base, "event_name": "delivery.ordering_blocked"},
        {**base, "event_name": "delivery.unknown"},
        {**base, "unknown": True},
        {**base, "abi_version": True},
        {**base, "created_at": "2026-02-30T00:00:00Z"},
        {**base, "created_at": "2026-01-01 00:00:00Z"},
        {**base, "event_id": ""},
    ]
    for value in cases:
        with pytest.raises(ValueError, match="public ABI-v1"):
            cynapsa.decode_event(_raw(value))
    duplicate = b'{"abi_version":1,"event_id":"a","event_id":"b","event_name":"session.state_changed","created_at":"1970-01-01T00:00:01Z","payload":{"previous":"created","current":"ready"}}'
    with pytest.raises(ValueError):
        cynapsa.decode_event(duplicate)
    with pytest.raises(ValueError):
        cynapsa.decode_event(_raw(base), diagnostic=True)


def test_created_at_accepts_only_the_go_boundary_canonical_rfc3339_spelling() -> None:
    base = json.loads(VECTORS.read_text())[0]["json"]
    accepted = [
        "1970-01-01T00:00:01Z",
        "1970-01-01T00:00:01.1Z",
        "1970-01-01T00:00:01.123456789Z",
    ]
    for created_at in accepted:
        assert cynapsa.decode_event(
            _raw({**base, "created_at": created_at})
        ).created_at.tzinfo is timezone.utc
    rejected = [
        "1970-01-01T00:00:01.0Z",
        "1970-01-01T00:00:01.10Z",
        "1970-01-01T00:00:01.123456780Z",
        "1970-01-01T00:00:01+00:00",
        "1970-01-01t00:00:01Z",
        "1970-01-01T00:00:01z",
    ]
    for created_at in rejected:
        with pytest.raises(ValueError, match="public ABI-v1"):
            cynapsa.decode_event(_raw({**base, "created_at": created_at}))


def test_message_event_mode_handle_payload_path_base64_and_size_are_strict() -> None:
    vector = next(
        item["json"]
        for item in json.loads(VECTORS.read_text())
        if item["name"] == "message.received"
    )
    event = cynapsa.decode_event(_raw(vector))
    assert isinstance(event.payload, MessageReceived)
    assert event.payload.request_handle is None
    payload = vector["payload"]
    reqh = "reqh_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    rpc = {**vector, "payload": {**payload, "mode": "rpc", "request_handle": reqh}}
    assert isinstance(cynapsa.decode_event(_raw(rpc)).payload, MessageReceived)
    mutations = [
        {**payload, "mode": "rpc", "request_handle": ""},
        {**payload, "mode": "msg", "request_handle": reqh},
        {**payload, "mode": "rpc_response", "request_handle": ""},
        {**payload, "message_id": ""},
        {**payload, "payload": {"native": {"content_type": "", "path": "relative", "body": ""}}},
        {**payload, "payload": {"native": {"content_type": "", "path": "/", "body": "eA"}}},
        {**payload, "payload": {"native": {"content_type": "", "path": "/", "body": base64.b64encode(b"x" * (256 << 10)).decode()}}},
    ]
    for changed in mutations:
        with pytest.raises(ValueError):
            cynapsa.decode_event(_raw({**vector, "payload": changed}))


def test_native_message_event_accepts_http_request_but_not_http_response() -> None:
    vector = next(
        item["json"]
        for item in json.loads(VECTORS.read_text())
        if item["name"] == "message.received"
    )
    payload = vector["payload"]
    request_variant = {
        "http_request": {
            "method": "POST",
            "path": "/http",
            "query": "x=1",
            "headers": [
                {"name": "x-dup", "value": "one"},
                {"name": "x-dup", "value": "two"},
            ],
            "body": base64.b64encode(b"body").decode(),
        }
    }
    event = cynapsa.decode_event(
        _raw({**vector, "payload": {**payload, "payload": request_variant}})
    )
    assert isinstance(event.payload, MessageReceived)
    assert type(event.payload.payload) is cynapsa.HTTPRequestPayload
    assert event.payload.payload.headers == (("x-dup", "one"), ("x-dup", "two"))
    assert event.payload.payload.body == b"body"

    response_variant = {
        "http_response": {
            "status_code": 200,
            "reason": "OK",
            "headers": [],
            "body": "",
        }
    }
    with pytest.raises(ValueError):
        cynapsa.decode_event(
            _raw({**vector, "payload": {**payload, "payload": response_variant}})
        )
