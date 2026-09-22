from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

import cynapsa
from cynapsa.http import CynapsaResponse as WireResponse


VECTORS = Path(__file__).parents[1] / "src/cynapsa/_vendor/conformance/v1"


def _application_error_wire(details_json: str) -> dict[str, object]:
    return {
        "status_code": 400,
        "reason": "Bad Request",
        "headers": [],
        "body": "",
        "error": {
            "code": "bad_request",
            "detail": "Invalid request",
            "details_json": details_json,
        },
    }


def test_request_owns_exact_body_and_preserves_ordered_duplicate_headers() -> None:
    request = cynapsa.CynapsaRequest(
        "POST",
        "/items",
        "tag=one&tag=two",
        (("x-duplicate", "one"), ("x-duplicate", "two")),
        b"\x00body\xff",
    )

    assert request.content == b"\x00body\xff"
    assert request.headers == (
        ("x-duplicate", "one"),
        ("x-duplicate", "two"),
    )
    assert request.query == "tag=one&tag=two"
    assert not hasattr(request, "reply")


def test_inbound_context_is_never_serialized_as_caller_payload() -> None:
    request = cynapsa.CynapsaRequest(
        "POST",
        "/items",
        message_id="message-one",
        conversation_id="conversation-one",
        from_agent_id="agent-a@example.test",
        mesh_id="mesh-one",
        mode="rpc",
    )

    assert set(request._wire()["http_request"]) == {
        "method",
        "path",
        "query",
        "headers",
        "body",
    }


def test_request_and_response_helpers_decode_text_and_json() -> None:
    request = cynapsa.CynapsaRequest(
        "POST",
        "/",
        headers=(("content-type", "application/json"),),
        body=b'{"ok":true}',
    )
    response = cynapsa.CynapsaResponse.from_value(None)

    assert request.text() == '{"ok":true}'
    assert request.json() == {"ok": True}
    assert response.content == b"null"
    assert response.text() == "null"
    assert response.json() is None


def test_application_error_round_trips_without_changing_exact_body() -> None:
    error = cynapsa.CynapsaApplicationError(
        "not_found", "The record does not exist", {"record_id": 7}
    )
    response = cynapsa.CynapsaResponse(
        404,
        "Not Found",
        (
            ("set-cookie", "a=1"),
            ("x-cynapsa-error", "ordinary application header"),
            ("set-cookie", "b=2"),
        ),
        b"custom exact body",
        error,
    )

    decoded = WireResponse.from_canonical_json(response.canonical_json())
    assert decoded.body == b"custom exact body"
    assert decoded.error == error
    assert decoded.headers == response.headers
    wire = response._wire()["http_response"]
    assert wire["headers"] == [
        {"name": name, "value": value} for name, value in response.headers
    ]
    assert wire["error"]["details_json"] == '{"record_id":7}'
    with pytest.raises(cynapsa.RemoteApplicationError) as raised:
        decoded.raise_for_error()
    assert raised.value.response is decoded


def test_legacy_and_extended_http_response_vectors_decode_strictly() -> None:
    vectors = {
        item["name"]: item["json"]
        for item in json.loads((VECTORS / "payloads.json").read_text())
    }

    legacy = WireResponse.from_canonical_json(
        json.dumps(vectors["http response legacy"], separators=(",", ":")).encode()
    )
    assert legacy.status_code == 201
    assert legacy.error is None
    assert legacy.body == b"\x00\xff"

    extended = WireResponse.from_canonical_json(
        json.dumps(vectors["http response"], separators=(",", ":")).encode()
    )
    assert extended.status_code == 404
    assert extended.error == cynapsa.CynapsaApplicationError(
        "not_found", "The record does not exist", {"record_id": 7}
    )
    assert extended.body == b"\x00\xff"


def test_http_response_rejects_unknown_outer_and_error_fields() -> None:
    base = {
        "status_code": 404,
        "reason": "Not Found",
        "headers": [],
        "body": "",
    }
    with pytest.raises(ValueError, match="unknown or missing fields"):
        WireResponse.from_wire({**base, "unknown": True})
    with pytest.raises(ValueError, match="invalid Cynapsa error metadata"):
        WireResponse.from_wire(
            {
                **base,
                "error": {
                    "code": "not_found",
                    "detail": "Not found",
                    "details_json": "{}",
                    "unknown": True,
                },
            }
        )


@pytest.mark.parametrize(
    "details_json",
    [
        r'{"value":"\ud800"}',
        r'{"value":"\udc00"}',
        r'{"value":[{"nested":"\ud800A"}]}',
        r'{"\ud800":true}',
        r'{"\ud83d\ude00":1,"😀":2}',
    ],
)
def test_http_response_rejects_unpaired_application_error_surrogates(
    details_json: str,
) -> None:
    with pytest.raises(ValueError, match="invalid Cynapsa error metadata"):
        WireResponse.from_wire(_application_error_wire(details_json))


@pytest.mark.parametrize(
    "details_json, expected",
    [
        (r'{"value":"\ud83d\ude00"}', {"value": "😀"}),
        ('{"value":"😀"}', {"value": "😀"}),
        (r'{"\ud83d\ude00":"valid key"}', {"😀": "valid key"}),
    ],
)
def test_http_response_accepts_unicode_scalars_and_surrogate_pairs(
    details_json: str,
    expected: dict[str, object],
) -> None:
    response = WireResponse.from_wire(_application_error_wire(details_json))
    assert response.error is not None
    assert response.error.details == expected


def test_remote_application_error_is_not_raised_automatically() -> None:
    response = cynapsa.CynapsaResponse.from_rpc_exception(
        cynapsa.RPCException(
            403,
            code="forbidden",
            detail="Access denied",
            details={"safe": True},
        )
    )

    assert response.status_code == 403
    assert response.error is not None
    assert response.error.code == "forbidden"
    assert response.json()["error"]["detail"] == "Access denied"


@pytest.mark.parametrize(
    "value",
    [
        {"nested": (1, 2)},
        {"nested": {1: "invalid key"}},
        [object()],
    ],
)
def test_handler_return_rejects_implicit_or_custom_json_conversions(
    value: object,
) -> None:
    with pytest.raises(TypeError, match="JSON-compatible"):
        cynapsa.CynapsaResponse.from_value(value)


def test_rpc_exception_freezes_safe_details_and_headers() -> None:
    details = {"items": [1, 2]}
    headers = (("x-duplicate", "one"), ("x-duplicate", "two"))
    exception = cynapsa.RPCException(
        409,
        code="conflict",
        detail="Conflict",
        details=details,
        headers=headers,
    )
    details["items"].append(3)

    assert exception.details == {"items": (1, 2)}
    assert exception.headers == headers


def test_application_error_rejects_inconsistent_or_unbounded_metadata() -> None:
    error = cynapsa.CynapsaApplicationError("failed", "failed")
    with pytest.raises(ValueError, match="4xx or 5xx"):
        cynapsa.CynapsaResponse(200, error=error)
    with pytest.raises(ValueError, match="8192-byte"):
        cynapsa.CynapsaResponse(
            400,
            error=cynapsa.CynapsaApplicationError(
                "failed", "failed", {"value": "x" * 8192}
            ),
        )
    with pytest.raises(TypeError, match="JSON-compatible"):
        cynapsa.CynapsaApplicationError("failed", "failed", {1: "bad"})


def test_unified_handler_signature_has_no_mode_or_reply_api() -> None:
    assert "mode" not in inspect.signature(cynapsa.AztmSession.on).parameters
    assert "mode" not in inspect.signature(cynapsa.AsyncAztmSession.on).parameters
    assert not hasattr(cynapsa.CynapsaRequest, "reply")
