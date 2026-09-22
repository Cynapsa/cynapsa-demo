"""Strict immutable models for the public ABI-v1 event catalog."""

from __future__ import annotations

import base64
import binascii
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TypeAlias

from cynapsa.http import HTTPRequestPayload
from cynapsa.messaging import NativePayload, _native_payload_from_wire
from cynapsa.native.command import (
    MAX_CANONICAL_PAYLOAD_BYTES,
    MAX_IDENTIFIER_BYTES,
    MAX_PATH_BYTES,
    MAX_PUBLIC_STRING_BYTES,
    MAX_QUEUE_CAPACITY,
    PUBLIC_ERROR_MESSAGES,
    PUBLIC_ERROR_STAGES,
)

_EVENT_FIELDS = {"abi_version", "event_id", "event_name", "created_at", "payload"}
_RFC3339_UTC = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$"
)
_LIFECYCLES = frozenset(
    {"created", "connecting", "ready", "degraded", "closing", "closed", "failed"}
)
_CONNECTIVITY = frozenset({"unknown", "available", "degraded", "unavailable"})
_MESSAGE_STATES = {
    "message.queued": "queued",
    "message.retried": "queued",
    "message.deduplicated": "ready",
    "delivery.resumed": "ready",
    "delivery.replayed": "queued",
}
_TRANSFER_STATES = {
    "payload.transfer_started": "started",
    "payload.transfer_progress": "progress",
    "payload.transfer_completed": "completed",
    "payload.transfer_failed": "failed",
}
_DIAGNOSTIC_MESSAGES = {
    "core_state": "Core state changed",
    "connectivity": "Connectivity state changed",
    "queue_pressure": "A local queue reached capacity",
    "payload_transfer": "Payload transfer state changed",
}
EVENT_NAMES = frozenset(
    {
        "session.state_changed",
        "connectivity.state_changed",
        "peer.reachable",
        "peer.unreachable",
        "message.received",
        *_MESSAGE_STATES,
        *_TRANSFER_STATES,
        "policy.rejected",
        "rpc.timeout",
        "command.queue_full",
        "event.queue_full",
        "core.error",
        "diagnostics.log",
    }
)


def _object(value: object, fields: set[str], label: str) -> dict[str, object]:
    if type(value) is not dict or set(value) != fields:
        raise ValueError(f"{label} has unknown or missing fields")
    return value


def _optional_object(
    value: object, required: set[str], optional: set[str], label: str
) -> dict[str, object]:
    if type(value) is not dict or not required <= set(value) or not set(value) <= required | optional:
        raise ValueError(f"{label} has unknown or missing fields")
    return value


def _text(
    value: object,
    field: str,
    *,
    maximum: int = MAX_IDENTIFIER_BYTES,
    empty: bool = False,
) -> str:
    if type(value) is not str or (not empty and not value):
        raise ValueError(f"{field} is not valid text")
    try:
        size = len(value.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise ValueError(f"{field} contains invalid Unicode") from exc
    if size > maximum:
        raise ValueError(f"{field} exceeds its limit")
    return value


def _integer(value: object, field: str, *, minimum: int = 0, maximum: int) -> int:
    if type(value) is not int or value < minimum or value > maximum:
        raise ValueError(f"{field} is outside its allowed range")
    return value


def _opaque(value: object, field: str, prefix: str, byte_length: int) -> str:
    handle = _text(value, field)
    if not handle.startswith(prefix):
        raise ValueError(f"{field} has the wrong opaque class")
    encoded = handle[len(prefix) :]
    try:
        decoded = base64.b64decode(
            encoded + "=" * (-len(encoded) % 4), altchars=b"-_", validate=True
        )
    except (ValueError, binascii.Error) as exc:
        raise ValueError(f"{field} has invalid opaque text") from exc
    if (
        len(decoded) != byte_length
        or base64.urlsafe_b64encode(decoded).rstrip(b"=").decode("ascii") != encoded
    ):
        raise ValueError(f"{field} has an invalid opaque shape")
    return handle


def _diagnostic_id(value: object) -> str:
    return _opaque("diag_" + _text(value, "diagnostic_id"), "diagnostic_id", "diag_", 32)[5:]


def _path(value: object) -> str:
    path = _text(value, "path", maximum=MAX_PATH_BYTES)
    if not path.startswith("/") or "?" in path or "#" in path:
        raise ValueError("path is not an absolute application path")
    return path


def _timestamp(value: object) -> datetime:
    text = _text(value, "created_at", maximum=MAX_PUBLIC_STRING_BYTES)
    match = _RFC3339_UTC.fullmatch(text)
    if match is None:
        raise ValueError("created_at is not canonical UTC RFC3339")
    year, month, day, hour, minute, second = map(int, match.groups()[:6])
    fraction = match.group(7) or ""
    # The Go boundary uses the RFC3339Nano layout with trailing fractional
    # zeroes removed. Accepting another spelling of the same instant would
    # make event identity and compatibility checks non-canonical.
    if fraction.endswith("0"):
        raise ValueError("created_at is not canonical UTC RFC3339")
    try:
        return datetime(
            year,
            month,
            day,
            hour,
            minute,
            second,
            int((fraction + "000000")[:6]),
            tzinfo=timezone.utc,
        )
    except ValueError as exc:
        raise ValueError("created_at is not a valid timestamp") from exc


def _strict_document(raw: bytes) -> dict[str, object]:
    if type(raw) is not bytes:
        raise TypeError("raw event must be bytes")
    if len(raw) > 2 << 20:
        raise ValueError("event document exceeds the ABI input limit")

    def pairs(items: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in items:
            if key in result:
                raise ValueError("event contains a duplicate JSON field")
            result[key] = value
        return result

    def constant(_: str) -> object:
        raise ValueError("event contains a non-finite number")

    value = json.loads(
        raw.decode("utf-8", errors="strict"),
        object_pairs_hook=pairs,
        parse_constant=constant,
    )
    return _object(value, _EVENT_FIELDS, "event")


@dataclass(frozen=True, slots=True)
class SessionStateChanged:
    previous: str
    current: str


@dataclass(frozen=True, slots=True)
class ConnectivityStateChanged:
    previous: str
    current: str


@dataclass(frozen=True, slots=True)
class PeerReachability:
    peer: str
    reachable: bool


@dataclass(frozen=True, slots=True)
class MessageReceived:
    message_id: str
    conversation_id: str
    from_agent_id: str
    mesh_id: str
    mode: str
    request_handle: str | None
    payload: NativePayload | HTTPRequestPayload


@dataclass(frozen=True, slots=True)
class MessageStateChanged:
    message_id: str
    conversation_id: str
    state: str


@dataclass(frozen=True, slots=True)
class PayloadTransferChanged:
    handle: str
    state: str
    completed: int
    total: int


@dataclass(frozen=True, slots=True)
class PolicyRejected:
    message_id: str
    peer: str
    path: str


@dataclass(frozen=True, slots=True)
class RpcTimedOut:
    command_id: str


@dataclass(frozen=True, slots=True)
class QueueFull:
    queue: str
    capacity: int


@dataclass(frozen=True, slots=True)
class AztmEventError:
    code: str
    message: str
    retryable: bool
    stage: str
    local_or_remote: str
    diagnostic_id: str | None = None


@dataclass(frozen=True, slots=True)
class CoreError:
    error: AztmEventError


@dataclass(frozen=True, slots=True)
class DiagnosticLog:
    level: str
    code: str
    message: str
    diagnostic_id: str | None = None


EventPayload: TypeAlias = (
    SessionStateChanged
    | ConnectivityStateChanged
    | PeerReachability
    | MessageReceived
    | MessageStateChanged
    | PayloadTransferChanged
    | PolicyRejected
    | RpcTimedOut
    | QueueFull
    | CoreError
    | DiagnosticLog
)


@dataclass(frozen=True, slots=True)
class AztmEvent:
    event_id: str
    event_name: str
    created_at: datetime
    payload: EventPayload


def _error(value: object) -> AztmEventError:
    fields = _optional_object(
        value,
        {"code", "message", "retryable", "stage", "local_or_remote"},
        {"diagnostic_id"},
        "event error",
    )
    code = _text(fields["code"], "error.code")
    message = _text(fields["message"], "error.message", maximum=MAX_PUBLIC_STRING_BYTES)
    stage = _text(fields["stage"], "error.stage")
    location = _text(fields["local_or_remote"], "error.local_or_remote")
    if code not in PUBLIC_ERROR_MESSAGES or message != PUBLIC_ERROR_MESSAGES[code]:
        raise ValueError("event error is not normalized")
    if stage not in PUBLIC_ERROR_STAGES or location not in {"local", "remote"}:
        raise ValueError("event error has an unknown stage or location")
    if type(fields["retryable"]) is not bool:
        raise ValueError("event error retryable is not boolean")
    diagnostic = _diagnostic_id(fields["diagnostic_id"]) if "diagnostic_id" in fields else None
    return AztmEventError(code, message, fields["retryable"], stage, location, diagnostic)


def _payload(
    name: str, value: object, *, http_bridge: bool = False
) -> EventPayload:
    if name == "session.state_changed":
        fields = _object(value, {"previous", "current"}, "event payload")
        previous = _text(fields["previous"], "previous")
        current = _text(fields["current"], "current")
        if previous not in _LIFECYCLES or current not in _LIFECYCLES:
            raise ValueError("event lifecycle is unknown")
        return SessionStateChanged(previous, current)
    if name == "connectivity.state_changed":
        fields = _object(value, {"previous", "current"}, "event payload")
        previous = _text(fields["previous"], "previous")
        current = _text(fields["current"], "current")
        if previous not in _CONNECTIVITY or current not in _CONNECTIVITY:
            raise ValueError("event connectivity is unknown")
        return ConnectivityStateChanged(previous, current)
    if name in {"peer.reachable", "peer.unreachable"}:
        fields = _object(value, {"peer", "reachable"}, "event payload")
        if type(fields["reachable"]) is not bool or fields["reachable"] != (name == "peer.reachable"):
            raise ValueError("event reachability does not match its name")
        return PeerReachability(_text(fields["peer"], "peer"), fields["reachable"])
    if name == "message.received":
        fields = _object(
            value,
            {"message_id", "conversation_id", "from_agent_id", "mesh_id", "mode", "request_handle", "payload"},
            "event payload",
        )
        mode = _text(fields["mode"], "mode")
        raw_handle = _text(fields["request_handle"], "request_handle", empty=True)
        if mode == "rpc":
            request_handle: str | None = _opaque(raw_handle, "request_handle", "reqh_", 32)
        elif mode == "msg":
            if raw_handle:
                raise ValueError("one-way message exposes a request handle")
            request_handle = None
        else:
            # rpc_response is deliberately not a public inbound event mode.
            raise ValueError("message mode is unknown")
        if http_bridge:
            wire_payload = fields["payload"]
            if (
                not isinstance(wire_payload, dict)
                or set(wire_payload) != {"http_request"}
            ):
                raise ValueError("HTTP Bridge delivery is not an HTTP request")
            payload: NativePayload | HTTPRequestPayload = HTTPRequestPayload.from_wire(
                wire_payload["http_request"]
            )
        else:
            wire_payload = fields["payload"]
            if not isinstance(wire_payload, dict) or len(wire_payload) != 1:
                raise ValueError("message payload must contain exactly one supported variant")
            if "native" in wire_payload:
                payload = _native_payload_from_wire(wire_payload)
            elif "http_request" in wire_payload:
                payload = HTTPRequestPayload.from_wire(wire_payload["http_request"])
            else:
                raise ValueError("message payload is not an inbound request variant")
        return MessageReceived(
            _text(fields["message_id"], "message_id"),
            _text(fields["conversation_id"], "conversation_id"),
            _text(fields["from_agent_id"], "from_agent_id"),
            _text(fields["mesh_id"], "mesh_id"),
            mode,
            request_handle,
            payload,
        )
    if name in _MESSAGE_STATES:
        fields = _object(value, {"message_id", "conversation_id", "state"}, "event payload")
        if fields["state"] != _MESSAGE_STATES[name]:
            raise ValueError("message state does not match its event name")
        return MessageStateChanged(
            _text(fields["message_id"], "message_id"),
            _text(fields["conversation_id"], "conversation_id"),
            _text(fields["state"], "state"),
        )
    if name in _TRANSFER_STATES:
        fields = _object(value, {"handle", "state", "completed", "total"}, "event payload")
        if fields["state"] != _TRANSFER_STATES[name]:
            raise ValueError("payload state does not match its event name")
        completed = _integer(fields["completed"], "completed", maximum=MAX_CANONICAL_PAYLOAD_BYTES)
        total = _integer(fields["total"], "total", maximum=MAX_CANONICAL_PAYLOAD_BYTES)
        if completed > total:
            raise ValueError("payload progress exceeds total")
        return PayloadTransferChanged(
            _opaque(fields["handle"], "handle", "payh_", 32),
            _text(fields["state"], "state"),
            completed,
            total,
        )
    if name == "policy.rejected":
        fields = _object(value, {"message_id", "peer", "path"}, "event payload")
        return PolicyRejected(
            _text(fields["message_id"], "message_id"),
            _text(fields["peer"], "peer"),
            _path(fields["path"]),
        )
    if name == "rpc.timeout":
        fields = _object(value, {"command_id"}, "event payload")
        return RpcTimedOut(_text(fields["command_id"], "command_id"))
    if name in {"command.queue_full", "event.queue_full"}:
        fields = _object(value, {"queue", "capacity"}, "event payload")
        expected = "command" if name == "command.queue_full" else "event"
        if fields["queue"] != expected:
            raise ValueError("queue does not match its event name")
        return QueueFull(
            expected,
            _integer(fields["capacity"], "capacity", minimum=1, maximum=MAX_QUEUE_CAPACITY),
        )
    if name == "core.error":
        fields = _object(value, {"error"}, "event payload")
        return CoreError(_error(fields["error"]))
    if name == "diagnostics.log":
        fields = _optional_object(
            value,
            {"level", "code", "message"},
            {"diagnostic_id"},
            "diagnostic payload",
        )
        level = _text(fields["level"], "level")
        code = _text(fields["code"], "code")
        message = _text(fields["message"], "message", maximum=MAX_PUBLIC_STRING_BYTES)
        if level not in {"debug", "info", "warn", "error"}:
            raise ValueError("diagnostic level is unknown")
        if code not in _DIAGNOSTIC_MESSAGES or message != _DIAGNOSTIC_MESSAGES[code]:
            raise ValueError("diagnostic is not normalized")
        diagnostic = _diagnostic_id(fields["diagnostic_id"]) if "diagnostic_id" in fields else None
        return DiagnosticLog(level, code, message, diagnostic)
    raise ValueError("event name is unknown")


def decode_event(
    raw: bytes,
    *,
    diagnostic: bool | None = None,
    http_bridge: bool = False,
) -> AztmEvent:
    """Decode exactly one closed ABI-v1 event, rejecting every extension field."""

    try:
        body = _strict_document(raw)
        if type(body["abi_version"]) is not int or body["abi_version"] != 1:
            raise ValueError("event ABI version is unsupported")
        event_id = _text(body["event_id"], "event_id")
        name = _text(body["event_name"], "event_name")
        if name not in EVENT_NAMES:
            raise ValueError("event name is unknown")
        if diagnostic is True and name != "diagnostics.log":
            raise ValueError("diagnostic stream contains a non-diagnostic event")
        if diagnostic is False and name == "diagnostics.log":
            raise ValueError("event stream contains a diagnostic event")
        created_at = _timestamp(body["created_at"])
        payload = _payload(name, body["payload"], http_bridge=http_bridge)
        return AztmEvent(event_id, name, created_at, payload)
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        RecursionError,
        TypeError,
        ValueError,
    ):
        raise ValueError(
            "native event does not conform to the public ABI-v1 schema"
        ) from None


__all__ = [
    "AztmEvent",
    "AztmEventError",
    "ConnectivityStateChanged",
    "CoreError",
    "DiagnosticLog",
    "EVENT_NAMES",
    "MessageReceived",
    "MessageStateChanged",
    "PayloadTransferChanged",
    "PeerReachability",
    "PolicyRejected",
    "QueueFull",
    "RpcTimedOut",
    "SessionStateChanged",
    "decode_event",
]
