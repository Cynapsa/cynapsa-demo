"""Versioned public SDK models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re
from typing import Literal, Mapping, TypeAlias

from .errors import ErrorDetails, sdk_error

JsonScalar: TypeAlias = None | bool | int | float | str
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
JsonObject: TypeAlias = dict[str, JsonValue]

CommandName: TypeAlias = Literal[
    "core.init",
    "core.capabilities",
    "core.status",
    "core.shutdown",
    "session.config.get",
    "session.config.update",
    "command.channel.register",
    "command.channel.clear",
    "command.cancel",
    "event.sink.register",
    "event.sink.clear",
    "event.sink.bind",
    "auth.login",
    "auth.connect",
    "auth.token_login",
    "auth.token_connect",
    "auth.installation_login",
    "auth.installation_connect",
    "auth.logout",
    "auth.agent_id",
    "mesh.list",
    "mesh.membership.refresh",
    "address.map.put",
    "address.map.remove",
    "address.map.list",
    "address.resolve",
    "message.send",
    "message.request",
    "message.reply",
    "delivery.next",
    "delivery.accept",
    "handler.register",
    "handler.unregister",
    "payload.open",
    "payload.write_chunk",
    "payload.finish",
    "payload.cancel",
    "payload.read",
    "payload.close",
    "payload.retain",
    "payload.release",
    "delivery.queue.status",
    "delivery.retry",
    "delivery.pause",
    "delivery.resume",
    "delivery.drop",
    "conversation.list",
    "conversation.status",
    "conversation.close",
    "policy.set",
    "policy.get",
    "policy.test",
    "diagnostics.peer_status",
    "diagnostics.connectivity_status",
    "diagnostics.snapshot",
    "diagnostics.logs.subscribe",
]

RESULT_TYPES = frozenset(
    {
        "empty",
        "capabilities",
        "status",
        "config",
        "auth",
        "agent_id",
        "mesh_list",
        "address_mappings",
        "address_resolution",
        "send",
        "response",
        "event",
        "delivery_queue_status",
        "payload_handle",
        "conversation_status",
        "conversation_list",
        "policy",
        "peer_status",
        "connectivity_status",
        "diagnostic_snapshot",
        "core_init",
        "completion_channel",
        "event_sink",
    }
)

EVENT_NAMES = frozenset(
    {
        "session.state_changed",
        "connectivity.state_changed",
        "peer.reachable",
        "peer.unreachable",
        "message.received",
        "message.queued",
        "message.retried",
        "message.deduplicated",
        "delivery.resumed",
        "delivery.replayed",
        "payload.transfer_started",
        "payload.transfer_progress",
        "payload.transfer_completed",
        "payload.transfer_failed",
        "policy.rejected",
        "rpc.timeout",
        "command.queue_full",
        "event.queue_full",
        "core.error",
        "diagnostics.log",
    }
)

_LIFECYCLES = frozenset({"created", "connecting", "ready", "degraded", "closing", "closed", "failed"})
_CONNECTIVITY_STATES = frozenset({"unknown", "available", "degraded", "unavailable"})
_PERSONALITIES = frozenset({"unset", "http_bridge", "native"})
_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z$")
MAX_TIMEOUT_MS = 9_223_372_036_854
MAXIMUM_PAYLOAD_BYTES = 134_217_696


@dataclass(frozen=True, slots=True)
class CoreConfig:
    """Process-local V1 limits used when one core is created."""

    command_timeout_ms: int = 0
    rpc_timeout_ms: int = 0
    queue_limit: int = 256
    payload_limit: int = 64 * 1024 * 1024

    def to_document(self) -> JsonObject:
        values = (
            self.command_timeout_ms,
            self.rpc_timeout_ms,
            self.queue_limit,
            self.payload_limit,
        )
        if any(type(value) is not int for value in values):
            raise sdk_error("Core configuration values must be integers")
        if not 0 <= self.command_timeout_ms <= MAX_TIMEOUT_MS or not 0 <= self.rpc_timeout_ms <= MAX_TIMEOUT_MS:
            raise sdk_error("Core timeouts are outside the V1 range")
        if not 1 <= self.queue_limit <= 65_536:
            raise sdk_error("Core queue_limit is outside the V1 range")
        if not 1 <= self.payload_limit <= MAXIMUM_PAYLOAD_BYTES:
            raise sdk_error("Core payload_limit is outside the V1 range")
        return {
            "abi_version": 1,
            "command_timeout_ms": self.command_timeout_ms,
            "rpc_timeout_ms": self.rpc_timeout_ms,
            "queue_limit": self.queue_limit,
            "payload_limit": self.payload_limit,
        }


@dataclass(frozen=True, slots=True)
class Admission:
    command_id: str
    command_handle: str
    accepted: bool
    error: ErrorDetails | None

    @classmethod
    def from_document(cls, document: object) -> "Admission":
        value = _document(document, {"abi_version", "command_id", "accepted"}, {"command_handle", "error"})
        command_id = _string(value, "command_id")
        accepted = _boolean(value, "accepted")
        command_handle = value.get("command_handle", "")
        if not isinstance(command_handle, str):
            raise sdk_error("The native core returned an invalid admission")
        error = ErrorDetails.from_value(value["error"]) if "error" in value else None
        if accepted != (error is None) or accepted != bool(command_handle):
            raise sdk_error("The native core returned an inconsistent admission")
        return cls(command_id, command_handle, accepted, error)


@dataclass(frozen=True, slots=True)
class Completion:
    command_id: str
    ok: bool
    result_type: str | None
    error: ErrorDetails | None

    @classmethod
    def from_document(cls, document: object) -> "Completion":
        value = _document(document, {"abi_version", "command_id", "ok"}, {"result_type", "result", "error"})
        command_id = _string(value, "command_id")
        ok = _boolean(value, "ok")
        result_type = value.get("result_type")
        result = value.get("result")
        error = ErrorDetails.from_value(value["error"]) if "error" in value else None
        if ok:
            if result_type not in RESULT_TYPES or not isinstance(result, dict) or error is not None:
                raise sdk_error("The native core returned an inconsistent completion")
            return cls(command_id, True, result_type, None)
        if result_type is not None or result is not None or error is None:
            raise sdk_error("The native core returned an inconsistent completion")
        return cls(command_id, False, None, error)


@dataclass(frozen=True, slots=True)
class CoreEvent:
    event_id: str
    event_name: str
    created_at: str

    @classmethod
    def from_document(cls, document: object) -> "CoreEvent":
        value = _document(document, {"abi_version", "event_id", "event_name", "created_at", "payload"}, set())
        payload = value["payload"]
        if not isinstance(payload, dict):
            raise sdk_error("The native core returned an invalid event")
        event_name = _string(value, "event_name")
        if event_name not in EVENT_NAMES:
            raise sdk_error("The native core returned an unknown event type")
        created_at = _string(value, "created_at")
        try:
            parsed_timestamp = datetime.fromisoformat(created_at[:-1] + "+00:00")
        except ValueError:
            parsed_timestamp = None
        if _TIMESTAMP.fullmatch(created_at) is None or parsed_timestamp is None:
            raise sdk_error("The native core returned an invalid event timestamp")
        return cls(
            event_id=_string(value, "event_id"),
            event_name=event_name,
            created_at=created_at,
        )


@dataclass(frozen=True, slots=True)
class CoreStatus:
    lifecycle: str
    connectivity: str
    personality: str
    agent_id: str
    mesh_id: str
    mesh_endpoint: str
    queued_message_count: int

    @classmethod
    def from_document(cls, document: object) -> "CoreStatus":
        value = _document(document, {"abi_version", "status"}, set())
        status = value["status"]
        if not isinstance(status, Mapping) or set(status) != {
            "lifecycle",
            "connectivity",
            "personality",
            "agent_id",
            "mesh_id",
            "mesh_endpoint",
            "queued_message_count",
        }:
            raise sdk_error("The native core returned an invalid status")
        queued = status["queued_message_count"]
        if type(queued) is not int or queued < 0:
            raise sdk_error("The native core returned an invalid status")
        lifecycle = _string(status, "lifecycle")
        connectivity = _string(status, "connectivity")
        personality = _string(status, "personality")
        if lifecycle not in _LIFECYCLES or connectivity not in _CONNECTIVITY_STATES or personality not in _PERSONALITIES:
            raise sdk_error("The native core returned an unknown status value")
        return cls(
            lifecycle=lifecycle,
            connectivity=connectivity,
            personality=personality,
            agent_id=_string(status, "agent_id"),
            mesh_id=_string(status, "mesh_id"),
            mesh_endpoint=_string(status, "mesh_endpoint"),
            queued_message_count=queued,
        )


def _document(value: object, required: set[str], optional: set[str]) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or not required.issubset(value) or set(value) - required - optional:
        raise sdk_error("The native core returned an invalid V1 document")
    if value["abi_version"] != 1:
        raise sdk_error("The native core returned an unsupported ABI version", code="unsupported_version")
    return value


def _string(value: Mapping[str, object], key: str) -> str:
    item = value[key]
    if not isinstance(item, str):
        raise sdk_error("The native core returned an invalid V1 document")
    return item


def _boolean(value: Mapping[str, object], key: str) -> bool:
    item = value[key]
    if type(item) is not bool:
        raise sdk_error("The native core returned an invalid V1 document")
    return item
