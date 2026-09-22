"""Strict ABI-v1 command documents and polling-backed command futures."""

from __future__ import annotations

import asyncio
import base64
import binascii
import ipaddress
import json
import math
import re
import secrets
import threading
import time
import unicodedata
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Any
from urllib.parse import quote, urlsplit

from cynapsa.exceptions import NativeError

from .abi import ABI_VERSION, CYNAPSA_STATUS_V1_ERROR

MAX_ABI_INPUT_BYTES = 2 << 20
MAX_IDENTIFIER_BYTES = 512
MAX_AGENT_ID_BYTES = 256
MAX_PUBLIC_STRING_BYTES = 8192
MAX_MESH_ENDPOINT_BYTES = 4096
MAX_MESH_ID_BYTES = 255
MAX_AUTH_TOKEN_BYTES = 88
MAX_PROFILE_ID_BYTES = 128
MAX_PATH_BYTES = 2048
MAX_QUEUE_CAPACITY = 65_536
MAX_HEADER_COUNT = 4096
MAX_POLICY_RULES = 1024
MAX_INLINE_PAYLOAD_BYTES = 256 << 10
MAX_PAYLOAD_CHUNK_BYTES = 1 << 20
MAX_CANONICAL_PAYLOAD_BYTES = 1 << 30
MAX_TIMEOUT_MS = (2**63 - 1) // 1_000_000

PUBLIC_ERROR_MESSAGES = MappingProxyType(
    {
        "sdk_error": "The SDK operation could not be completed",
        "command_error": "The command could not be completed",
        "invalid_input": "The command input is invalid",
        "malformed_input": "The command input is invalid",
        "unsupported_version": "The requested contract version is not supported",
        "invalid_handle": "The local handle is invalid",
        "queue_full": "The local queue is full",
        "request_cancelled": "The local request wait was cancelled",
        "authentication_failed": "Authentication failed",
        "credential_missing": "Required authentication information is missing",
        "authorization_rejected": "The requested operation is not authorized",
        "connectivity_unavailable": "Connectivity is unavailable",
        "peer_unreachable": "The peer is unreachable",
        "payload_transfer_failed": "The payload could not be transferred",
        "payload_too_large": "The payload exceeds the configured limit",
        "payload_integrity_failed": "Payload integrity validation failed",
        "delivery_timeout": "Delivery timed out",
        "duplicate_conflict": "Conflicting duplicate message data was rejected",
        "handler_error": "The application handler failed",
        "rpc_timeout": "The application response timed out",
        "shutdown_in_progress": "Shutdown is in progress",
        "shutdown_timeout": "Shutdown did not finish before the deadline",
        "core_error": "The AZTM core could not complete the operation",
    }
)

COMMAND_NAMES = frozenset(
    {
        "core.init", "core.capabilities", "core.status", "core.shutdown",
        "session.config.get", "session.config.update",
        "command.channel.register", "command.channel.clear", "command.cancel",
        "event.sink.register", "event.sink.clear", "event.sink.bind",
        "auth.login", "auth.connect", "auth.token_login", "auth.token_connect",
        "auth.installation_login", "auth.installation_connect", "auth.logout",
        "auth.agent_id",
        "mesh.list", "mesh.membership.refresh", "address.map.put",
        "address.map.remove", "address.map.list", "address.resolve",
        "message.send", "message.request", "message.reply", "delivery.next",
        "delivery.accept", "handler.register", "handler.unregister",
        "payload.open", "payload.write_chunk", "payload.finish", "payload.cancel",
        "payload.read", "payload.close", "payload.retain", "payload.release",
        "delivery.queue.status", "delivery.retry", "delivery.pause",
        "delivery.resume", "delivery.drop", "conversation.list",
        "conversation.status", "conversation.close", "policy.set", "policy.get",
        "policy.test", "diagnostics.peer_status",
        "diagnostics.connectivity_status", "diagnostics.snapshot",
        "diagnostics.logs.subscribe",
    }
)

RESULT_TYPES = frozenset(
    {
        "empty", "capabilities", "status", "config", "auth", "agent_id",
        "mesh_list", "address_mappings", "address_resolution", "send",
        "response", "event", "delivery_queue_status", "payload_handle",
        "conversation_status", "conversation_list", "policy", "peer_status",
        "connectivity_status", "diagnostic_snapshot", "core_init",
        "completion_channel", "event_sink",
    }
)

EVENT_NAMES = frozenset(
    {
        "session.state_changed", "connectivity.state_changed", "peer.reachable",
        "peer.unreachable", "message.received", "message.queued",
        "message.retried", "message.deduplicated", "delivery.resumed",
        "delivery.replayed",
        "payload.transfer_started", "payload.transfer_progress",
        "payload.transfer_completed", "payload.transfer_failed",
        "policy.rejected", "rpc.timeout", "command.queue_full",
        "event.queue_full", "core.error", "diagnostics.log",
    }
)

PUBLIC_ERROR_CODES = frozenset(PUBLIC_ERROR_MESSAGES)
PUBLIC_ERROR_STAGES = frozenset(
    {"sdk", "command", "auth", "policy", "connectivity", "delivery", "payload",
     "handler", "rpc", "shutdown"}
)


def _failure(code: str, message: str, **details: Any) -> NativeError:
    return NativeError(CYNAPSA_STATUS_V1_ERROR, code, message, details)


def _utf8_length(value: str) -> int:
    try:
        return len(value.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise ValueError("a string contains an invalid Unicode surrogate") from exc


def _bounded_string(
    value: object,
    field: str,
    *,
    maximum: int = MAX_IDENTIFIER_BYTES,
    allow_empty: bool = False,
) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        requirement = "a string" if allow_empty else "a nonempty string"
        raise ValueError(f"{field} must be {requirement}")
    if _utf8_length(value) > maximum:
        raise ValueError(f"{field} exceeds its {maximum}-byte limit")
    return value


def _validate_agent_id(
    value: object,
    field: str,
    *,
    allow_empty: bool = False,
) -> str:
    """Validate one transport-opaque public AgentID without parsing its syntax."""

    if not isinstance(value, str) or (not allow_empty and not value):
        requirement = "a string" if allow_empty else "a nonempty string"
        raise ValueError(f"{field} must be {requirement}")
    if not value:
        return value
    if _utf8_length(value) > MAX_AGENT_ID_BYTES:
        raise ValueError(f"{field} exceeds its {MAX_AGENT_ID_BYTES}-byte limit")
    if "\x00" in value:
        raise ValueError(f"{field} contains NUL")
    if any(unicodedata.category(character) == "Cc" for character in value):
        raise ValueError(f"{field} contains a Unicode control character")
    return value


def _exact(value: object, fields: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError(f"{label} has unknown or missing fields")
    if not all(isinstance(field, str) for field in value):
        raise ValueError(f"{label} has a non-string field name")
    return value


def _exact_mapping(
    value: object, fields: Sequence[str], label: str
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be a mapping")
    keys = tuple(value.keys())
    if not all(isinstance(key, str) for key in keys):
        raise ValueError(f"{label} has a non-string field name")
    if set(keys) != set(fields):
        raise ValueError(f"{label} has unknown or missing fields")
    return {field: value[field] for field in fields}


def _optional_mapping(
    value: object, fields: Sequence[str], label: str
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be a mapping")
    keys = tuple(value.keys())
    if not all(isinstance(key, str) for key in keys):
        raise ValueError(f"{label} has a non-string field name")
    if not set(keys) <= set(fields):
        raise ValueError(f"{label} has unknown fields")
    return {field: value[field] for field in fields if field in value}


def _integer(
    value: object, field: str, *, minimum: int = 0, maximum: int = 2**64 - 1
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{field} is outside its integer range")
    return value


def _boolean(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field} must be a boolean")
    return value


def _strict_json_object(raw: bytes) -> dict[str, Any]:
    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON field: {key}")
            result[key] = value
        return result

    def reject_constant(value: str) -> Any:
        raise ValueError(f"invalid JSON number: {value}")

    value = json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=object_pairs,
        parse_constant=reject_constant,
    )
    if not isinstance(value, dict):
        raise ValueError("the JSON document is not an object")
    return value


def _validate_json_value(value: object, *, depth: int = 0) -> None:
    if depth > 32:
        raise ValueError("args exceeds the maximum JSON nesting depth")
    if value is None or isinstance(value, (str, bool)):
        if isinstance(value, str):
            _utf8_length(value)
        return
    if isinstance(value, int) and not isinstance(value, bool):
        if value < -(2**63) or value > 2**64 - 1:
            raise ValueError("args contains an integer outside the ABI range")
        return
    if isinstance(value, float):
        raise ValueError("args contains a floating-point value")
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("args contains a non-string object key")
            _utf8_length(key)
            _validate_json_value(item, depth=depth + 1)
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            _validate_json_value(item, depth=depth + 1)
        return
    raise ValueError(f"args contains a non-JSON value of type {type(value).__name__}")


_EMPTY_COMMANDS = frozenset(
    {
        "core.init", "core.capabilities", "core.status", "core.shutdown",
        "session.config.get", "auth.logout", "auth.agent_id", "mesh.list",
        "mesh.membership.refresh", "address.map.list", "delivery.next",
        "delivery.queue.status", "delivery.pause", "delivery.resume",
        "payload.open", "conversation.list", "policy.get",
        "diagnostics.connectivity_status", "diagnostics.snapshot",
    }
)
_PAYLOAD_HANDLE_COMMANDS = frozenset(
    {"payload.finish", "payload.cancel", "payload.close", "payload.retain", "payload.release"}
)
_MESSAGE_ID_COMMANDS = frozenset({"delivery.retry", "delivery.drop"})
_CONVERSATION_ID_COMMANDS = frozenset({"conversation.status", "conversation.close"})
_HANDLER_COMMANDS = frozenset({"handler.register", "handler.unregister"})
_TOKEN_RE = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")
_ENROLLMENT_TOKEN_RE = re.compile(
    r"^cpsa_e1\.([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.([A-Za-z0-9_-]{43})$"
)
_TIMESTAMP_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$"
)


def _canonical_handle(value: object, field: str, prefix: str, size: int) -> str:
    handle = _bounded_string(value, field)
    if not handle.startswith(prefix):
        raise ValueError(f"{field} has the wrong class")
    encoded = handle[len(prefix) :]
    try:
        decoded = base64.b64decode(
            encoded + "=" * (-len(encoded) % 4), altchars=b"-_", validate=True
        )
    except (ValueError, binascii.Error) as exc:
        raise ValueError(f"{field} has invalid base64url") from exc
    canonical = base64.urlsafe_b64encode(decoded).rstrip(b"=").decode("ascii")
    if len(decoded) != size or canonical != encoded:
        raise ValueError(f"{field} has an invalid opaque shape")
    return handle


def _valid_dns_hostname(value: str) -> bool:
    if not value or len(value) > 253 or value.endswith("."):
        return False
    for label in value.split("."):
        if (
            not 1 <= len(label) <= 63
            or label.startswith("-")
            or label.endswith("-")
            or any(not (character.isascii() and (character.isalnum() or character == "-")) for character in label)
        ):
            return False
    return True


def _validate_mesh_endpoint(value: object) -> str:
    endpoint = _bounded_string(
        value, "mesh_endpoint", maximum=MAX_MESH_ENDPOINT_BYTES
    )
    if (
        endpoint.strip() != endpoint
        or any(ord(character) < 0x21 or ord(character) > 0x7E for character in endpoint)
        or any(character in endpoint for character in "@/?#")
        or "://" in endpoint
    ):
        raise ValueError("mesh_endpoint is invalid")
    bracketed = endpoint.startswith("[")
    if bracketed:
        closing = endpoint.find("]")
        if closing <= 1 or closing + 1 >= len(endpoint) or endpoint[closing + 1] != ":":
            raise ValueError("mesh_endpoint is invalid")
        host, port_text = endpoint[1:closing], endpoint[closing + 2 :]
        if "%" in host:
            raise ValueError("mesh_endpoint is invalid")
        try:
            parsed_host = ipaddress.ip_address(host)
            if (
                not isinstance(parsed_host, ipaddress.IPv6Address)
                or parsed_host.ipv4_mapped is not None
            ):
                raise ValueError("mesh_endpoint is invalid")
        except ValueError as exc:
            raise ValueError("mesh_endpoint is invalid") from exc
    else:
        if endpoint.count(":") != 1:
            raise ValueError("mesh_endpoint is invalid")
        host, port_text = endpoint.rsplit(":", 1)
        try:
            parsed_ip = ipaddress.ip_address(host)
        except ValueError:
            if not _valid_dns_hostname(host):
                raise ValueError("mesh_endpoint is invalid")
        else:
            if not isinstance(parsed_ip, ipaddress.IPv4Address):
                raise ValueError("mesh_endpoint is invalid")
    if not port_text or not port_text.isascii() or not port_text.isdecimal():
        raise ValueError("mesh_endpoint is invalid")
    if not 1 <= int(port_text) <= 65_535:
        raise ValueError("mesh_endpoint is invalid")
    if bracketed:
        canonical_host = parsed_host.compressed
        return f"[{canonical_host}]:{int(port_text)}"
    try:
        parsed_host = ipaddress.ip_address(host)
    except ValueError:
        canonical_host = host.lower()
    else:
        canonical_host = str(parsed_host)
    return f"{canonical_host}:{int(port_text)}"


def _validate_enrollment_token(value: object) -> str:
    token = _bounded_string(value, "token", maximum=MAX_AUTH_TOKEN_BYTES)
    match = _ENROLLMENT_TOKEN_RE.fullmatch(token)
    if match is None or len(token.encode("utf-8")) != MAX_AUTH_TOKEN_BYTES:
        raise ValueError("token is invalid")
    secret = match.group(2)
    try:
        decoded = base64.urlsafe_b64decode(secret + "=")
    except (ValueError, binascii.Error) as exc:
        raise ValueError("token is invalid") from exc
    canonical = base64.urlsafe_b64encode(decoded).rstrip(b"=").decode("ascii")
    if len(decoded) != 32 or canonical != secret:
        raise ValueError("token is invalid")
    return token


def _validate_profile_id(value: object) -> str:
    profile_id = _bounded_string(
        value, "profile_id", maximum=MAX_PROFILE_ID_BYTES
    )
    encoded = profile_id.encode("utf-8")
    if (
        profile_id in {".", ".."}
        or "/" in profile_id
        or "\\" in profile_id
        or any(byte <= 0x20 or byte == 0x7F for byte in encoded)
    ):
        raise ValueError("profile_id is invalid")
    return profile_id


def _new_public_identifier(prefix: str) -> str:
    """Return a 256-bit, ABI-bounded identifier controlled only by the SDK."""

    return f"{prefix}_{secrets.token_urlsafe(32)}"


def _validate_application_url(value: object, *, origin_only: bool) -> str:
    field = "virtual_origin" if origin_only else "url"
    raw = _bounded_string(value, field, maximum=MAX_PUBLIC_STRING_BYTES)
    if raw.strip() != raw:
        raise ValueError(f"{field} is invalid")
    try:
        parsed = urlsplit(raw)
        port = parsed.port
    except ValueError as exc:
        raise ValueError(f"{field} is invalid") from exc
    if (
        parsed.scheme.lower() not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or not parsed.hostname
        or not _valid_dns_hostname(parsed.hostname)
        or parsed.netloc.endswith(":")
        or "[" in parsed.netloc
        or "]" in parsed.netloc
        or (port is not None and not 1 <= port <= 65_535)
        or (origin_only and (parsed.path or parsed.query))
    ):
        raise ValueError(f"{field} is invalid")
    path = parsed.path or "/"
    # Go validates url.URL.EscapedPath(), which rejects malformed escapes and
    # applies the path bound after non-ASCII code points have been escaped.
    for index, character in enumerate(path):
        if character == "%" and (
            index + 2 >= len(path)
            or any(item not in "0123456789abcdefABCDEF" for item in path[index + 1:index + 3])
        ):
            raise ValueError(f"{field} is invalid")
    escaped_path = quote(path, safe="/%:@!$&'()*+,;=-._~")
    _validate_path(escaped_path, field="url.path")
    return raw


def _validate_path(value: object, *, field: str = "path", allow_empty: bool = False) -> str:
    path = _bounded_string(
        value, field, maximum=MAX_PATH_BYTES, allow_empty=allow_empty
    )
    if path:
        if not path.startswith("/") or "?" in path or "#" in path:
            raise ValueError(f"{field} is invalid")
    return path


def _canonical_headers(value: object) -> list[dict[str, str]]:
    if not isinstance(value, (list, tuple)) or len(value) > MAX_HEADER_COUNT:
        raise ValueError("headers is not a bounded array")
    output: list[dict[str, str]] = []
    total = 0
    for item in value:
        header = _exact_mapping(item, ("name", "value"), "header")
        name = _bounded_string(header["name"], "header.name")
        text = _bounded_string(
            header["value"], "header.value", maximum=MAX_PUBLIC_STRING_BYTES,
            allow_empty=True,
        )
        if _TOKEN_RE.fullmatch(name) is None or "\r" in text or "\n" in text:
            raise ValueError("header is invalid")
        total += _utf8_length(name) + _utf8_length(text)
        if total > MAX_INLINE_PAYLOAD_BYTES:
            raise ValueError("headers exceed the canonical payload limit")
        output.append({"name": name, "value": text})
    return output


def _canonical_payload(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("payload must be a mapping")
    keys = tuple(value.keys())
    if len(keys) != 1 or not isinstance(keys[0], str):
        raise ValueError("payload must contain exactly one variant")
    variant = keys[0]
    raw = value[variant]
    if variant == "payload_handle":
        return {variant: _canonical_handle(raw, "payload_handle", "payh_", 32)}
    if variant == "native":
        body = _exact_mapping(raw, ("content_type", "path", "body"), "native payload")
        content_type = _bounded_string(
            body["content_type"], "content_type", allow_empty=True
        )
        if "\r" in content_type or "\n" in content_type:
            raise ValueError("content_type is invalid")
        path = _validate_path(body["path"])
        encoded_body = _bounded_base64(body["body"], "body", MAX_INLINE_PAYLOAD_BYTES)
        if _utf8_length(content_type) + _utf8_length(path) + len(encoded_body[1]) > MAX_INLINE_PAYLOAD_BYTES:
            raise ValueError("payload exceeds the inline canonical limit")
        return {variant: {"content_type": content_type, "path": path, "body": encoded_body[0]}}
    if variant == "http_request":
        body = _exact_mapping(
            raw, ("method", "path", "query", "headers", "body"),
            "http_request payload",
        )
        method = _bounded_string(body["method"], "method", maximum=32)
        if _TOKEN_RE.fullmatch(method) is None:
            raise ValueError("method is invalid")
        path = _validate_path(body["path"])
        query = _bounded_string(
            body["query"], "query", maximum=MAX_PUBLIC_STRING_BYTES,
            allow_empty=True,
        )
        headers = _canonical_headers(body["headers"])
        encoded_body = _bounded_base64(body["body"], "body", MAX_INLINE_PAYLOAD_BYTES)
        total = sum(_utf8_length(item["name"]) + _utf8_length(item["value"]) for item in headers)
        total += _utf8_length(method) + _utf8_length(path) + _utf8_length(query) + len(encoded_body[1])
        if total > MAX_INLINE_PAYLOAD_BYTES:
            raise ValueError("payload exceeds the inline canonical limit")
        return {variant: {"method": method, "path": path, "query": query, "headers": headers, "body": encoded_body[0]}}
    if variant == "http_response":
        if not isinstance(raw, Mapping) or set(raw) not in (
            {"status_code", "reason", "headers", "body"},
            {"status_code", "reason", "headers", "body", "error"},
        ):
            raise ValueError("http_response payload has unknown or missing fields")
        body = dict(raw)
        status_code = _integer(body["status_code"], "status_code", minimum=100, maximum=599)
        reason = _bounded_string(
            body["reason"], "reason", maximum=MAX_IDENTIFIER_BYTES,
            allow_empty=True,
        )
        if "\r" in reason or "\n" in reason:
            raise ValueError("reason is invalid")
        headers = _canonical_headers(body["headers"])
        encoded_body = _bounded_base64(body["body"], "body", MAX_INLINE_PAYLOAD_BYTES)
        total = sum(_utf8_length(item["name"]) + _utf8_length(item["value"]) for item in headers)
        total += _utf8_length(reason) + len(encoded_body[1])
        error = None
        if "error" in body:
            if status_code < 400:
                raise ValueError("application error metadata requires a 4xx or 5xx status")
            error = _canonical_application_error(body["error"])
            total += sum(_utf8_length(error[field]) for field in ("code", "detail", "details_json"))
        if total > MAX_INLINE_PAYLOAD_BYTES:
            raise ValueError("payload exceeds the inline canonical limit")
        response = {"status_code": status_code, "reason": reason, "headers": headers, "body": encoded_body[0]}
        if error is not None:
            response["error"] = error
        return {variant: response}
    raise ValueError("payload has an unknown variant")


def _canonical_application_error(value: object) -> dict[str, str]:
    fields = _exact_mapping(
        value, ("code", "detail", "details_json"), "application error"
    )
    code = _bounded_string(fields["code"], "application error code")
    detail = _bounded_string(
        fields["detail"], "application error detail", maximum=MAX_PUBLIC_STRING_BYTES
    )
    details_json = _bounded_string(
        fields["details_json"],
        "application error details_json",
        maximum=MAX_PUBLIC_STRING_BYTES,
    )
    details = _strict_json_object(details_json.encode("utf-8"))
    _validate_application_error_json(details)
    return {"code": code, "detail": detail, "details_json": details_json}


def _normalized_unicode_scalar_string(value: str) -> str:
    output: list[str] | None = None
    index = 0
    while index < len(value):
        codepoint = ord(value[index])
        if 0xD800 <= codepoint <= 0xDBFF:
            if index + 1 >= len(value):
                raise ValueError(
                    "application error details contain an unpaired surrogate"
                )
            low = ord(value[index + 1])
            if not 0xDC00 <= low <= 0xDFFF:
                raise ValueError(
                    "application error details contain an unpaired surrogate"
                )
            if output is None:
                output = list(value[:index])
            output.append(
                chr(0x10000 + ((codepoint - 0xD800) << 10) + low - 0xDC00)
            )
            index += 2
            continue
        if 0xDC00 <= codepoint <= 0xDFFF:
            raise ValueError(
                "application error details contain an unpaired surrogate"
            )
        if output is not None:
            output.append(value[index])
        index += 1
    return value if output is None else "".join(output)


def _validate_application_error_json(value: object, *, depth: int = 0) -> None:
    if depth > 32:
        raise ValueError("application error details exceed the maximum JSON nesting depth")
    if value is None or type(value) in {bool, int}:
        return
    if type(value) is str:
        _normalized_unicode_scalar_string(value)
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("application error details contain a non-finite number")
        return
    if isinstance(value, Mapping):
        normalized_keys: set[str] = set()
        for key, item in value.items():
            if type(key) is not str:
                raise ValueError("application error details contain a non-string key")
            normalized_key = _normalized_unicode_scalar_string(key)
            if normalized_key in normalized_keys:
                raise ValueError("application error details contain a duplicate key")
            normalized_keys.add(normalized_key)
            _validate_application_error_json(item, depth=depth + 1)
        return
    if isinstance(value, list):
        for item in value:
            _validate_application_error_json(item, depth=depth + 1)
        return
    raise ValueError("application error details contain a non-JSON value")


def _bounded_base64(value: object, field: str, maximum: int) -> tuple[str, bytes]:
    text = _bounded_string(
        value, field, maximum=((maximum + 2) // 3) * 4, allow_empty=True
    )
    try:
        decoded = base64.b64decode(text, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ValueError(f"{field} is not canonical base64") from exc
    if len(decoded) > maximum or base64.b64encode(decoded).decode("ascii") != text:
        raise ValueError(f"{field} is not canonical bounded base64")
    return text, decoded


def _canonical_policy_rules(value: object) -> list[dict[str, str]]:
    if not isinstance(value, (list, tuple)) or len(value) > MAX_POLICY_RULES:
        raise ValueError("rules is not a bounded array")
    output: list[dict[str, str]] = []
    for item in value:
        rule = _exact_mapping(item, ("action", "path", "agent_id"), "policy rule")
        action = _bounded_string(rule["action"], "action")
        if action not in {"allow", "deny"}:
            raise ValueError("policy action is invalid")
        path = _validate_path(rule["path"], allow_empty=True)
        agent_id = _validate_agent_id(
            rule["agent_id"], "agent_id", allow_empty=True
        )
        output.append({"action": action, "path": path, "agent_id": agent_id})
    return output


def _canonical_command_args(command_name: str, value: object) -> dict[str, Any]:
    if command_name in _EMPTY_COMMANDS:
        return _exact_mapping(value, (), "args")
    if command_name == "session.config.update":
        args = _optional_mapping(
            value,
            ("command_timeout_ms", "rpc_timeout_ms", "queue_limit", "payload_limit"),
            "args",
        )
        output: dict[str, Any] = {}
        for field in ("command_timeout_ms", "rpc_timeout_ms"):
            if field in args:
                output[field] = _integer(args[field], field, maximum=MAX_TIMEOUT_MS)
        if "queue_limit" in args:
            output["queue_limit"] = _integer(
                args["queue_limit"], "queue_limit", minimum=1,
                maximum=MAX_QUEUE_CAPACITY,
            )
        if "payload_limit" in args:
            output["payload_limit"] = _integer(
                args["payload_limit"], "payload_limit", minimum=1,
                maximum=MAX_CANONICAL_PAYLOAD_BYTES,
            )
        return output
    if command_name in {"command.channel.register", "event.sink.register"}:
        args = _exact_mapping(value, ("capacity",), "args")
        return {"capacity": _integer(args["capacity"], "capacity", minimum=1, maximum=MAX_QUEUE_CAPACITY)}
    if command_name == "command.channel.clear":
        args = _exact_mapping(value, ("channel_id",), "args")
        return {"channel_id": _bounded_string(args["channel_id"], "channel_id")}
    if command_name == "command.cancel":
        args = _exact_mapping(value, ("command_handle",), "args")
        return {"command_handle": _validate_command_handle(args["command_handle"])}
    if command_name in {"event.sink.clear", "event.sink.bind"}:
        args = _exact_mapping(value, ("sink_id",), "args")
        return {"sink_id": _bounded_string(args["sink_id"], "sink_id")}
    if command_name in {"auth.login", "auth.connect"}:
        args = _exact_mapping(
            value,
            ("mesh_endpoint", "username", "password", "mesh_id", "agent_instance_id"),
            "args",
        )
        return {
            "mesh_endpoint": _validate_mesh_endpoint(args["mesh_endpoint"]),
            "username": _validate_agent_id(args["username"], "username"),
            "password": _bounded_string(args["password"], "password", maximum=MAX_PUBLIC_STRING_BYTES),
            "mesh_id": _bounded_string(args["mesh_id"], "mesh_id"),
            "agent_instance_id": _bounded_string(args["agent_instance_id"], "agent_instance_id", allow_empty=True),
        }
    if command_name in {"auth.token_login", "auth.token_connect"}:
        args = _optional_mapping(value, ("token", "mesh_id", "profile_id"), "args")
        if "token" not in args or "mesh_id" not in args:
            raise ValueError("args has unknown or missing fields")
        output = {
            "token": _validate_enrollment_token(args["token"]),
            "mesh_id": _bounded_string(
                args["mesh_id"], "mesh_id", maximum=MAX_MESH_ID_BYTES
            ),
        }
        if "profile_id" in args:
            output["profile_id"] = _validate_profile_id(args["profile_id"])
        return output
    if command_name in {"auth.installation_login", "auth.installation_connect"}:
        args = _exact_mapping(value, ("profile_id", "mesh_id"), "args")
        return {
            "profile_id": _validate_profile_id(args["profile_id"]),
            "mesh_id": _bounded_string(
                args["mesh_id"], "mesh_id", maximum=MAX_MESH_ID_BYTES
            ),
        }
    if command_name == "address.map.put":
        args = _exact_mapping(value, ("virtual_origin", "recipient"), "args")
        return {
            "virtual_origin": _validate_application_url(args["virtual_origin"], origin_only=True),
            "recipient": _validate_agent_id(args["recipient"], "recipient"),
        }
    if command_name == "address.map.remove":
        args = _exact_mapping(value, ("virtual_origin",), "args")
        return {"virtual_origin": _validate_application_url(args["virtual_origin"], origin_only=True)}
    if command_name == "address.resolve":
        args = _exact_mapping(value, ("url",), "args")
        return {"url": _validate_application_url(args["url"], origin_only=False)}
    if command_name in {"message.send", "message.request"}:
        fields = ("to", "payload", "ttl_ms") if command_name == "message.request" else ("to", "payload")
        args = _exact_mapping(value, fields, "args")
        output = {
            "to": _validate_agent_id(args["to"], "to"),
            "payload": _canonical_payload(args["payload"]),
        }
        if command_name == "message.request":
            output["ttl_ms"] = _integer(args["ttl_ms"], "ttl_ms", maximum=MAX_TIMEOUT_MS)
        return output
    if command_name == "message.reply":
        args = _exact_mapping(value, ("request_handle", "payload"), "args")
        return {
            "request_handle": _canonical_handle(args["request_handle"], "request_handle", "reqh_", 32),
            "payload": _canonical_payload(args["payload"]),
        }
    if command_name == "delivery.accept":
        args = _exact_mapping(value, ("event_id",), "args")
        return {"event_id": _bounded_string(args["event_id"], "event_id")}
    if command_name in _MESSAGE_ID_COMMANDS:
        args = _exact_mapping(value, ("message_id",), "args")
        return {"message_id": _bounded_string(args["message_id"], "message_id")}
    if command_name in _HANDLER_COMMANDS:
        args = _exact_mapping(value, ("path",), "args")
        return {"path": _validate_path(args["path"])}
    if command_name == "payload.write_chunk":
        args = _exact_mapping(value, ("handle", "chunk"), "args")
        chunk, _ = _bounded_base64(args["chunk"], "chunk", MAX_PAYLOAD_CHUNK_BYTES)
        return {"handle": _canonical_handle(args["handle"], "handle", "payh_", 32), "chunk": chunk}
    if command_name in _PAYLOAD_HANDLE_COMMANDS:
        args = _exact_mapping(value, ("handle",), "args")
        return {"handle": _canonical_handle(args["handle"], "handle", "payh_", 32)}
    if command_name == "payload.read":
        args = _exact_mapping(value, ("handle", "offset", "limit"), "args")
        offset = _integer(args["offset"], "offset", maximum=MAX_CANONICAL_PAYLOAD_BYTES)
        limit = _integer(args["limit"], "limit", maximum=MAX_PAYLOAD_CHUNK_BYTES)
        if offset + limit > MAX_CANONICAL_PAYLOAD_BYTES:
            raise ValueError("payload range exceeds the canonical payload limit")
        return {"handle": _canonical_handle(args["handle"], "handle", "payh_", 32), "offset": offset, "limit": limit}
    if command_name in _CONVERSATION_ID_COMMANDS:
        args = _exact_mapping(value, ("conversation_id",), "args")
        return {"conversation_id": _bounded_string(args["conversation_id"], "conversation_id")}
    if command_name == "policy.set":
        args = _exact_mapping(value, ("rules",), "args")
        return {"rules": _canonical_policy_rules(args["rules"])}
    if command_name == "policy.test":
        args = _exact_mapping(value, ("input",), "args")
        nested = _exact_mapping(args["input"], ("to", "payload"), "policy input")
        return {
            "input": {
                "to": _validate_agent_id(nested["to"], "to"),
                "payload": _canonical_payload(nested["payload"]),
            }
        }
    if command_name == "diagnostics.peer_status":
        args = _exact_mapping(value, ("peer",), "args")
        return {"peer": _validate_agent_id(args["peer"], "peer")}
    if command_name == "diagnostics.logs.subscribe":
        args = _exact_mapping(value, ("enabled",), "args")
        return {"enabled": _boolean(args["enabled"], "enabled")}
    raise ValueError("command_name is not in the frozen V1 catalog")


def encode_command(
    command_name: str,
    sdk_session_id: str,
    args: Mapping[str, Any] | None = None,
    *,
    command_id: str | None = None,
) -> tuple[str, bytes]:
    """Validate and deterministically encode one exact ABI-v1 command envelope."""

    try:
        name = _bounded_string(command_name, "command_name")
        if name not in COMMAND_NAMES:
            raise ValueError("command_name is not in the frozen V1 catalog")
        session_id = _bounded_string(sdk_session_id, "sdk_session_id")
        identifier = _bounded_string(
            _new_public_identifier("cmd") if command_id is None else command_id,
            "command_id",
        )
        command_args = _canonical_command_args(name, {} if args is None else args)
        document = {
            "abi_version": ABI_VERSION,
            "command_id": identifier,
            "command_name": name,
            "sdk_session_id": session_id,
            "args": command_args,
        }
        encoded = json.dumps(
            document,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=False,
            allow_nan=False,
        ).encode("utf-8")
        if len(encoded) > MAX_ABI_INPUT_BYTES:
            raise ValueError("command document exceeds the 2 MiB ABI input limit")
        return identifier, encoded
    except NativeError:
        raise
    except Exception as exc:
        # Validation diagnostics intentionally identify no caller-supplied value;
        # command arguments may contain passwords, payloads, cookies, or tokens.
        raise _failure("invalid_input", "The command input is invalid") from exc


def encode_cancel(command_handle: str) -> bytes:
    try:
        _validate_command_handle(command_handle)
    except ValueError as exc:
        raise _failure("invalid_handle", "The local command handle is invalid") from exc
    return json.dumps(
        {"abi_version": ABI_VERSION, "handle": command_handle},
        separators=(",", ":"),
    ).encode("ascii")


def _validate_command_handle(value: object) -> str:
    return _canonical_handle(value, "command_handle", "cmdh_", 40)


def _public_error(value: object, *, failure_kind: str) -> NativeError:
    body = value
    if not isinstance(body, dict):
        raise ValueError("error is not an object")
    required = {"code", "message", "retryable", "stage", "local_or_remote"}
    if set(body) not in (required, required | {"diagnostic_id"}):
        raise ValueError("error has unknown or missing fields")
    code = _bounded_string(body["code"], "error.code")
    message = _bounded_string(
        body["message"], "error.message", maximum=MAX_PUBLIC_STRING_BYTES
    )
    retryable = _boolean(body["retryable"], "error.retryable")
    stage = _bounded_string(body["stage"], "error.stage")
    location = _bounded_string(body["local_or_remote"], "error.local_or_remote")
    if code not in PUBLIC_ERROR_CODES or stage not in PUBLIC_ERROR_STAGES:
        raise ValueError("error contains an unknown code or stage")
    if message != PUBLIC_ERROR_MESSAGES[code]:
        raise ValueError("error message is not the boundary-owned template")
    if location not in {"local", "remote"}:
        raise ValueError("error contains an unknown location")
    details: dict[str, Any] = {
        "retryable": retryable,
        "stage": stage,
        "local_or_remote": location,
        "failure_kind": failure_kind,
    }
    if "diagnostic_id" in body:
        diagnostic_id = _bounded_string(body["diagnostic_id"], "error.diagnostic_id")
        try:
            decoded = base64.b64decode(
                diagnostic_id + "=" * (-len(diagnostic_id) % 4),
                altchars=b"-_",
                validate=True,
            )
        except (ValueError, binascii.Error) as exc:
            raise ValueError("diagnostic_id has invalid base64url") from exc
        if len(decoded) != 32 or base64.urlsafe_b64encode(decoded).rstrip(b"=").decode() != diagnostic_id:
            raise ValueError("diagnostic_id has an invalid opaque shape")
        details["diagnostic_id"] = diagnostic_id
    return NativeError(CYNAPSA_STATUS_V1_ERROR, code, message, details)


@dataclass(frozen=True, slots=True)
class Admission:
    command_id: str
    command_handle: str | None
    accepted: bool
    error: NativeError | None


def decode_admission(raw: bytes) -> Admission:
    try:
        body = _strict_json_object(raw)
        if body.get("abi_version") != ABI_VERSION or isinstance(body.get("abi_version"), bool):
            raise ValueError("admission has an invalid ABI version")
        accepted = _boolean(body.get("accepted"), "accepted")
        command_id = _bounded_string(body.get("command_id"), "command_id")
        if accepted:
            _exact(body, {"abi_version", "command_id", "command_handle", "accepted"}, "admission")
            return Admission(command_id, _validate_command_handle(body["command_handle"]), True, None)
        _exact(body, {"abi_version", "command_id", "accepted", "error"}, "admission")
        return Admission(command_id, None, False, _public_error(body["error"], failure_kind="admission_rejected"))
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise _failure(
            "native_admission_decode_failed",
            "The native core returned a nonconforming command admission",
            reason=str(exc),
        ) from exc


def _string_list(value: object, field: str, *, allowed: frozenset[str] | None = None) -> None:
    if not isinstance(value, list) or len(value) > MAX_QUEUE_CAPACITY:
        raise ValueError(f"{field} is not a bounded array")
    for item in value:
        text = _bounded_string(item, field)
        if allowed is not None and text not in allowed:
            raise ValueError(f"{field} contains an unknown value")


def _validate_status(value: object) -> None:
    body = _exact(
        value,
        {"lifecycle", "connectivity", "personality", "agent_id", "mesh_id",
         "mesh_endpoint", "queued_message_count"},
        "status",
    )
    if body["lifecycle"] not in {"created", "connecting", "ready", "degraded", "closing", "closed", "failed"}:
        raise ValueError("status.lifecycle is invalid")
    if body["connectivity"] not in {"unknown", "available", "degraded", "unavailable"}:
        raise ValueError("status.connectivity is invalid")
    if body["personality"] not in {"unset", "http_bridge", "native"}:
        raise ValueError("status.personality is invalid")
    _validate_agent_id(body["agent_id"], "agent_id", allow_empty=True)
    _bounded_string(body["mesh_id"], "mesh_id", allow_empty=True)
    _bounded_string(body["mesh_endpoint"], "mesh_endpoint", maximum=MAX_PUBLIC_STRING_BYTES, allow_empty=True)
    _integer(body["queued_message_count"], "queued_message_count", maximum=MAX_QUEUE_CAPACITY)


def _validate_base64(value: object, field: str, maximum: int) -> bytes:
    return _bounded_base64(value, field, maximum)[1]


def _validate_payload(value: object) -> None:
    _canonical_payload(value)


def _validate_timestamp(value: object) -> None:
    text = _bounded_string(value, "created_at", maximum=MAX_PUBLIC_STRING_BYTES)
    match = _TIMESTAMP_RE.fullmatch(text)
    if match is None:
        raise ValueError("created_at is not canonical UTC RFC3339")
    fraction = match.group(7) or ""
    if fraction.endswith("0"):
        raise ValueError("created_at is not canonical UTC RFC3339")
    year, month, day, hour, minute, second = (
        int(component) for component in match.groups()[:6]
    )
    try:
        datetime(year, month, day, hour, minute, second)
    except ValueError as exc:
        raise ValueError("created_at is not a valid timestamp") from exc


_MESSAGE_EVENT_STATES = {
    "message.queued": "queued",
    "message.retried": "queued",
    "message.deduplicated": "ready",
    "delivery.resumed": "ready",
    "delivery.replayed": "queued",
}
_PAYLOAD_EVENT_STATES = {
    "payload.transfer_started": "started",
    "payload.transfer_progress": "progress",
    "payload.transfer_completed": "completed",
    "payload.transfer_failed": "failed",
}
_DIAGNOSTIC_LOG_MESSAGES = {
    "core_state": "Core state changed",
    "connectivity": "Connectivity state changed",
    "queue_pressure": "A local queue reached capacity",
    "payload_transfer": "Payload transfer state changed",
}


def _validate_event(value: object) -> None:
    body = _exact(
        value,
        {"abi_version", "event_id", "event_name", "created_at", "payload"},
        "event",
    )
    if body["abi_version"] != ABI_VERSION or isinstance(body["abi_version"], bool):
        raise ValueError("event has an invalid ABI version")
    _bounded_string(body["event_id"], "event_id")
    name = _bounded_string(body["event_name"], "event_name")
    if name not in EVENT_NAMES:
        raise ValueError("event has an unknown event_name")
    _validate_timestamp(body["created_at"])
    payload = body["payload"]
    lifecycles = {"created", "connecting", "ready", "degraded", "closing", "closed", "failed"}
    connectivity = {"unknown", "available", "degraded", "unavailable"}
    if name == "session.state_changed":
        fields = _exact(payload, {"previous", "current"}, "event payload")
        if fields["previous"] not in lifecycles or fields["current"] not in lifecycles:
            raise ValueError("event lifecycle is invalid")
        return
    if name == "connectivity.state_changed":
        fields = _exact(payload, {"previous", "current"}, "event payload")
        if fields["previous"] not in connectivity or fields["current"] not in connectivity:
            raise ValueError("event connectivity is invalid")
        return
    if name in {"peer.reachable", "peer.unreachable"}:
        fields = _exact(payload, {"peer", "reachable"}, "event payload")
        _validate_agent_id(fields["peer"], "peer")
        reachable = _boolean(fields["reachable"], "reachable")
        if reachable != (name == "peer.reachable"):
            raise ValueError("event reachability does not match event_name")
        return
    if name == "message.received":
        fields = _exact(
            payload,
            {"message_id", "conversation_id", "from_agent_id", "mesh_id", "mode", "request_handle", "payload"},
            "event payload",
        )
        for field in ("message_id", "conversation_id", "mesh_id"):
            _bounded_string(fields[field], field)
        _validate_agent_id(fields["from_agent_id"], "from_agent_id")
        mode = _bounded_string(fields["mode"], "mode")
        if mode not in {"msg", "rpc"}:
            raise ValueError("message mode is invalid")
        request_handle = _bounded_string(
            fields["request_handle"], "request_handle", allow_empty=True
        )
        if mode == "rpc":
            _canonical_handle(request_handle, "request_handle", "reqh_", 32)
        elif request_handle:
            raise ValueError("one-way message has a request handle")
        _validate_payload(fields["payload"])
        return
    if name in _MESSAGE_EVENT_STATES:
        fields = _exact(payload, {"message_id", "conversation_id", "state"}, "event payload")
        _bounded_string(fields["message_id"], "message_id")
        _bounded_string(fields["conversation_id"], "conversation_id")
        if fields["state"] != _MESSAGE_EVENT_STATES[name]:
            raise ValueError("message event state does not match event_name")
        return
    if name in _PAYLOAD_EVENT_STATES:
        fields = _exact(payload, {"handle", "state", "completed", "total"}, "event payload")
        _canonical_handle(fields["handle"], "handle", "payh_", 32)
        if fields["state"] != _PAYLOAD_EVENT_STATES[name]:
            raise ValueError("payload event state does not match event_name")
        completed = _integer(fields["completed"], "completed", maximum=MAX_CANONICAL_PAYLOAD_BYTES)
        total = _integer(fields["total"], "total", maximum=MAX_CANONICAL_PAYLOAD_BYTES)
        if completed > total:
            raise ValueError("payload event completed bytes exceed total")
        return
    if name == "policy.rejected":
        fields = _exact(payload, {"message_id", "peer", "path"}, "event payload")
        _bounded_string(fields["message_id"], "message_id")
        _validate_agent_id(fields["peer"], "peer")
        _validate_path(fields["path"])
        return
    if name == "rpc.timeout":
        fields = _exact(payload, {"command_id"}, "event payload")
        _bounded_string(fields["command_id"], "command_id")
        return
    if name in {"command.queue_full", "event.queue_full"}:
        fields = _exact(payload, {"queue", "capacity"}, "event payload")
        expected = "command" if name == "command.queue_full" else "event"
        if fields["queue"] != expected:
            raise ValueError("queue event does not match event_name")
        _integer(fields["capacity"], "capacity", minimum=1, maximum=MAX_QUEUE_CAPACITY)
        return
    if name == "core.error":
        fields = _exact(payload, {"error"}, "event payload")
        _public_error(fields["error"], failure_kind="event")
        return
    if name == "diagnostics.log":
        if not isinstance(payload, dict):
            raise ValueError("event payload is not an object")
        required = {"level", "code", "message"}
        if set(payload) not in (required, required | {"diagnostic_id"}):
            raise ValueError("diagnostic log has unknown or missing fields")
        if payload["level"] not in {"debug", "info", "warn", "error"}:
            raise ValueError("diagnostic log level is invalid")
        code = _bounded_string(payload["code"], "code")
        message = _bounded_string(
            payload["message"], "message", maximum=MAX_PUBLIC_STRING_BYTES
        )
        if code not in _DIAGNOSTIC_LOG_MESSAGES or message != _DIAGNOSTIC_LOG_MESSAGES[code]:
            raise ValueError("diagnostic log is not normalized")
        if "diagnostic_id" in payload:
            _canonical_handle(
                "diag_" + payload["diagnostic_id"],
                "diagnostic_id",
                "diag_",
                32,
            )
        return
    raise ValueError("event payload has no frozen validator")


def _validate_conversation(value: object) -> None:
    body = _exact(
        value,
        {"conversation_id", "mesh_id", "peer", "delivery_state",
         "queued_message_count", "blocked"},
        "conversation status",
    )
    for field in ("conversation_id", "mesh_id"):
        _bounded_string(body[field], field)
    _validate_agent_id(body["peer"], "peer")
    if body["delivery_state"] not in {"unknown", "ready", "queued", "blocked", "failed"}:
        raise ValueError("delivery_state is invalid")
    _integer(body["queued_message_count"], "queued_message_count", maximum=MAX_QUEUE_CAPACITY)
    _boolean(body["blocked"], "blocked")


def _validate_result(result_type: str, value: object) -> None:
    if result_type not in RESULT_TYPES:
        raise ValueError("completion has an unknown result_type")
    if result_type == "empty":
        _exact(value, set(), "empty result")
    elif result_type == "capabilities":
        body = _exact(value, {"commands", "features"}, "capabilities result")
        _string_list(body["commands"], "commands", allowed=COMMAND_NAMES)
        _string_list(body["features"], "features", allowed=frozenset({
            "native_messaging", "rpc", "http_bridge",
            "offline_delivery", "large_payloads", "payload_streaming_handles",
            "local_command_cancellation", "bounded_queues", "event_stream"}))
    elif result_type == "status":
        _validate_status(value)
    elif result_type == "config":
        body = _exact(value, {"command_timeout_ms", "rpc_timeout_ms", "queue_limit", "payload_limit"}, "config result")
        _integer(body["command_timeout_ms"], "command_timeout_ms", minimum=1, maximum=9_223_372_036_854)
        _integer(body["rpc_timeout_ms"], "rpc_timeout_ms", minimum=1, maximum=9_223_372_036_854)
        _integer(body["queue_limit"], "queue_limit", minimum=1, maximum=MAX_QUEUE_CAPACITY)
        _integer(body["payload_limit"], "payload_limit", minimum=1, maximum=MAX_CANONICAL_PAYLOAD_BYTES)
    elif result_type == "auth":
        required = {"agent_id", "mesh_id", "agent_instance_id", "personality"}
        extended = {
            "profile_id",
            "credential_expires_at",
            "offline_start_deadline",
            "offline_cold_start_target_seconds",
            "offline_target_satisfied",
            "policy_revision",
            "session_expiry_mode",
            "preparation_status",
        }
        if (
            not isinstance(value, Mapping)
            or not required <= set(value)
            or not set(value) <= required | extended
        ):
            raise ValueError("auth result has unknown or missing fields")
        body = dict(value)
        _validate_agent_id(body["agent_id"], "agent_id")
        _bounded_string(body["mesh_id"], "mesh_id")
        _bounded_string(body["agent_instance_id"], "agent_instance_id", allow_empty=True)
        if body["personality"] not in {"http_bridge", "native"}:
            raise ValueError("auth personality is invalid")
        present_extended = set(body) & extended
        if present_extended and "profile_id" not in body:
            raise ValueError("extended auth result requires profile_id")
        if "profile_id" in body:
            _validate_profile_id(body["profile_id"])
        for field in ("credential_expires_at", "offline_start_deadline"):
            if field in body:
                _validate_timestamp(body[field])
        if "offline_cold_start_target_seconds" in body:
            _integer(
                body["offline_cold_start_target_seconds"],
                "offline_cold_start_target_seconds",
                maximum=86_400,
            )
        if "offline_target_satisfied" in body:
            _boolean(body["offline_target_satisfied"], "offline_target_satisfied")
        if "policy_revision" in body:
            _integer(
                body["policy_revision"],
                "policy_revision",
                maximum=2**63 - 1,
            )
        if "session_expiry_mode" in body and body["session_expiry_mode"] not in {
            "continue",
            "disconnect",
        }:
            raise ValueError("session_expiry_mode is invalid")
        if "preparation_status" in body and body["preparation_status"] not in {
            "disabled",
            "preparing",
            "ready",
        }:
            raise ValueError("preparation_status is invalid")
    elif result_type in {"agent_id", "core_init", "event_sink"}:
        field = {"agent_id": "agent_id", "core_init": "sdk_session_id", "event_sink": "sink_id"}[result_type]
        body = _exact(value, {field}, f"{result_type} result")
        if result_type == "agent_id":
            _validate_agent_id(body[field], field)
        else:
            _bounded_string(body[field], field)
    elif result_type == "mesh_list":
        body = _exact(value, {"meshes"}, "mesh list result")
        if not isinstance(body["meshes"], list) or len(body["meshes"]) > MAX_QUEUE_CAPACITY:
            raise ValueError("meshes is not a bounded array")
        for item in body["meshes"]:
            mesh = _exact(item, {"mesh_id", "active"}, "mesh summary")
            _bounded_string(mesh["mesh_id"], "mesh_id")
            _boolean(mesh["active"], "active")
    elif result_type == "address_mappings":
        body = _exact(value, {"mappings"}, "address mappings result")
        if not isinstance(body["mappings"], list) or len(body["mappings"]) > MAX_QUEUE_CAPACITY:
            raise ValueError("mappings is not a bounded array")
        for item in body["mappings"]:
            mapping = _exact(item, {"virtual_origin", "recipient"}, "address mapping")
            _validate_application_url(mapping["virtual_origin"], origin_only=True)
            _validate_agent_id(mapping["recipient"], "recipient")
    elif result_type == "address_resolution":
        body = _exact(value, {"recipient", "path", "query"}, "address resolution")
        _validate_agent_id(body["recipient"], "recipient")
        _validate_path(body["path"])
        _bounded_string(body["query"], "query", maximum=MAX_PUBLIC_STRING_BYTES, allow_empty=True)
    elif result_type == "send":
        body = _exact(value, {"message_id", "conversation_id", "accepted"}, "send result")
        _bounded_string(body["message_id"], "message_id")
        _bounded_string(body["conversation_id"], "conversation_id")
        if _boolean(body["accepted"], "accepted") is not True:
            raise ValueError("send result is not accepted")
    elif result_type == "response":
        body = _exact(value, {"message_id", "conversation_id", "from_agent_id", "mesh_id", "payload"}, "response result")
        for field in ("message_id", "conversation_id", "mesh_id"):
            _bounded_string(body[field], field)
        _validate_agent_id(body["from_agent_id"], "from_agent_id")
        _validate_payload(body["payload"])
    elif result_type == "event":
        _validate_event(value)
    elif result_type == "delivery_queue_status":
        body = _exact(value, {"queued", "paused"}, "delivery queue status")
        _integer(body["queued"], "queued", maximum=MAX_QUEUE_CAPACITY)
        _boolean(body["paused"], "paused")
    elif result_type == "payload_handle":
        body = _exact(value, {"handle", "size", "eof", "chunk"}, "payload handle result")
        _canonical_handle(body["handle"], "handle", "payh_", 32)
        size = _integer(body["size"], "size", maximum=MAX_CANONICAL_PAYLOAD_BYTES)
        _boolean(body["eof"], "eof")
        if body["chunk"] is not None and len(_validate_base64(body["chunk"], "chunk", MAX_PAYLOAD_CHUNK_BYTES)) > size:
            raise ValueError("payload chunk exceeds payload size")
    elif result_type == "conversation_status":
        _validate_conversation(value)
    elif result_type == "conversation_list":
        body = _exact(value, {"conversations"}, "conversation list")
        if not isinstance(body["conversations"], list) or len(body["conversations"]) > MAX_QUEUE_CAPACITY:
            raise ValueError("conversations is not a bounded array")
        for item in body["conversations"]:
            _validate_conversation(item)
    elif result_type == "policy":
        body = _exact(value, {"rules", "allowed"}, "policy result")
        if not isinstance(body["rules"], list) or len(body["rules"]) > 1024:
            raise ValueError("rules is not a bounded array")
        for item in body["rules"]:
            rule = _exact(item, {"action", "path", "agent_id"}, "policy rule")
            if rule["action"] not in {"allow", "deny"}:
                raise ValueError("policy action is invalid")
            _validate_path(rule["path"], allow_empty=True)
            _validate_agent_id(rule["agent_id"], "agent_id", allow_empty=True)
        _boolean(body["allowed"], "allowed")
    elif result_type == "peer_status":
        body = _exact(value, {"peer", "connectivity", "reachable", "recovery_in_progress"}, "peer status")
        _validate_agent_id(body["peer"], "peer")
        if body["connectivity"] not in {"unknown", "available", "degraded", "unavailable"}:
            raise ValueError("peer connectivity is invalid")
        _boolean(body["reachable"], "reachable")
        _boolean(body["recovery_in_progress"], "recovery_in_progress")
    elif result_type == "connectivity_status":
        body = _exact(value, {"state"}, "connectivity status")
        if body["state"] not in {"unknown", "available", "degraded", "unavailable"}:
            raise ValueError("connectivity state is invalid")
    elif result_type == "diagnostic_snapshot":
        body = _exact(value, {"status", "command_queue_depth", "event_queue_depth", "peer_count", "queued_message_count", "pending_rpc_count", "payload_transfer_count"}, "diagnostic snapshot")
        _validate_status(body["status"])
        for field in set(body) - {"status"}:
            _integer(body[field], field, maximum=MAX_QUEUE_CAPACITY)
    elif result_type == "completion_channel":
        body = _exact(value, {"channel_id", "max_in_flight"}, "completion channel result")
        _bounded_string(body["channel_id"], "channel_id")
        _integer(body["max_in_flight"], "max_in_flight", minimum=1, maximum=MAX_QUEUE_CAPACITY)


def _freeze(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


@dataclass(frozen=True, slots=True)
class CommandCompletion:
    """One strictly decoded terminal command completion."""

    command_id: str
    ok: bool
    result_type: str | None
    result: Mapping[str, Any] | None
    error: NativeError | None


def decode_completion(raw: bytes) -> CommandCompletion:
    try:
        body = _strict_json_object(raw)
        if body.get("abi_version") != ABI_VERSION or isinstance(body.get("abi_version"), bool):
            raise ValueError("completion has an invalid ABI version")
        command_id = _bounded_string(body.get("command_id"), "command_id")
        ok = _boolean(body.get("ok"), "ok")
        if ok:
            _exact(body, {"abi_version", "command_id", "ok", "result_type", "result"}, "completion")
            result_type = _bounded_string(body["result_type"], "result_type")
            _validate_result(result_type, body["result"])
            result = _freeze(body["result"])
            assert isinstance(result, Mapping)
            return CommandCompletion(command_id, True, result_type, result, None)
        _exact(body, {"abi_version", "command_id", "ok", "error"}, "completion")
        error = _public_error(body["error"], failure_kind="command_completion")
        return CommandCompletion(command_id, False, None, None, error)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise _failure(
            "native_completion_decode_failed",
            "The native core returned a nonconforming command completion",
            reason=str(exc),
        ) from exc


class CommandFuture:
    """A thread-safe future completed by the core's selected delivery mode."""

    def __init__(
        self,
        command_id: str,
        command_name: str,
        cancel: Callable[[CommandFuture], bool],
    ) -> None:
        self._command_id = command_id
        self._command_name = command_name
        self._cancel_callback = cancel
        self._condition = threading.Condition()
        self._command_handle: str | None = None
        self._completion: CommandCompletion | None = None
        self._terminal_error: BaseException | None = None
        self._cancel_started = False

    @property
    def command_id(self) -> str:
        return self._command_id

    @property
    def command_name(self) -> str:
        return self._command_name

    def done(self) -> bool:
        with self._condition:
            return self._completion is not None or self._terminal_error is not None

    def cancelled(self) -> bool:
        with self._condition:
            return bool(
                self._completion is not None
                and self._completion.error is not None
                and self._completion.error.code == "request_cancelled"
            )

    def cancel(self) -> bool:
        """Request direct native cancellation using this admission's opaque handle."""

        return self._cancel_callback(self)

    def completion(self, timeout: float | None = None) -> CommandCompletion:
        """Wait synchronously and return the terminal completion, including failures."""

        deadline = None if timeout is None else time.monotonic() + _timeout_seconds(timeout)
        with self._condition:
            while self._completion is None and self._terminal_error is None:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("the command future did not complete before the timeout")
                self._condition.wait(remaining)
            if self._terminal_error is not None:
                raise self._terminal_error
            assert self._completion is not None
            return self._completion

    def result(self, timeout: float | None = None) -> CommandCompletion:
        """Wait synchronously; raise the normalized command error on failure."""

        completion = self.completion(timeout)
        if completion.error is not None:
            raise completion.error
        return completion

    def exception(self, timeout: float | None = None) -> BaseException | None:
        completion: CommandCompletion | None = None
        try:
            completion = self.completion(timeout)
        except BaseException as exc:
            return exc
        return completion.error

    async def wait_async(self, timeout: float | None = None) -> CommandCompletion:
        """Await a future while callback delivery or external polling progresses."""

        return await asyncio.to_thread(self.result, timeout)

    def __await__(self) -> Any:
        return self.wait_async().__await__()

    def _admit(self, command_handle: str) -> bool:
        with self._condition:
            if self._completion is not None or self._terminal_error is not None:
                return False
            self._command_handle = command_handle
            return True

    def _complete(self, completion: CommandCompletion) -> bool:
        with self._condition:
            if self._completion is not None or self._terminal_error is not None:
                return False
            self._completion = completion
            self._command_handle = None
            self._condition.notify_all()
            return True

    def _terminate(self, error: BaseException) -> bool:
        with self._condition:
            if self._completion is not None or self._terminal_error is not None:
                return False
            self._terminal_error = error
            self._command_handle = None
            self._condition.notify_all()
            return True

    def _claim_cancel(self) -> str | None:
        with self._condition:
            if self._completion is not None or self._terminal_error is not None:
                return None
            if self._cancel_started:
                return None
            if self._command_handle is None:
                raise _failure(
                    "command_not_admitted",
                    "The command does not have an accepted cancellation handle",
                )
            self._cancel_started = True
            return self._command_handle

    def _cancel_failed(self) -> None:
        with self._condition:
            if self._completion is None and self._terminal_error is None:
                self._cancel_started = False


def _timeout_seconds(value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise TypeError("timeout must be a nonnegative number of seconds or None")
    return float(value)
