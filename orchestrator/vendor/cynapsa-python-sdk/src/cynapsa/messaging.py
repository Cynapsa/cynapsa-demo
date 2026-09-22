"""Immutable native-messaging models and canonical payload conversion."""

from __future__ import annotations

import asyncio
import base64
import binascii
import codecs
import json
import math
import re
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Awaitable, TypeAlias

from cynapsa.http import HTTPRequestPayload, HTTPResponsePayload
from cynapsa.native.command import (
    MAX_IDENTIFIER_BYTES,
    MAX_INLINE_PAYLOAD_BYTES,
    MAX_PATH_BYTES,
    MAX_PUBLIC_STRING_BYTES,
)

_TOKEN_RE = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")
_DEFAULT_PATH = "/"
_BYTES_CONTENT_TYPE = "application/octet-stream"
_TEXT_CONTENT_TYPE = "text/plain; charset=utf-8"
_JSON_CONTENT_TYPE = "application/json"


class _PayloadTooLarge(ValueError):
    pass


def _utf8_bytes(value: object, field: str, *, maximum: int, empty: bool) -> bytes:
    if type(value) is not str or (not empty and not value):
        requirement = "a string" if empty else "a nonempty string"
        raise ValueError(f"{field} must be {requirement}")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError(f"{field} contains invalid Unicode") from exc
    if len(encoded) > maximum:
        raise ValueError(f"{field} exceeds its {maximum}-byte limit")
    return encoded


def _identifier(value: object, field: str) -> str:
    _utf8_bytes(value, field, maximum=MAX_IDENTIFIER_BYTES, empty=False)
    assert isinstance(value, str)
    return value


def _application_path(value: object) -> str:
    _utf8_bytes(value, "path", maximum=MAX_PATH_BYTES, empty=False)
    assert isinstance(value, str)
    if not value.startswith("/") or "?" in value or "#" in value:
        raise ValueError("path must be an absolute application path without query or fragment")
    return value


def _content_type(value: object) -> str:
    _utf8_bytes(value, "content_type", maximum=MAX_IDENTIFIER_BYTES, empty=True)
    assert isinstance(value, str)
    if "\r" in value or "\n" in value:
        raise ValueError("content_type must not contain CR or LF")
    return value


def _canonical_size(content_type: str, path: str, body: bytes) -> int:
    return len(content_type.encode("utf-8")) + len(path.encode("utf-8")) + len(body)


def _validate_json_value(value: object, *, depth: int = 0) -> None:
    if depth > 32:
        raise ValueError("JSON value exceeds the maximum nesting depth")
    if value is None or type(value) is bool or type(value) is int:
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("JSON value contains a non-finite number")
        return
    if type(value) is str:
        _utf8_bytes(value, "JSON string", maximum=2**63 - 1, empty=True)
        return
    if type(value) is list:
        for item in value:
            _validate_json_value(item, depth=depth + 1)
        return
    if type(value) is dict:
        for key, item in value.items():
            if type(key) is not str:
                raise TypeError("JSON object keys must be strings")
            _utf8_bytes(key, "JSON object key", maximum=2**63 - 1, empty=True)
            _validate_json_value(item, depth=depth + 1)
        return
    raise TypeError(f"unsupported native payload value: {type(value).__name__}")


def _canonical_json_body(value: object) -> bytes:
    _validate_json_value(value)
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


@dataclass(frozen=True, slots=True)
class NativePayload:
    """One owned, canonical native payload with exact immutable bytes."""

    content_type: str
    path: str
    body: bytes

    def __post_init__(self) -> None:
        content_type = _content_type(self.content_type)
        path = _application_path(self.path)
        if type(self.body) is not bytes:
            raise TypeError("body must be bytes")
        # Force a distinct immutable ownership snapshot even when the caller
        # supplied an existing bytes object.
        body = memoryview(self.body).tobytes()
        if _canonical_size(content_type, path, body) > MAX_INLINE_PAYLOAD_BYTES:
            raise _PayloadTooLarge(
                "native payload exceeds the inline limit; payload handles are unavailable"
            )
        object.__setattr__(self, "content_type", content_type)
        object.__setattr__(self, "path", path)
        object.__setattr__(self, "body", body)

    @classmethod
    def from_bytes(
        cls,
        value: bytes,
        *,
        path: str = _DEFAULT_PATH,
        content_type: str = _BYTES_CONTENT_TYPE,
    ) -> NativePayload:
        if type(value) is not bytes:
            raise TypeError("value must be bytes")
        return cls(content_type, path, value)

    @classmethod
    def from_text(
        cls,
        value: str,
        *,
        path: str = _DEFAULT_PATH,
        content_type: str = _TEXT_CONTENT_TYPE,
    ) -> NativePayload:
        if type(value) is not str:
            raise TypeError("value must be str")
        try:
            body = value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise ValueError("text payload contains invalid Unicode") from exc
        return cls(content_type, path, body)

    @classmethod
    def from_json(
        cls,
        value: object,
        *,
        path: str = _DEFAULT_PATH,
        content_type: str = _JSON_CONTENT_TYPE,
    ) -> NativePayload:
        return cls(content_type, path, _canonical_json_body(value))

    @classmethod
    def from_value(
        cls,
        value: object,
        *,
        path: str | None = None,
        content_type: str | None = None,
    ) -> NativePayload:
        """Convert a supported value using explicit, type-safe defaults."""

        if type(value) is NativePayload:
            if path is not None or content_type is not None:
                raise ValueError("path and content_type cannot override a NativePayload")
            return value
        selected_path = _DEFAULT_PATH if path is None else path
        if type(value) is bytes:
            return cls.from_bytes(
                value,
                path=selected_path,
                content_type=_BYTES_CONTENT_TYPE if content_type is None else content_type,
            )
        if type(value) is str:
            return cls.from_text(
                value,
                path=selected_path,
                content_type=_TEXT_CONTENT_TYPE if content_type is None else content_type,
            )
        if value is None or type(value) in {bool, int, float, list, dict}:
            return cls.from_json(
                value,
                path=selected_path,
                content_type=_JSON_CONTENT_TYPE if content_type is None else content_type,
            )
        raise TypeError(f"unsupported native payload value: {type(value).__name__}")

    def _wire(self) -> dict[str, dict[str, str]]:
        return {
            "native": {
                "content_type": self.content_type,
                "path": self.path,
                "body": base64.b64encode(self.body).decode("ascii"),
            }
        }

    def canonical_json(self) -> bytes:
        """Return deterministic ABI payload JSON with canonical padded base64."""

        return json.dumps(
            self._wire(),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")


RequestPayload: TypeAlias = NativePayload | HTTPRequestPayload
ResponsePayload: TypeAlias = NativePayload | HTTPResponsePayload


@dataclass(frozen=True, slots=True)
class AztmSendResult:
    """Local Go-core acceptance of one send or reply command."""

    message_id: str
    conversation_id: str
    accepted: bool

    def __post_init__(self) -> None:
        _identifier(self.message_id, "message_id")
        _identifier(self.conversation_id, "conversation_id")
        if type(self.accepted) is not bool or not self.accepted:
            raise ValueError("accepted must be true for a successful send result")

    @property
    def accepted_by_core(self) -> bool:
        """True only because the frozen successful result says accepted=true."""

        return self.accepted


def _strict_json_loads(body: bytes) -> Any:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for key, value in items:
            if key in output:
                raise ValueError(f"duplicate JSON object field: {key}")
            output[key] = value
        return output

    def constant(value: str) -> Any:
        raise ValueError(f"non-finite JSON number: {value}")

    text = body.decode("utf-8", errors="strict")
    value = json.loads(text, object_pairs_hook=pairs, parse_constant=constant)
    # parse_constant does not catch finite-looking exponents that overflow a
    # Python float (for example 1e999), and json.loads preserves lone UTF-16
    # surrogate escapes in Python strings. Apply the same closed value rules as
    # outbound JSON before exposing the result to application code.
    _validate_json_value(value)
    return value


def _skip_ows(value: str, index: int) -> int:
    while index < len(value) and value[index] in " \t":
        index += 1
    return index


def _parse_content_type(content_type: str) -> tuple[str, dict[str, str]]:
    if not content_type:
        return "", {}
    if any(
        (ord(character) < 0x20 and character != "\t")
        or ord(character) == 0x7F
        for character in content_type
    ):
        raise ValueError("content_type must not contain invalid control characters")
    first_semicolon = content_type.find(";")
    raw_media_type = (
        content_type if first_semicolon == -1 else content_type[:first_semicolon]
    )
    media_type = raw_media_type.strip().lower()
    if media_type.count("/") != 1:
        raise ValueError("content_type is not a valid media type")
    major, minor = media_type.split("/", 1)
    if _TOKEN_RE.fullmatch(major) is None or _TOKEN_RE.fullmatch(minor) is None:
        raise ValueError("content_type is not a valid media type")

    parameters: dict[str, str] = {}
    index = len(content_type) if first_semicolon == -1 else first_semicolon
    while index < len(content_type):
        if content_type[index] != ";":
            raise ValueError("content_type has malformed parameters")
        index = _skip_ows(content_type, index + 1)
        if index >= len(content_type):
            raise ValueError("content_type has malformed parameters")
        name_start = index
        while index < len(content_type) and content_type[index] not in "=;":
            index += 1
        name = content_type[name_start:index].strip().lower()
        if not name or _TOKEN_RE.fullmatch(name) is None:
            raise ValueError("content_type has malformed parameters")
        if name.endswith("*"):
            raise ValueError("content_type has malformed parameters")
        index = _skip_ows(content_type, index)
        if index >= len(content_type) or content_type[index] != "=":
            raise ValueError("content_type has malformed parameters")
        index = _skip_ows(content_type, index + 1)
        if index >= len(content_type):
            raise ValueError("content_type has malformed parameters")
        if content_type[index] == '"':
            index += 1
            chars: list[str] = []
            while index < len(content_type):
                character = content_type[index]
                if character == '"':
                    index += 1
                    break
                if character == "\\":
                    index += 1
                    if index >= len(content_type):
                        raise ValueError("content_type has malformed parameters")
                    character = content_type[index]
                if character in "\r\n":
                    raise ValueError("content_type has malformed parameters")
                chars.append(character)
                index += 1
            else:
                raise ValueError("content_type has malformed parameters")
            parameter_value = "".join(chars)
            index = _skip_ows(content_type, index)
            if index < len(content_type) and content_type[index] != ";":
                raise ValueError("content_type has malformed parameters")
        else:
            value_start = index
            while index < len(content_type) and content_type[index] != ";":
                index += 1
            parameter_value = content_type[value_start:index].strip()
            if not parameter_value or _TOKEN_RE.fullmatch(parameter_value) is None:
                raise ValueError("content_type has malformed parameters")
        if name in parameters:
            raise ValueError("content_type has ambiguous parameters")
        if name == "charset" and not parameter_value:
            raise ValueError("content_type has an ambiguous charset")
        parameters[name] = parameter_value
    return media_type, parameters


def _text_encoding(content_type: str) -> str:
    media_type, parameters = _parse_content_type(content_type)
    if not media_type:
        return "utf-8"
    major, minor = media_type.split("/", 1)
    # RFC 8259 defines JSON exchanged between systems as UTF-8, and the
    # application/json registration defines no charset parameter. Do not let
    # a sender-provided charset reinterpret JSON bytes for the text helper.
    if minor == "json" or minor.endswith("+json"):
        return "utf-8"
    if "charset" in parameters:
        try:
            return codecs.lookup(parameters["charset"]).name
        except (LookupError, TypeError, ValueError) as exc:
            raise ValueError("content_type names an unknown charset") from exc
    if major == "text":
        return "utf-8"
    raise ValueError("payload content_type does not identify text")


def _http_content_type(headers: tuple[tuple[str, str], ...]) -> str:
    values = [value for name, value in headers if name.lower() == "content-type"]
    if not values:
        return ""
    if len(values) > 1:
        raise ValueError("HTTP content-type is ambiguous")
    _utf8_bytes(
        values[0],
        "HTTP content-type",
        maximum=MAX_PUBLIC_STRING_BYTES,
        empty=True,
    )
    content_type = values[0]
    if not content_type:
        raise ValueError("HTTP content-type is empty")
    try:
        _parse_content_type(content_type)
    except ValueError as exc:
        raise ValueError("HTTP content-type is malformed") from exc
    return content_type


def _payload_content_type(payload: RequestPayload | ResponsePayload) -> str:
    if type(payload) is NativePayload:
        return payload.content_type
    if type(payload) is HTTPRequestPayload:
        return _http_content_type(payload.headers)
    if type(payload) is HTTPResponsePayload:
        return _http_content_type(payload.headers)
    raise TypeError("payload has an unsupported canonical variant")


@dataclass(frozen=True, slots=True)
class AztmResponse:
    """One completed remote application response with a canonical payload."""

    message_id: str
    conversation_id: str
    from_agent_id: str
    mesh_id: str
    payload: ResponsePayload

    def __post_init__(self) -> None:
        _identifier(self.message_id, "message_id")
        _identifier(self.conversation_id, "conversation_id")
        _identifier(self.from_agent_id, "from_agent_id")
        _identifier(self.mesh_id, "mesh_id")
        if type(self.payload) not in {NativePayload, HTTPResponsePayload}:
            raise TypeError("payload must be a NativePayload or HTTPResponsePayload")

    @property
    def body(self) -> bytes:
        return self.payload.body

    def text(self) -> str:
        return self.body.decode(_text_encoding(_payload_content_type(self.payload)), errors="strict")

    def json(self) -> Any:
        """Decode strict UTF-8 JSON; a content-type charset never overrides JSON."""

        if type(self.payload) is HTTPResponsePayload:
            _payload_content_type(self.payload)
        return _strict_json_loads(self.body)


def _canonical_base64(value: object) -> bytes:
    if type(value) is not str:
        raise ValueError("body must be canonical base64 text")
    try:
        encoded = value.encode("ascii")
        decoded = base64.b64decode(encoded, validate=True)
    except (UnicodeEncodeError, ValueError, binascii.Error) as exc:
        raise ValueError("body must be canonical base64 text") from exc
    if base64.b64encode(decoded) != encoded:
        raise ValueError("body must use canonical padded base64")
    return decoded


def _native_payload_from_wire(value: object) -> NativePayload:
    if not isinstance(value, Mapping) or len(value) != 1 or set(value) != {"native"}:
        raise ValueError("response must contain exactly one native payload")
    native = value["native"]
    if not isinstance(native, Mapping) or set(native) != {"content_type", "path", "body"}:
        raise ValueError("native response payload has unknown or missing fields")
    return NativePayload(
        native["content_type"],
        native["path"],
        _canonical_base64(native["body"]),
    )


def _request_handle(value: object) -> str:
    handle = _identifier(value, "request_handle")
    if not handle.startswith("reqh_"):
        raise ValueError("request_handle has the wrong opaque handle class")
    encoded = handle[5:]
    try:
        raw = encoded.encode("ascii")
        decoded = base64.b64decode(raw + b"=" * (-len(raw) % 4), altchars=b"-_", validate=True)
    except (UnicodeEncodeError, ValueError, binascii.Error) as exc:
        raise ValueError("request_handle is not canonical base64url") from exc
    canonical = base64.urlsafe_b64encode(decoded).rstrip(b"=").decode("ascii")
    if len(decoded) != 32 or canonical != encoded:
        raise ValueError("request_handle has an invalid opaque shape")
    return handle


class _ReplyState:
    __slots__ = ("_lock", "_state")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state = "ready"

    def begin(self) -> None:
        with self._lock:
            if self._state == "replied":
                raise RuntimeError("this AZTM request has already been replied to")
            if self._state == "replying":
                raise RuntimeError("a reply to this AZTM request is already in progress")
            self._state = "replying"

    def begin_if_ready(self) -> bool:
        """Claim an automatic reply only when no explicit reply owns it."""

        with self._lock:
            if self._state != "ready":
                return False
            self._state = "replying"
            return True

    def commit(self) -> None:
        with self._lock:
            if self._state != "replying":
                raise RuntimeError("the AZTM reply state is inconsistent")
            self._state = "replied"

    def release(self) -> None:
        with self._lock:
            if self._state == "replying":
                self._state = "ready"

    @property
    def replied(self) -> bool:
        with self._lock:
            return self._state == "replied"

    @property
    def claimed(self) -> bool:
        with self._lock:
            return self._state != "ready"


class _RequestBase:
    __slots__ = (
        "_message_id",
        "_conversation_id",
        "_from_agent_id",
        "_mesh_id",
        "_mode",
        "_payload",
        "_request_handle",
        "_reply_state",
    )
    _message_id: str
    _conversation_id: str
    _from_agent_id: str
    _mesh_id: str
    _mode: str
    _payload: RequestPayload
    _request_handle: str | None
    _reply_state: _ReplyState

    def __init__(
        self,
        *,
        message_id: str,
        conversation_id: str,
        from_agent_id: str,
        mesh_id: str,
        payload: RequestPayload,
        request_handle: str | None,
        mode: str = "rpc",
    ) -> None:
        object.__setattr__(self, "_message_id", _identifier(message_id, "message_id"))
        object.__setattr__(
            self, "_conversation_id", _identifier(conversation_id, "conversation_id")
        )
        object.__setattr__(
            self, "_from_agent_id", _identifier(from_agent_id, "from_agent_id")
        )
        object.__setattr__(self, "_mesh_id", _identifier(mesh_id, "mesh_id"))
        if type(payload) not in {NativePayload, HTTPRequestPayload}:
            raise TypeError("payload must be a NativePayload or HTTPRequestPayload")
        if mode not in {"msg", "rpc"}:
            raise ValueError("mode must be 'msg' or 'rpc'")
        if mode == "rpc":
            if request_handle is None:
                raise ValueError("rpc requests require a request_handle")
            validated_handle: str | None = _request_handle(request_handle)
        else:
            if request_handle is not None:
                raise ValueError("msg requests must not expose a request_handle")
            validated_handle = None
        object.__setattr__(self, "_payload", payload)
        object.__setattr__(self, "_mode", mode)
        object.__setattr__(self, "_request_handle", validated_handle)
        object.__setattr__(self, "_reply_state", _ReplyState())

    def __setattr__(self, name: str, value: object) -> None:
        del name, value
        raise AttributeError("AZTM request models are immutable")

    @property
    def message_id(self) -> str:
        return self._message_id

    @property
    def conversation_id(self) -> str:
        return self._conversation_id

    @property
    def from_agent_id(self) -> str:
        return self._from_agent_id

    @property
    def mesh_id(self) -> str:
        return self._mesh_id

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def path(self) -> str:
        return self._payload.path

    @property
    def payload(self) -> RequestPayload:
        return self._payload

    @property
    def request_handle(self) -> str | None:
        return self._request_handle

    @property
    def body(self) -> bytes:
        return self._payload.body

    @property
    def replied(self) -> bool:
        return self._reply_state.replied

    @property
    def reply_claimed(self) -> bool:
        return self._reply_state.claimed

    def text(self) -> str:
        return self.body.decode(_text_encoding(_payload_content_type(self.payload)), errors="strict")

    def json(self) -> Any:
        if type(self.payload) is HTTPRequestPayload:
            _payload_content_type(self.payload)
        return _strict_json_loads(self.body)


class _LegacyAztmRequest(_RequestBase):
    """Reply-capable synchronous inbound RPC model (dispatch arrives later)."""

    __slots__ = ("_reply_callback",)
    _reply_callback: Callable[[str, ResponsePayload], None]

    def __init__(self, *, _reply_callback: Callable[[str, ResponsePayload], None], **fields: Any) -> None:
        super().__init__(**fields)
        object.__setattr__(self, "_reply_callback", _reply_callback)

    def _reply_payload(
        self,
        value: object,
        *,
        path: str | None,
        content_type: str | None,
    ) -> ResponsePayload:
        if type(self.payload) is HTTPRequestPayload:
            if path is not None or content_type is not None:
                raise ValueError("path and content_type cannot override an HTTP response payload")
            if type(value) is not HTTPResponsePayload:
                raise TypeError("HTTP request RPC replies require an explicit HTTPResponsePayload")
            return value
        if type(value) is HTTPResponsePayload:
            raise TypeError("native RPC replies require a NativePayload or native shorthand value")
        return NativePayload.from_value(value, path=path, content_type=content_type)

    def reply(
        self,
        value: object = None,
        *,
        path: str | None = None,
        content_type: str | None = None,
    ) -> None:
        if self.request_handle is None:
            raise RuntimeError("one-way AZTM messages cannot be replied to")
        self._reply_state.begin()
        try:
            payload = self._reply_payload(value, path=path, content_type=content_type)
            self._reply_callback(self.request_handle, payload)
        except BaseException:
            self._reply_state.release()
            raise
        self._reply_state.commit()

    def _reply_return_value(self, value: object) -> bool:
        if not self._reply_state.begin_if_ready():
            return False
        assert self.request_handle is not None
        try:
            payload = self._reply_payload(value, path=None, content_type=None)
            self._reply_callback(self.request_handle, payload)
        except BaseException:
            self._reply_state.release()
            raise
        self._reply_state.commit()
        return True


class _LegacyAsyncAztmRequest(_RequestBase):
    """Reply-capable asynchronous inbound RPC model (dispatch arrives later)."""

    __slots__ = ("_reply_callback",)
    _reply_callback: Callable[[str, ResponsePayload], Awaitable[None]]

    def __init__(
        self,
        *,
        _reply_callback: Callable[[str, ResponsePayload], Awaitable[None]],
        **fields: Any,
    ) -> None:
        super().__init__(**fields)
        object.__setattr__(self, "_reply_callback", _reply_callback)

    def _reply_payload(
        self,
        value: object,
        *,
        path: str | None,
        content_type: str | None,
    ) -> ResponsePayload:
        if type(self.payload) is HTTPRequestPayload:
            if path is not None or content_type is not None:
                raise ValueError("path and content_type cannot override an HTTP response payload")
            if type(value) is not HTTPResponsePayload:
                raise TypeError("HTTP request RPC replies require an explicit HTTPResponsePayload")
            return value
        if type(value) is HTTPResponsePayload:
            raise TypeError("native RPC replies require a NativePayload or native shorthand value")
        return NativePayload.from_value(value, path=path, content_type=content_type)

    async def reply(
        self,
        value: object = None,
        *,
        path: str | None = None,
        content_type: str | None = None,
    ) -> None:
        if self.request_handle is None:
            raise RuntimeError("one-way AZTM messages cannot be replied to")
        self._reply_state.begin()
        try:
            payload = await asyncio.to_thread(
                self._reply_payload,
                value,
                path=path,
                content_type=content_type,
            )
            await self._reply_callback(self.request_handle, payload)
        except BaseException:
            self._reply_state.release()
            raise
        self._reply_state.commit()

    async def _reply_return_value(self, value: object) -> bool:
        if not self._reply_state.begin_if_ready():
            return False
        assert self.request_handle is not None
        try:
            payload = await asyncio.to_thread(
                self._reply_payload,
                value,
                path=None,
                content_type=None,
            )
            await self._reply_callback(self.request_handle, payload)
        except BaseException:
            self._reply_state.release()
            raise
        self._reply_state.commit()
        return True


# Historical import names resolve to the immutable universal request. The
# reply-capable implementations above are intentionally private compatibility
# code and are not used by Session dispatch.
AztmRequest = HTTPRequestPayload
AsyncAztmRequest = HTTPRequestPayload

__all__ = [
    "AsyncAztmRequest",
    "AztmRequest",
    "AztmResponse",
    "AztmSendResult",
    "NativePayload",
]
