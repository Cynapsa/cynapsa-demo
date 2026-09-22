"""Dependency-free canonical HTTP payloads shared by native and HTTP surfaces."""

from __future__ import annotations

import base64
import binascii
import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, ClassVar

from cynapsa.native.command import (
    MAX_ABI_INPUT_BYTES,
    MAX_HEADER_COUNT,
    MAX_IDENTIFIER_BYTES,
    MAX_INLINE_PAYLOAD_BYTES,
    MAX_PATH_BYTES,
    MAX_PUBLIC_STRING_BYTES,
)


_TOKEN_RE = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")


def _utf8(value: object, field: str, maximum: int, *, empty: bool) -> bytes:
    if type(value) is not str or (not empty and not value):
        requirement = "text" if empty else "nonempty text"
        raise ValueError(f"{field} must be {requirement}")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError(f"{field} contains invalid Unicode") from exc
    if len(encoded) > maximum:
        raise ValueError(f"{field} exceeds its {maximum}-byte limit")
    return encoded


def _path(value: object) -> tuple[str, bytes]:
    encoded = _utf8(value, "path", MAX_PATH_BYTES, empty=False)
    assert isinstance(value, str)
    if not value.startswith("/") or "?" in value or "#" in value:
        raise ValueError(
            "path must be an absolute application path without query or fragment"
        )
    return value, encoded


def _body(value: object) -> bytes:
    if type(value) is not bytes:
        raise TypeError("body must be bytes")
    # bytes(x) is permitted to return x. A memoryview copy gives each model a
    # distinct immutable ownership snapshot, matching the native boundary.
    return memoryview(value).tobytes()


def _headers(value: object) -> tuple[tuple[str, str], ...]:
    if not isinstance(value, (list, tuple)):
        raise TypeError("headers must be an ordered sequence")
    if len(value) > MAX_HEADER_COUNT:
        raise ValueError(f"headers exceeds its {MAX_HEADER_COUNT}-field limit")
    output: list[tuple[str, str]] = []
    for item in value:
        if isinstance(item, Mapping):
            if set(item) != {"name", "value"}:
                raise ValueError("each header must contain exactly name and value")
            name, text = item["name"], item["value"]
        elif isinstance(item, (list, tuple)) and len(item) == 2:
            name, text = item
        else:
            raise TypeError("each header must be a name/value pair")
        name_bytes = _utf8(name, "header.name", MAX_IDENTIFIER_BYTES, empty=False)
        _utf8(text, "header.value", MAX_PUBLIC_STRING_BYTES, empty=True)
        assert isinstance(name, str) and isinstance(text, str)
        if (
            any(byte >= 0x80 for byte in name_bytes)
            or _TOKEN_RE.fullmatch(name) is None
            or "\r" in text
            or "\n" in text
        ):
            raise ValueError("header is invalid")
        output.append((name, text))
    return tuple(output)


def _strict_base64(value: object, field: str = "body") -> bytes:
    if type(value) is not str:
        raise TypeError(f"{field} must be canonical base64 text")
    try:
        encoded = value.encode("ascii")
        decoded = base64.b64decode(encoded, validate=True)
    except (UnicodeEncodeError, ValueError, binascii.Error) as exc:
        raise ValueError(f"{field} is not canonical base64") from exc
    if base64.b64encode(decoded) != encoded:
        raise ValueError(f"{field} is not canonical padded base64")
    return decoded


def _wire_headers(headers: tuple[tuple[str, str], ...]) -> list[dict[str, str]]:
    return [{"name": name, "value": value} for name, value in headers]


def _strict_document(raw: bytes) -> dict[str, Any]:
    if type(raw) is not bytes:
        raise TypeError("canonical JSON must be bytes")
    if len(raw) > MAX_ABI_INPUT_BYTES:
        raise ValueError("canonical JSON exceeds the ABI input limit")

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for key, value in items:
            if key in output:
                raise ValueError("canonical JSON contains a duplicate field")
            output[key] = value
        return output

    def constant(_: str) -> Any:
        raise ValueError("canonical JSON contains a non-finite number")

    try:
        value = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=pairs,
            parse_constant=constant,
        )
    except RecursionError as exc:
        raise ValueError("canonical JSON exceeds the nesting limit") from exc
    if type(value) is not dict:
        raise ValueError("canonical JSON must contain one payload object")
    return value


def _freeze_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze_json(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze_json(item) for item in value)
    return value


def _plain_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _plain_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_plain_json(item) for item in value]
    return value


def _normalize_error_string(value: str) -> str:
    try:
        return value.encode("utf-16-le", errors="surrogatepass").decode(
            "utf-16-le", errors="strict"
        )
    except UnicodeDecodeError as exc:
        raise ValueError("error.details contains an unpaired surrogate") from exc


def _normalize_error_details(value: Any, *, depth: int = 0) -> Any:
    if depth > 32:
        raise ValueError("error.details exceeds the maximum nesting depth")
    if value is None or type(value) in {bool, int}:
        return value
    if type(value) is str:
        return _normalize_error_string(value)
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("error.details contains a non-finite number")
        return value
    if isinstance(value, (list, tuple)):
        return [
            _normalize_error_details(item, depth=depth + 1) for item in value
        ]
    if isinstance(value, Mapping):
        output: dict[str, Any] = {}
        for key, item in value.items():
            if type(key) is not str:
                raise TypeError("error.details keys must be strings")
            normalized_key = _normalize_error_string(key)
            if normalized_key in output:
                raise ValueError("error.details contains a duplicate key")
            output[normalized_key] = _normalize_error_details(
                item, depth=depth + 1
            )
        return output
    raise TypeError("error.details must contain JSON-compatible values")


class _HTTPPayload:
    headers: tuple[tuple[str, str], ...]
    body: bytes

    @property
    def inline_size(self) -> int:
        raise NotImplementedError

    def validate_payload_limit(self, payload_limit: object) -> None:
        if type(payload_limit) is not int or payload_limit < 1:
            raise ValueError("payload_limit must be a positive integer")
        if self.inline_size > payload_limit:
            raise ValueError("HTTP payload exceeds the configured payload limit")

    def canonical_json(self) -> bytes:
        return json.dumps(
            self._wire(),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")

    def _wire(self) -> dict[str, Any]:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class CynapsaRequest(_HTTPPayload):
    """Transport-neutral request used by native handlers and HTTP adapters."""

    method: str
    path: str
    query: str = ""
    headers: tuple[tuple[str, str], ...] = ()
    body: bytes = b""
    message_id: str | None = None
    conversation_id: str | None = None
    from_agent_id: str | None = None
    mesh_id: str | None = None
    mode: str | None = None
    variant: ClassVar[str] = "http_request"

    def __post_init__(self) -> None:
        method_bytes = _utf8(self.method, "method", 32, empty=False)
        if any(byte >= 0x80 for byte in method_bytes) or _TOKEN_RE.fullmatch(self.method) is None:
            raise ValueError("method is not a valid HTTP token")
        path, path_bytes = _path(self.path)
        query_bytes = _utf8(
            self.query, "query", MAX_PUBLIC_STRING_BYTES, empty=True
        )
        headers = _headers(self.headers)
        body = _body(self.body)
        inline = len(method_bytes) + len(path_bytes) + len(query_bytes) + len(body)
        inline += sum(
            len(name.encode("utf-8")) + len(value.encode("utf-8"))
            for name, value in headers
        )
        if inline > MAX_INLINE_PAYLOAD_BYTES:
            raise ValueError("HTTP request exceeds the 256 KiB inline payload limit")
        object.__setattr__(self, "path", path)
        object.__setattr__(self, "headers", headers)
        object.__setattr__(self, "body", body)
        for field in ("message_id", "conversation_id", "from_agent_id", "mesh_id"):
            value = getattr(self, field)
            if value is not None:
                _utf8(value, field, MAX_IDENTIFIER_BYTES, empty=False)
        if self.mode not in {None, "rpc", "msg"}:
            raise ValueError("mode must be 'rpc', 'msg', or None")

    @property
    def inline_size(self) -> int:
        return (
            len(self.method.encode("utf-8"))
            + len(self.path.encode("utf-8"))
            + len(self.query.encode("utf-8"))
            + sum(
                len(name.encode("utf-8")) + len(value.encode("utf-8"))
                for name, value in self.headers
            )
            + len(self.body)
        )

    @property
    def content(self) -> bytes:
        return self.body

    def text(self) -> str:
        return _decode_text(self.headers, self.body)

    def json(self) -> Any:
        return _decode_json(self.headers, self.body)

    def _wire(self) -> dict[str, Any]:
        return {
            self.variant: {
                "method": self.method,
                "path": self.path,
                "query": self.query,
                "headers": _wire_headers(self.headers),
                "body": base64.b64encode(self.body).decode("ascii"),
            }
        }

    @classmethod
    def from_wire(cls, value: object) -> CynapsaRequest:
        if not isinstance(value, Mapping) or set(value) != {
            "method", "path", "query", "headers", "body"
        }:
            raise ValueError("http_request has unknown or missing fields")
        return cls(
            value["method"],
            value["path"],
            value["query"],
            value["headers"],
            _strict_base64(value["body"]),
        )

    @classmethod
    def from_canonical_json(cls, raw: bytes) -> CynapsaRequest:
        document = _strict_document(raw)
        if set(document) != {cls.variant}:
            raise ValueError("canonical JSON is not an HTTP request payload")
        return cls.from_wire(document[cls.variant])


@dataclass(frozen=True, slots=True)
class CynapsaApplicationError:
    """Safe metadata for an intentional remote application error."""

    code: str
    detail: str
    details: Mapping[str, Any] = field(
        default_factory=lambda: MappingProxyType({})
    )

    def __post_init__(self) -> None:
        _utf8(self.code, "error.code", MAX_IDENTIFIER_BYTES, empty=False)
        _utf8(self.detail, "error.detail", MAX_PUBLIC_STRING_BYTES, empty=False)
        if not isinstance(self.details, Mapping):
            raise TypeError("error.details must be a mapping")
        try:
            normalized_details = _normalize_error_details(self.details)
            encoded = json.dumps(
                normalized_details,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
        except (TypeError, ValueError, UnicodeEncodeError) as exc:
            raise TypeError("error.details must contain JSON-compatible values") from exc
        normalized = json.loads(encoded.decode("utf-8"))
        object.__setattr__(self, "details", _freeze_json(normalized))


def _error_details_json(error: CynapsaApplicationError) -> str:
    return json.dumps(
        _plain_json(error.details),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _error_wire(error: CynapsaApplicationError) -> dict[str, str]:
    return {
        "code": error.code,
        "detail": error.detail,
        "details_json": _error_details_json(error),
    }


def _decode_error(value: object) -> CynapsaApplicationError:
    try:
        if not isinstance(value, Mapping) or set(value) != {
            "code",
            "detail",
            "details_json",
        }:
            raise ValueError
        details_json = value["details_json"]
        if type(details_json) is not str:
            raise TypeError
        _utf8(
            details_json,
            "error.details_json",
            MAX_PUBLIC_STRING_BYTES,
            empty=True,
        )
        details = _strict_document(details_json.encode("utf-8"))
        return CynapsaApplicationError(
            value["code"], value["detail"], details
        )
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise ValueError("http_response has invalid Cynapsa error metadata") from exc


def _decode_text(headers: tuple[tuple[str, str], ...], body: bytes) -> str:
    # Reuse the native payload parser without making optional HTTP libraries a
    # dependency of this universal model. The import is delayed to avoid the
    # messaging/http module initialization cycle.
    from cynapsa.messaging import _http_content_type, _text_encoding

    encoding = _text_encoding(_http_content_type(headers))
    return body.decode(encoding, errors="strict")


def _decode_json(headers: tuple[tuple[str, str], ...], body: bytes) -> Any:
    from cynapsa.messaging import _http_content_type, _strict_json_loads

    _http_content_type(headers)
    return _strict_json_loads(body)


@dataclass(frozen=True, slots=True)
class CynapsaResponse(_HTTPPayload):
    """Transport-neutral response returned by native and HTTP RPC calls."""

    status_code: int
    reason: str = ""
    headers: tuple[tuple[str, str], ...] = ()
    body: bytes = b""
    error: CynapsaApplicationError | None = None
    variant: ClassVar[str] = "http_response"

    def __post_init__(self) -> None:
        if type(self.status_code) is not int or not 100 <= self.status_code <= 599:
            raise ValueError("status_code must be an integer in 100..599")
        reason_bytes = _utf8(
            self.reason, "reason", MAX_IDENTIFIER_BYTES, empty=True
        )
        if "\r" in self.reason or "\n" in self.reason:
            raise ValueError("reason must not contain CR or LF")
        headers = _headers(self.headers)
        body = _body(self.body)
        if self.error is not None and type(self.error) is not CynapsaApplicationError:
            raise TypeError("error must be a CynapsaApplicationError or None")
        if self.error is not None and self.status_code < 400:
            raise ValueError("error metadata requires a 4xx or 5xx status")
        error_size = 0
        if self.error is not None:
            details_json = _error_details_json(self.error)
            _utf8(
                details_json,
                "error.details_json",
                MAX_PUBLIC_STRING_BYTES,
                empty=True,
            )
            error_size = (
                len(self.error.code.encode("utf-8"))
                + len(self.error.detail.encode("utf-8"))
                + len(details_json.encode("utf-8"))
            )
        inline = len(reason_bytes) + len(body) + error_size
        inline += sum(
            len(name.encode("utf-8")) + len(value.encode("utf-8"))
            for name, value in headers
        )
        if inline > MAX_INLINE_PAYLOAD_BYTES:
            raise ValueError("HTTP response exceeds the 256 KiB inline payload limit")
        object.__setattr__(self, "headers", headers)
        object.__setattr__(self, "body", body)

    @property
    def content(self) -> bytes:
        """Return the exact immutable response bytes."""

        return self.body

    def text(self) -> str:
        """Decode the response body, using a declared charset when present."""

        return _decode_text(self.headers, self.body)

    def json(self) -> Any:
        """Decode strict UTF-8 JSON from the response body."""

        return _decode_json(self.headers, self.body)

    def raise_for_error(self) -> None:
        """Raise for 4xx/5xx only when the caller explicitly requests it."""

        if self.status_code >= 400:
            from cynapsa.exceptions import RemoteApplicationError

            raise RemoteApplicationError(self)

    @classmethod
    def from_value(cls, value: object) -> CynapsaResponse:
        """Normalize one native handler return value."""

        if type(value) is cls:
            return value
        if type(value) is bytes:
            return cls(200, "OK", (("content-type", "application/octet-stream"),), value)
        if type(value) is str:
            return cls(
                200,
                "OK",
                (("content-type", "text/plain; charset=utf-8"),),
                value.encode("utf-8"),
            )
        if value is None or type(value) in {bool, int, float, list, dict}:
            try:
                # Keep handler-return normalization identical to native
                # shorthand: exact JSON container/scalar types only, with no
                # implicit tuple/subclass/custom-object conversion.
                from cynapsa.messaging import _canonical_json_body

                body = _canonical_json_body(value)
            except (TypeError, ValueError, UnicodeEncodeError) as exc:
                raise TypeError("handler return value is not JSON-compatible") from exc
            return cls(200, "OK", (("content-type", "application/json"),), body)
        raise TypeError(f"unsupported handler return value: {type(value).__name__}")

    @classmethod
    def from_rpc_exception(cls, exception: Any) -> CynapsaResponse:
        metadata = CynapsaApplicationError(
            exception.code, exception.detail, exception.details
        )
        body = json.dumps(
            {
                "error": {
                    "code": metadata.code,
                    "detail": metadata.detail,
                    "details": _plain_json(metadata.details),
                }
            },
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        headers = tuple(exception.headers)
        if not any(name.lower() == "content-type" for name, _ in headers):
            headers += (("content-type", "application/json"),)
        return cls(exception.status_code, "", headers, body, metadata)

    @classmethod
    def handler_error(cls) -> CynapsaResponse:
        metadata = CynapsaApplicationError(
            "handler_error", "The application handler failed"
        )
        body = b'{"error":{"code":"handler_error","detail":"The application handler failed","details":{}}}'
        return cls(
            500,
            "Internal Server Error",
            (("content-type", "application/json"),),
            body,
            metadata,
        )

    @property
    def inline_size(self) -> int:
        error_size = 0
        if self.error is not None:
            error_size = (
                len(self.error.code.encode("utf-8"))
                + len(self.error.detail.encode("utf-8"))
                + len(_error_details_json(self.error).encode("utf-8"))
            )
        return (
            len(self.reason.encode("utf-8"))
            + sum(
                len(name.encode("utf-8")) + len(value.encode("utf-8"))
                for name, value in self.headers
            )
            + len(self.body)
            + error_size
        )

    def _wire(self) -> dict[str, Any]:
        response: dict[str, Any] = {
            "status_code": self.status_code,
            "reason": self.reason,
            "headers": _wire_headers(self.headers),
            "body": base64.b64encode(self.body).decode("ascii"),
        }
        if self.error is not None:
            response["error"] = _error_wire(self.error)
        return {self.variant: response}

    @classmethod
    def from_wire(cls, value: object) -> CynapsaResponse:
        if not isinstance(value, Mapping) or set(value) not in (
            {"status_code", "reason", "headers", "body"},
            {"status_code", "reason", "headers", "body", "error"},
        ):
            raise ValueError("http_response has unknown or missing fields")
        headers = _headers(value["headers"])
        body = _strict_base64(value["body"])
        error = _decode_error(value["error"]) if "error" in value else None
        return cls(
            value["status_code"],
            value["reason"],
            headers,
            body,
            error,
        )

    @classmethod
    def from_canonical_json(cls, raw: bytes) -> CynapsaResponse:
        document = _strict_document(raw)
        if set(document) != {cls.variant}:
            raise ValueError("canonical JSON is not an HTTP response payload")
        return cls.from_wire(document[cls.variant])


HTTPRequestPayload = CynapsaRequest
HTTPResponsePayload = CynapsaResponse


def _http_payload_from_wire(value: object) -> CynapsaRequest | CynapsaResponse:
    if not isinstance(value, Mapping) or len(value) != 1:
        raise ValueError("payload must contain exactly one HTTP variant")
    if "http_request" in value:
        return HTTPRequestPayload.from_wire(value["http_request"])
    if "http_response" in value:
        return HTTPResponsePayload.from_wire(value["http_response"])
    raise ValueError("payload is not a canonical HTTP variant")


__all__ = ["CynapsaApplicationError", "CynapsaRequest", "CynapsaResponse"]
