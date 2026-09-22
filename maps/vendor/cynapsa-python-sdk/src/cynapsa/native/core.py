"""Owned buffers, normalized errors, and native core lifecycle."""

from __future__ import annotations

import base64
import binascii
import ctypes
import json
import os
import threading
from collections import deque
from collections.abc import Mapping
from typing import Any

from cynapsa.exceptions import NativeError

from ._callback import CallbackDispatcher, CallbackEventItem
from .abi import (
    ABI_VERSION,
    CYNAPSA_STATUS_V1_ERROR,
    CYNAPSA_STATUS_V1_OK,
    CYNAPSA_STATUS_V1_WAIT_TIMEOUT,
    cynapsa_buffer_desc_v1,
    configure_library,
    validate_abi_version,
)
from .command import (
    CommandCompletion,
    CommandFuture,
    PUBLIC_ERROR_MESSAGES,
    decode_admission,
    decode_completion,
    encode_cancel,
    encode_command,
)

MAX_NATIVE_BUFFER_BYTES = 64 * 1024 * 1024
MAX_TIMEOUT_MS = (2**63 - 1) // 1_000_000
MAX_QUEUE_LIMIT = 65_536
MAX_PAYLOAD_BYTES = 134_217_696

_CORE_CONFIG_FIELDS = (
    "abi_version",
    "command_timeout_ms",
    "rpc_timeout_ms",
    "queue_limit",
    "payload_limit",
)
_PUBLIC_ERROR_FIELDS = {
    "code",
    "message",
    "retryable",
    "stage",
    "local_or_remote",
}
_PUBLIC_ERROR_CODES = frozenset(PUBLIC_ERROR_MESSAGES)
_PUBLIC_ERROR_STAGES = {
    "sdk",
    "command",
    "auth",
    "policy",
    "connectivity",
    "delivery",
    "payload",
    "handler",
    "rpc",
    "shutdown",
}
_PUBLIC_LIFECYCLES = {
    "created",
    "connecting",
    "ready",
    "degraded",
    "closing",
    "closed",
    "failed",
}
_PUBLIC_CONNECTIVITY = {"unknown", "available", "degraded", "unavailable"}
_PUBLIC_PERSONALITIES = {"unset", "http_bridge", "native"}
_STATUS_FIELDS = {
    "lifecycle",
    "connectivity",
    "personality",
    "agent_id",
    "mesh_id",
    "mesh_endpoint",
    "queued_message_count",
}


def _descriptor_is_empty(descriptor: cynapsa_buffer_desc_v1) -> bool:
    return not descriptor.buffer_handle and not descriptor.byte_length


def _error_summary(error: NativeError) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "status": error.status,
        "code": error.code,
        "message": error.message,
    }
    if error.details:
        summary["details"] = dict(error.details)
    return summary


def _with_detail(error: NativeError, key: str, value: Any) -> NativeError:
    details = dict(error.details)
    details[key] = value
    return NativeError(error.status, error.code, error.message, details)


def _with_cleanup_error(error: NativeError, cleanup: NativeError) -> NativeError:
    details = dict(error.details)
    cleanup_errors = list(details.get("cleanup_errors", []))
    cleanup_errors.append(_error_summary(cleanup))
    details["cleanup_errors"] = cleanup_errors
    return NativeError(error.status, error.code, error.message, details)


def _strict_json_object(raw: bytes) -> dict[str, Any]:
    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError(f"duplicate JSON field: {key}")
            value[key] = item
        return value

    def reject_constant(value: str) -> Any:
        raise ValueError(f"invalid JSON number: {value}")

    document = json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=object_pairs,
        parse_constant=reject_constant,
    )
    if not isinstance(document, dict):
        raise ValueError("the JSON document is not an object")
    return document


def _free_buffer(
    library: Any,
    handle: int,
    retired: set[int] | None = None,
) -> None:
    if not handle:
        return
    if retired is None:
        retired = set()
    if handle in retired:
        raise NativeError(
            CYNAPSA_STATUS_V1_ERROR,
            "invalid_buffer_descriptor",
            "The native core reused one owned buffer handle in multiple descriptors",
            {"buffer_handle": handle},
        )
    # Record the retirement attempt before the call. Even a failed FreeBuffer
    # must not lead the binding to free the same descriptor a second time.
    retired.add(handle)
    out_error = cynapsa_buffer_desc_v1()
    try:
        status = int(
            library.cynapsa_v1_buffer_free(handle, ctypes.byref(out_error))
        )
    except Exception as exc:
        error = NativeError(
            CYNAPSA_STATUS_V1_ERROR,
            "buffer_free_failed",
            "The native core buffer release call failed",
            {"buffer_handle": handle, "operation": "buffer_free"},
        )
        if not _descriptor_is_empty(out_error):
            if int(out_error.buffer_handle) == handle:
                nested = NativeError(
                    CYNAPSA_STATUS_V1_ERROR,
                    "invalid_buffer_descriptor",
                    "The native buffer free reused its input handle as an error descriptor",
                    {"buffer_handle": handle},
                )
            else:
                nested = _decode_native_error(
                    library,
                    CYNAPSA_STATUS_V1_ERROR,
                    out_error,
                    retired,
                )
            error = _with_detail(error, "native_error", _error_summary(nested))
        raise error from exc

    nested_error: NativeError | None = None
    if not _descriptor_is_empty(out_error):
        nested_error = _decode_native_error(
            library,
            status if status != CYNAPSA_STATUS_V1_OK else CYNAPSA_STATUS_V1_ERROR,
            out_error,
            retired,
        )

    if status != CYNAPSA_STATUS_V1_OK:
        details: dict[str, Any] = {"buffer_handle": handle}
        if nested_error is not None:
            details["native_error"] = _error_summary(nested_error)
        raise NativeError(
            status,
            "buffer_free_failed",
            "The native core could not release an owned buffer",
            details,
        )
    if nested_error is not None:
        raise NativeError(
            CYNAPSA_STATUS_V1_ERROR,
            "invalid_native_output",
            "The native core returned an error buffer with a successful buffer free",
            {
                "buffer_handle": handle,
                "native_error": _error_summary(nested_error),
            },
        )


def _copy_and_free_buffer(
    library: Any,
    descriptor: cynapsa_buffer_desc_v1,
    retired: set[int],
) -> bytes:
    handle = int(descriptor.buffer_handle)
    length = int(descriptor.byte_length)
    if not handle:
        if length:
            raise NativeError(
                CYNAPSA_STATUS_V1_ERROR,
                "invalid_buffer_descriptor",
                "The native core returned a buffer length without a handle",
                {"byte_length": length},
            )
        return b""
    if handle in retired:
        raise NativeError(
            CYNAPSA_STATUS_V1_ERROR,
            "invalid_buffer_descriptor",
            "The native core reused one owned buffer handle in multiple descriptors",
            {"buffer_handle": handle, "byte_length": length},
        )

    result: bytes | None = None
    primary: BaseException | None = None
    if not length:
        primary = NativeError(
            CYNAPSA_STATUS_V1_ERROR,
            "invalid_buffer_descriptor",
            "The native core returned a buffer handle with zero length",
            {"buffer_handle": handle},
        )
    elif length > MAX_NATIVE_BUFFER_BYTES:
        primary = NativeError(
            CYNAPSA_STATUS_V1_ERROR,
            "native_buffer_too_large",
            "The native buffer exceeded the binding safety limit",
            {
                "buffer_handle": handle,
                "byte_length": length,
                "maximum": MAX_NATIVE_BUFFER_BYTES,
            },
        )
    else:
        try:
            destination = (ctypes.c_uint8 * length)()
            copied = ctypes.c_uint64()
            out_error = cynapsa_buffer_desc_v1()
            try:
                status = int(
                    library.cynapsa_v1_buffer_read(
                        handle,
                        0,
                        destination,
                        length,
                        ctypes.byref(copied),
                        ctypes.byref(out_error),
                    )
                )
            except Exception as exc:
                error = NativeError(
                    CYNAPSA_STATUS_V1_ERROR,
                    "buffer_read_failed",
                    "The native core buffer copy call failed",
                    {"buffer_handle": handle, "operation": "buffer_read"},
                )
                if not _descriptor_is_empty(out_error):
                    if int(out_error.buffer_handle) == handle:
                        nested = NativeError(
                            CYNAPSA_STATUS_V1_ERROR,
                            "invalid_buffer_descriptor",
                            "The native buffer read reused its input handle as an error descriptor",
                            {"buffer_handle": handle},
                        )
                    else:
                        nested = _decode_native_error(
                            library,
                            CYNAPSA_STATUS_V1_ERROR,
                            out_error,
                            retired,
                        )
                    error = _with_detail(
                        error, "native_error", _error_summary(nested)
                    )
                raise error from exc
            nested_error: NativeError | None = None
            if not _descriptor_is_empty(out_error):
                if int(out_error.buffer_handle) == handle:
                    nested_error = NativeError(
                        CYNAPSA_STATUS_V1_ERROR,
                        "invalid_buffer_descriptor",
                        "The native buffer read reused its input handle as an error descriptor",
                        {"buffer_handle": handle},
                    )
                else:
                    nested_error = _decode_native_error(
                        library,
                        status
                        if status != CYNAPSA_STATUS_V1_OK
                        else CYNAPSA_STATUS_V1_ERROR,
                        out_error,
                        retired,
                    )
            if status != CYNAPSA_STATUS_V1_OK:
                details: dict[str, Any] = {"buffer_handle": handle}
                if nested_error is not None:
                    details["native_error"] = _error_summary(nested_error)
                raise NativeError(
                    status,
                    "buffer_read_failed",
                    "The native core could not copy an owned buffer",
                    details,
                )
            if nested_error is not None:
                raise NativeError(
                    CYNAPSA_STATUS_V1_ERROR,
                    "invalid_native_output",
                    "The native core returned an error buffer with a successful buffer read",
                    {"native_error": _error_summary(nested_error)},
                )
            if copied.value != length:
                raise NativeError(
                    CYNAPSA_STATUS_V1_ERROR,
                    "short_buffer_read",
                    "The native core returned a byte count that did not match its buffer descriptor",
                    {"expected": length, "actual": int(copied.value)},
                )
            result = bytes(destination)
        except BaseException as exc:
            primary = exc

    cleanup_error: NativeError | None = None
    try:
        _free_buffer(library, handle, retired)
    except NativeError as exc:
        cleanup_error = exc

    if primary is not None:
        if isinstance(primary, NativeError) and cleanup_error is not None:
            primary = _with_cleanup_error(primary, cleanup_error)
        raise primary
    if cleanup_error is not None:
        raise cleanup_error
    assert result is not None
    return result


def copy_and_free_buffer(
    library: Any, descriptor: cynapsa_buffer_desc_v1
) -> bytes:
    """Copy one immutable native buffer and retire its handle exactly once."""

    return _copy_and_free_buffer(library, descriptor, set())


def _decode_native_error(
    library: Any,
    status: int,
    descriptor: cynapsa_buffer_desc_v1,
    retired: set[int],
) -> NativeError:
    if _descriptor_is_empty(descriptor):
        code = (
            "wait_timeout"
            if status == CYNAPSA_STATUS_V1_WAIT_TIMEOUT
            else "native_error"
        )
        message = (
            "The native operation timed out"
            if status == CYNAPSA_STATUS_V1_WAIT_TIMEOUT
            else "The native core returned an error without details"
        )
        return NativeError(status, code, message)

    try:
        raw = _copy_and_free_buffer(library, descriptor, retired)
        document = _strict_json_object(raw)
        if set(document) != {"abi_version", "error"}:
            raise ValueError("the JSON document has unknown or missing fields")
        version = document["abi_version"]
        if isinstance(version, bool) or version != ABI_VERSION:
            raise ValueError("the native error has an invalid ABI version")
        error = document["error"]
        if not isinstance(error, dict):
            raise ValueError("the JSON document has no error object")
        fields = set(error)
        if fields not in (
            _PUBLIC_ERROR_FIELDS,
            _PUBLIC_ERROR_FIELDS | {"diagnostic_id"},
        ):
            raise ValueError("the native error has unknown or missing fields")
        code = error["code"]
        message = error["message"]
        retryable = error["retryable"]
        stage = error["stage"]
        location = error["local_or_remote"]
        if not isinstance(code, str) or code not in _PUBLIC_ERROR_CODES:
            raise ValueError("the native error code is invalid")
        if message != PUBLIC_ERROR_MESSAGES[code]:
            raise ValueError("the native error message is not normalized")
        if not isinstance(retryable, bool):
            raise ValueError("the native retryable field is invalid")
        if not isinstance(stage, str) or stage not in _PUBLIC_ERROR_STAGES:
            raise ValueError("the native error stage is invalid")
        if location not in {"local", "remote"}:
            raise ValueError("the native error location is invalid")
        details: dict[str, Any] = {
            "retryable": retryable,
            "stage": stage,
            "local_or_remote": location,
        }
        if "diagnostic_id" in error:
            diagnostic_id = error["diagnostic_id"]
            if not isinstance(diagnostic_id, str):
                raise ValueError("the native diagnostic identifier is invalid")
            try:
                decoded = base64.b64decode(
                    diagnostic_id + "=" * (-len(diagnostic_id) % 4),
                    altchars=b"-_",
                    validate=True,
                )
            except (ValueError, binascii.Error) as exc:
                raise ValueError(
                    "the native diagnostic identifier is invalid"
                ) from exc
            if (
                len(decoded) != 32
                or base64.urlsafe_b64encode(decoded).rstrip(b"=").decode()
                != diagnostic_id
            ):
                raise ValueError("the native diagnostic identifier is invalid")
            details["diagnostic_id"] = diagnostic_id
        return NativeError(status, code, message, details)
    except NativeError as exc:
        if exc.code in {
            "buffer_read_failed",
            "buffer_free_failed",
            "short_buffer_read",
            "invalid_buffer_descriptor",
            "invalid_native_output",
            "native_buffer_too_large",
        }:
            return exc
        return NativeError(
            status,
            "native_error_decode_failed",
            "The native core returned an unreadable error",
            {"reason": str(exc)},
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        return NativeError(
            status,
            "native_error_decode_failed",
            "The native core returned an unreadable error",
            {"reason": str(exc)},
        )


def decode_native_error(
    library: Any,
    status: int,
    descriptor: cynapsa_buffer_desc_v1,
) -> NativeError:
    """Consume a native error descriptor and return one normalized exception."""

    return _decode_native_error(library, status, descriptor, set())


def _invalid_success_error(
    library: Any,
    descriptor: cynapsa_buffer_desc_v1,
    operation: str,
    retired: set[int] | None = None,
) -> NativeError:
    nested = _decode_native_error(
        library,
        CYNAPSA_STATUS_V1_ERROR,
        descriptor,
        set() if retired is None else retired,
    )
    return NativeError(
        CYNAPSA_STATUS_V1_ERROR,
        "invalid_native_output",
        f"The native core returned an error buffer after successful {operation}",
        {"native_error": _error_summary(nested)},
    )


def _validated_integer(
    value: Any,
    field: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
        or value > maximum
    ):
        raise NativeError(
            CYNAPSA_STATUS_V1_ERROR,
            "invalid_input",
            f"{field} must be an integer in the range {minimum}..{maximum}",
            {"field": field, "minimum": minimum, "maximum": maximum},
        )
    return value


def _json_bytes(value: Mapping[str, Any]) -> bytes:
    if not isinstance(value, Mapping):
        raise NativeError(
            CYNAPSA_STATUS_V1_ERROR,
            "invalid_input",
            "Core configuration must be a mapping",
        )
    try:
        fields = set(value)
    except Exception as exc:
        raise NativeError(
            CYNAPSA_STATUS_V1_ERROR,
            "invalid_input",
            "Core configuration fields could not be read",
        ) from exc
    required = set(_CORE_CONFIG_FIELDS)
    if fields != required:
        raise NativeError(
            CYNAPSA_STATUS_V1_ERROR,
            "invalid_input",
            "Core configuration has unknown or missing fields",
            {
                "missing": sorted(required - fields),
                "unknown": sorted(str(field) for field in fields - required),
            },
        )

    version = _validated_integer(
        value["abi_version"], "abi_version", minimum=ABI_VERSION, maximum=ABI_VERSION
    )
    canonical = {
        "abi_version": version,
        "command_timeout_ms": _validated_integer(
            value["command_timeout_ms"],
            "command_timeout_ms",
            minimum=0,
            maximum=MAX_TIMEOUT_MS,
        ),
        "rpc_timeout_ms": _validated_integer(
            value["rpc_timeout_ms"],
            "rpc_timeout_ms",
            minimum=0,
            maximum=MAX_TIMEOUT_MS,
        ),
        "queue_limit": _validated_integer(
            value["queue_limit"],
            "queue_limit",
            minimum=1,
            maximum=MAX_QUEUE_LIMIT,
        ),
        "payload_limit": _validated_integer(
            value["payload_limit"],
            "payload_limit",
            minimum=1,
            maximum=MAX_PAYLOAD_BYTES,
        ),
    }
    return json.dumps(
        canonical,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _input_pointer(data: bytes) -> tuple[Any, Any]:
    if not data:
        return ctypes.POINTER(ctypes.c_uint8)(), None
    owner = (ctypes.c_uint8 * len(data)).from_buffer_copy(data)
    return ctypes.cast(owner, ctypes.POINTER(ctypes.c_uint8)), owner


def _validated_timeout(timeout_ms: int) -> int:
    if (
        isinstance(timeout_ms, bool)
        or not isinstance(timeout_ms, int)
        or timeout_ms < 0
        or timeout_ms > 2**63 - 1
    ):
        raise TypeError("timeout_ms must be a nonnegative signed 64-bit integer")
    return timeout_ms


def _decode_status(raw: bytes) -> dict[str, Any]:
    try:
        document = _strict_json_object(raw)
        if set(document) != {"abi_version", "status"}:
            raise ValueError("the status document has unknown or missing fields")
        version = document["abi_version"]
        if isinstance(version, bool) or version != ABI_VERSION:
            raise ValueError("the status document has an invalid ABI version")
        status = document["status"]
        if not isinstance(status, dict) or set(status) != _STATUS_FIELDS:
            raise ValueError("the status object has unknown or missing fields")
        if status["lifecycle"] not in _PUBLIC_LIFECYCLES:
            raise ValueError("the lifecycle value is invalid")
        if status["connectivity"] not in _PUBLIC_CONNECTIVITY:
            raise ValueError("the connectivity value is invalid")
        if status["personality"] not in _PUBLIC_PERSONALITIES:
            raise ValueError("the personality value is invalid")
        for field in ("agent_id", "mesh_id", "mesh_endpoint"):
            if not isinstance(status[field], str):
                raise ValueError(f"the {field} value is invalid")
        queued = status["queued_message_count"]
        if isinstance(queued, bool) or not isinstance(queued, int) or queued < 0 or queued > 2**64 - 1:
            raise ValueError("the queued_message_count value is invalid")
        return document
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise NativeError(
            CYNAPSA_STATUS_V1_ERROR,
            "native_result_decode_failed",
            "The native core returned an unreadable status result",
            {"reason": str(exc)},
        ) from exc


def _consume_document_outputs(
    library: Any,
    native_status: int,
    out_result: cynapsa_buffer_desc_v1,
    out_error: cynapsa_buffer_desc_v1,
    operation: str,
    *,
    allow_wait_timeout: bool = False,
) -> bytes | None:
    """Consume one result/error descriptor pair without retiring either twice."""

    retired: set[int] = set()
    if native_status == CYNAPSA_STATUS_V1_WAIT_TIMEOUT:
        if allow_wait_timeout and _descriptor_is_empty(out_result) and _descriptor_is_empty(out_error):
            return None
        error = NativeError(
            CYNAPSA_STATUS_V1_ERROR,
            "invalid_native_output",
            f"The native core returned a nonconforming wait status for {operation}",
        )
        for descriptor in (out_result, out_error):
            if not _descriptor_is_empty(descriptor):
                try:
                    _copy_and_free_buffer(library, descriptor, retired)
                except NativeError as cleanup:
                    error = _with_cleanup_error(error, cleanup)
        raise error

    if native_status != CYNAPSA_STATUS_V1_OK:
        error = _decode_native_error(library, native_status, out_error, retired)
        error = _with_detail(error, "failure_kind", "abi_call")
        error = _with_detail(error, "operation", operation)
        if not _descriptor_is_empty(out_result):
            try:
                _copy_and_free_buffer(library, out_result, retired)
            except NativeError as cleanup:
                error = _with_cleanup_error(error, cleanup)
        raise error

    if not _descriptor_is_empty(out_error):
        error = _invalid_success_error(library, out_error, operation, retired)
        if not _descriptor_is_empty(out_result):
            try:
                _copy_and_free_buffer(library, out_result, retired)
            except NativeError as cleanup:
                error = _with_cleanup_error(error, cleanup)
        raise error
    return _copy_and_free_buffer(library, out_result, retired)


def _cleanup_untrusted_descriptors(
    library: Any,
    descriptors: tuple[cynapsa_buffer_desc_v1, ...],
    error: NativeError,
) -> NativeError:
    """Retire outputs populated before a foreign-call exception escaped."""

    retired: set[int] = set()
    for descriptor in descriptors:
        if _descriptor_is_empty(descriptor):
            continue
        try:
            _copy_and_free_buffer(library, descriptor, retired)
        except NativeError as cleanup:
            error = _with_cleanup_error(error, cleanup)
    return error


class _PendingCommands:
    """Core-local routing registry with bounded terminal-ID history."""

    def __init__(self, capacity: int) -> None:
        self._lock = threading.RLock()
        self._pending: dict[str, CommandFuture] = {}
        self._retired: set[str] = set()
        self._retired_order: deque[str] = deque()
        self._capacity = capacity
        self._terminal_factory: Any | None = None

    def _retire(self, command_id: str) -> None:
        if command_id in self._retired:
            return
        if len(self._retired_order) == self._capacity:
            self._retired.remove(self._retired_order.popleft())
        self._retired.add(command_id)
        self._retired_order.append(command_id)

    def reserve(self, future: CommandFuture) -> None:
        with self._lock:
            if self._terminal_factory is not None:
                raise self._terminal_factory(future.command_id)
            command_id = future.command_id
            if command_id in self._pending or command_id in self._retired:
                raise NativeError(
                    CYNAPSA_STATUS_V1_ERROR,
                    "duplicate_command_id",
                    "The command identifier is already pending or retired on this core",
                    {"command_id": command_id},
                )
            self._pending[command_id] = future

    def abandon(self, future: CommandFuture, *, retire: bool = False) -> None:
        with self._lock:
            if self._pending.get(future.command_id) is future:
                del self._pending[future.command_id]
                if retire:
                    self._retire(future.command_id)

    def route(self, completion: CommandCompletion) -> CommandFuture:
        with self._lock:
            future = self._pending.pop(completion.command_id, None)
            if future is None:
                code = (
                    "duplicate_completion"
                    if completion.command_id in self._retired
                    else "unknown_completion"
                )
                raise NativeError(
                    CYNAPSA_STATUS_V1_ERROR,
                    code,
                    "The native core returned a completion without a matching pending command",
                    {"command_id": completion.command_id},
                )
            self._retire(completion.command_id)
        if not future._complete(completion):
            raise NativeError(
                CYNAPSA_STATUS_V1_ERROR,
                "duplicate_completion",
                "The native core returned a duplicate command completion",
                {"command_id": completion.command_id},
            )
        return future

    def get(self, future: CommandFuture) -> bool:
        with self._lock:
            return self._pending.get(future.command_id) is future

    def fail_all(self, error_factory: Any, *, terminal: bool = False) -> None:
        with self._lock:
            if terminal and self._terminal_factory is None:
                self._terminal_factory = error_factory
            futures = tuple(self._pending.values())
            self._pending.clear()
            for future in futures:
                self._retire(future.command_id)
        for future in futures:
            future._terminate(error_factory(future.command_id))

    def count(self) -> int:
        with self._lock:
            return len(self._pending)


class NativeCore:
    """Own a single native core handle and enforce its shutdown contract."""

    def __init__(self, library: Any, handle: int, queue_limit: int) -> None:
        if not handle:
            raise ValueError("a native core handle must be nonzero")
        self._library = library
        self._handle = int(handle)
        self._state = "created"
        self._lock = threading.RLock()
        self._shutdown_call_lock = threading.Lock()
        self._close_lock = threading.Lock()
        self._callback_call_lock = threading.RLock()
        self._delivery_lock = threading.RLock()
        self._completion_poll_lock = threading.Lock()
        self._event_poll_lock = threading.Lock()
        self._pending = _PendingCommands(queue_limit)
        self._queue_limit = queue_limit
        self._delivery_mode: str | None = None
        self._callbacks: CallbackDispatcher | None = None
        self._last_callback_error: NativeError | None = None

    @classmethod
    def create(
        cls,
        config: Mapping[str, Any],
        *,
        library: Any | None = None,
        library_path: str | os.PathLike[str] | None = None,
    ) -> NativeCore:
        if library is not None and library_path is not None:
            raise TypeError("library and library_path are mutually exclusive")
        data = _json_bytes(config)
        # Retain the already-validated snapshot. A custom or concurrently
        # mutated Mapping must not change registry capacity after serialization.
        queue_limit = int(json.loads(data)["queue_limit"])
        if library is None:
            from .loader import load_library

            library = load_library(library_path)
        else:
            configure_library(library)
            validate_abi_version(
                library,
                decode_error=lambda status, descriptor: decode_native_error(
                    library, status, descriptor
                ),
            )

        input_pointer, input_owner = _input_pointer(data)
        out_core = ctypes.c_uint64()
        out_error = cynapsa_buffer_desc_v1()
        try:
            try:
                status = int(
                    library.cynapsa_v1_core_create(
                        input_pointer,
                        len(data),
                        ctypes.byref(out_core),
                        ctypes.byref(out_error),
                    )
                )
            except Exception as exc:
                error = NativeError(
                    CYNAPSA_STATUS_V1_ERROR,
                    "abi_call_failed",
                    "The native core creation call failed",
                    {"operation": "core_create"},
                )
                error = _cleanup_untrusted_descriptors(
                    library, (out_error,), error
                )
                if out_core.value:
                    error = cls._cleanup_partial_create(
                        library, int(out_core.value), error
                    )
                raise error from exc
        finally:
            # Keep the backing ctypes array strongly referenced for the complete
            # borrowed native call, independent of pointer implementation details.
            del input_owner

        if status != CYNAPSA_STATUS_V1_OK:
            error = decode_native_error(library, status, out_error)
            if out_core.value:
                error = cls._cleanup_partial_create(
                    library, int(out_core.value), error
                )
            raise error
        if not _descriptor_is_empty(out_error):
            error = _invalid_success_error(library, out_error, "core creation")
            if out_core.value:
                error = cls._cleanup_partial_create(
                    library, int(out_core.value), error
                )
            raise error
        if not out_core.value:
            raise NativeError(
                CYNAPSA_STATUS_V1_ERROR,
                "invalid_native_output",
                "The native core returned success without a core handle",
            )
        return cls(library, int(out_core.value), queue_limit)

    @staticmethod
    def _cleanup_partial_create(
        library: Any, handle: int, original_error: NativeError
    ) -> NativeError:
        cleanup_errors: list[NativeError] = []
        shutdown_error = cynapsa_buffer_desc_v1()
        try:
            shutdown_status = int(
                library.cynapsa_v1_core_shutdown(
                    handle, 0, ctypes.byref(shutdown_error)
                )
            )
        except Exception:
            cleanup_errors.append(
                _cleanup_untrusted_descriptors(
                    library,
                    (shutdown_error,),
                    NativeError(
                        CYNAPSA_STATUS_V1_ERROR,
                        "abi_call_failed",
                        "The partial-create shutdown call failed",
                        {"operation": "core_shutdown"},
                    ),
                )
            )
            return _with_detail(
                original_error,
                "partial_cleanup",
                _error_summary(cleanup_errors[0]),
            )
        if shutdown_status != CYNAPSA_STATUS_V1_OK:
            cleanup_errors.append(
                decode_native_error(library, shutdown_status, shutdown_error)
            )
        else:
            # OK confirms closed even if the library also violates the output
            # descriptor rule. Consume that descriptor and continue to destroy.
            if not _descriptor_is_empty(shutdown_error):
                cleanup_errors.append(
                    _invalid_success_error(
                        library, shutdown_error, "partial-create shutdown"
                    )
                )
            destroy_error = cynapsa_buffer_desc_v1()
            destroy_call_failed = False
            try:
                destroy_status = int(
                    library.cynapsa_v1_core_destroy(
                        handle, ctypes.byref(destroy_error)
                    )
                )
            except Exception:
                destroy_call_failed = True
                cleanup_errors.append(
                    _cleanup_untrusted_descriptors(
                        library,
                        (destroy_error,),
                        NativeError(
                            CYNAPSA_STATUS_V1_ERROR,
                            "abi_call_failed",
                            "The partial-create destruction call failed",
                            {"operation": "core_destroy"},
                        ),
                    )
                )
            if not destroy_call_failed and destroy_status != CYNAPSA_STATUS_V1_OK:
                cleanup_errors.append(
                    decode_native_error(library, destroy_status, destroy_error)
                )
            elif not destroy_call_failed and not _descriptor_is_empty(destroy_error):
                cleanup_errors.append(
                    _invalid_success_error(
                        library, destroy_error, "partial-create destruction"
                    )
                )

        if cleanup_errors:
            first = _error_summary(cleanup_errors[0])
            if len(cleanup_errors) > 1:
                first["additional_errors"] = [
                    _error_summary(error) for error in cleanup_errors[1:]
                ]
            return _with_detail(original_error, "partial_cleanup", first)
        return original_error

    @property
    def handle(self) -> int:
        with self._lock:
            return self._handle

    @property
    def lifecycle(self) -> str:
        with self._lock:
            return self._state

    def _require_live(self, operation: str) -> None:
        if self._state == "destroyed":
            raise NativeError(
                CYNAPSA_STATUS_V1_ERROR,
                "core_destroyed",
                f"Cannot {operation} a destroyed native core",
            )

    def start(self) -> None:
        with self._lock:
            self._require_live("start")
            if self._state != "created":
                raise NativeError(
                    CYNAPSA_STATUS_V1_ERROR,
                    "invalid_lifecycle",
                    f"Cannot start a native core in the {self._state} state",
                    {"lifecycle": self._state},
                )
            out_error = cynapsa_buffer_desc_v1()
            try:
                status = int(
                    self._library.cynapsa_v1_core_start(
                        self._handle, ctypes.byref(out_error)
                    )
                )
            except Exception as exc:
                error = NativeError(
                    CYNAPSA_STATUS_V1_ERROR,
                    "abi_call_failed",
                    "The native core startup call failed",
                    {"operation": "core_start"},
                )
                error = _cleanup_untrusted_descriptors(
                    self._library, (out_error,), error
                )
                raise error from exc
            if status != CYNAPSA_STATUS_V1_OK:
                raise decode_native_error(self._library, status, out_error)
            # The status confirms startup even if the descriptor is malformed.
            self._state = "started"
            if not _descriptor_is_empty(out_error):
                raise _invalid_success_error(
                    self._library, out_error, "core startup"
                )

    def status(self) -> dict[str, Any]:
        with self._lock:
            self._require_live("read status from")
            out_result = cynapsa_buffer_desc_v1()
            out_error = cynapsa_buffer_desc_v1()
            try:
                native_status = int(
                    self._library.cynapsa_v1_core_status(
                        self._handle,
                        ctypes.byref(out_result),
                        ctypes.byref(out_error),
                    )
                )
            except Exception as exc:
                error = NativeError(
                    CYNAPSA_STATUS_V1_ERROR,
                    "abi_call_failed",
                    "The native core status call failed",
                    {"operation": "core_status"},
                )
                error = _cleanup_untrusted_descriptors(
                    self._library, (out_result, out_error), error
                )
                raise error from exc
            retired: set[int] = set()
            if native_status != CYNAPSA_STATUS_V1_OK:
                error = _decode_native_error(
                    self._library, native_status, out_error, retired
                )
                if not _descriptor_is_empty(out_result):
                    try:
                        _copy_and_free_buffer(self._library, out_result, retired)
                    except NativeError as cleanup:
                        error = _with_cleanup_error(error, cleanup)
                raise error
            if not _descriptor_is_empty(out_error):
                error = _invalid_success_error(
                    self._library,
                    out_error,
                    "core status",
                    retired,
                )
                if not _descriptor_is_empty(out_result):
                    try:
                        _copy_and_free_buffer(self._library, out_result, retired)
                    except NativeError as cleanup:
                        error = _with_cleanup_error(error, cleanup)
                raise error
            raw = _copy_and_free_buffer(self._library, out_result, retired)
            document = _decode_status(raw)
            if document["status"]["lifecycle"] == "closed":
                self._state = "closed"
                # Callback items copied before native closure may still be in
                # the bounded Python handoff.  shutdown()/destroy() quiesces
                # that registration before terminating any remainder.
                if self._callbacks is None:
                    self._fail_pending_for_shutdown()
            return document

    @property
    def pending_command_count(self) -> int:
        return self._pending.count()

    @property
    def delivery_mode(self) -> str | None:
        """Current native queue owner (primarily useful for diagnostics/tests)."""

        with self._delivery_lock:
            if self._delivery_mode in {"callback_registering", "callback_clearing"}:
                return "callbacks"
            return self._delivery_mode

    @property
    def callback_dispatch_error(self) -> NativeError | None:
        with self._delivery_lock:
            if self._callbacks is not None and self._callbacks.fatal_error is not None:
                return self._callbacks.fatal_error
            return self._last_callback_error

    def _reject_callback_reentrancy(self, operation: str) -> None:
        callbacks = self._callbacks
        if callbacks is not None and (
            callbacks.in_callback_thread() or callbacks.in_worker_thread()
        ):
            raise NativeError(
                CYNAPSA_STATUS_V1_ERROR,
                "callback_reentrant_operation",
                f"Cannot {operation} the same native core from its callback dispatcher",
                {"operation": operation},
            )

    def register_callbacks(self, capacity: int | None = None) -> None:
        """Register the bounded native delivery path for completions and events."""

        if capacity is None:
            capacity = self._queue_limit
        capacity = _validated_integer(
            capacity, "capacity", minimum=1, maximum=MAX_QUEUE_LIMIT
        )
        # Reject before taking the teardown mutex.  Native clear/shutdown may
        # hold that mutex while waiting for this exact callback to return.
        self._reject_callback_reentrancy("register callbacks on")
        with self._callback_call_lock:
            self._reject_callback_reentrancy("register callbacks on")
            with self._delivery_lock:
                with self._lock:
                    self._require_live("register callbacks on")
                    if self._state in {"closing", "closed"}:
                        raise NativeError(
                            CYNAPSA_STATUS_V1_ERROR,
                            "shutdown_in_progress",
                            "Cannot register callbacks after native core shutdown has begun",
                            {"lifecycle": self._state},
                        )
                    handle = self._handle
                if self._callbacks is not None or self._delivery_mode in {
                    "callback_registering", "callbacks", "callback_clearing"
                }:
                    raise NativeError(
                        CYNAPSA_STATUS_V1_ERROR,
                        "callbacks_already_registered",
                        "Native callbacks are already registered on this core",
                    )
                if not self._completion_poll_lock.acquire(blocking=False):
                    raise NativeError(
                        CYNAPSA_STATUS_V1_ERROR,
                        "delivery_mode_conflict",
                        "Cannot switch to callbacks while completion polling is active",
                    )
                event_acquired = self._event_poll_lock.acquire(blocking=False)
                if not event_acquired:
                    self._completion_poll_lock.release()
                    raise NativeError(
                        CYNAPSA_STATUS_V1_ERROR,
                        "delivery_mode_conflict",
                        "Cannot switch to callbacks while event polling is active",
                    )
                previous_mode = self._delivery_mode
                self._delivery_mode = "callback_registering"
                try:
                    dispatcher = CallbackDispatcher(
                        self._library, self._pending, capacity
                    )
                except BaseException as exc:
                    self._delivery_mode = previous_mode
                    self._event_poll_lock.release()
                    self._completion_poll_lock.release()
                    if isinstance(exc, Exception):
                        raise NativeError(
                            CYNAPSA_STATUS_V1_ERROR,
                            "callback_worker_start_failed",
                            "The callback dispatcher worker could not start",
                            {"operation": "callback_worker_start"},
                        ) from exc
                    raise
                self._callbacks = dispatcher

            out_error = cynapsa_buffer_desc_v1()
            try:
                try:
                    assert dispatcher.callback is not None
                    status = int(
                        self._library.cynapsa_v1_callbacks_register(
                            handle,
                            dispatcher.callback,
                            dispatcher.completion_token,
                            dispatcher.event_token,
                            dispatcher.diagnostic_token,
                            capacity,
                            ctypes.byref(out_error),
                        )
                    )
                except Exception as exc:
                    error = NativeError(
                        CYNAPSA_STATUS_V1_ERROR,
                        "abi_call_failed",
                        "The native callback registration call failed",
                        {"operation": "callbacks_register"},
                    )
                    error = _cleanup_untrusted_descriptors(
                        self._library, (out_error,), error
                    )
                    # The foreign call may have installed the function pointer
                    # before raising.  Best-effort clear keeps its lifetime safe.
                    clear_error = cynapsa_buffer_desc_v1()
                    try:
                        clear_status = int(
                            self._library.cynapsa_v1_callbacks_clear(
                                handle, 0, ctypes.byref(clear_error)
                            )
                        )
                        if clear_status != CYNAPSA_STATUS_V1_OK:
                            error = _with_cleanup_error(
                                error,
                                decode_native_error(
                                    self._library, clear_status, clear_error
                                ),
                            )
                            error = _with_detail(
                                error, "callback_state_retained", True
                            )
                            with self._delivery_lock:
                                self._delivery_mode = "callback_clearing"
                            raise error from exc
                        elif not _descriptor_is_empty(clear_error):
                            error = _with_cleanup_error(
                                error,
                                _invalid_success_error(
                                    self._library,
                                    clear_error,
                                    "callback registration rollback",
                                ),
                            )
                    except Exception:
                        error = _with_detail(
                            error, "callback_state_retained", True
                        )
                        with self._delivery_lock:
                            self._delivery_mode = "callback_clearing"
                        raise error from exc
                    dispatcher.abandon_rejected_registration()
                    with self._delivery_lock:
                        self._callbacks = None
                        self._delivery_mode = previous_mode
                    raise error from exc

                if status != CYNAPSA_STATUS_V1_OK:
                    try:
                        error = decode_native_error(self._library, status, out_error)
                    finally:
                        dispatcher.abandon_rejected_registration()
                        with self._delivery_lock:
                            self._callbacks = None
                            self._delivery_mode = previous_mode
                    raise error

                # Success makes the callback pointer native-owned even when the
                # auxiliary error output is malformed, so publish ownership first.
                with self._delivery_lock:
                    self._delivery_mode = "callbacks"
                if not _descriptor_is_empty(out_error):
                    raise _with_detail(
                        _invalid_success_error(
                            self._library, out_error, "callback registration"
                        ),
                        "callback_state_retained",
                        True,
                    )
            finally:
                self._event_poll_lock.release()
                self._completion_poll_lock.release()

    def _clear_callbacks_locked(self, timeout: int) -> None:
        with self._delivery_lock:
            dispatcher = self._callbacks
            if dispatcher is None:
                return
            if dispatcher.in_callback_thread() or dispatcher.in_worker_thread():
                raise NativeError(
                    CYNAPSA_STATUS_V1_ERROR,
                    "callback_reentrant_operation",
                    "Cannot clear native callbacks from the same C callback",
                    {"operation": "callbacks_clear"},
                )
            handle = self._handle
            self._delivery_mode = "callback_clearing"

        out_error = cynapsa_buffer_desc_v1()
        try:
            status = int(
                self._library.cynapsa_v1_callbacks_clear(
                    handle, timeout, ctypes.byref(out_error)
                )
            )
        except Exception as exc:
            error = NativeError(
                CYNAPSA_STATUS_V1_ERROR,
                "abi_call_failed",
                "The native callback clear call failed",
                {"operation": "callbacks_clear"},
            )
            raise _cleanup_untrusted_descriptors(
                self._library, (out_error,), error
            ) from exc
        if status != CYNAPSA_STATUS_V1_OK:
            # Native admission is already closing after a timeout.  Keep the
            # callback and all owned state alive so a later clear can join it.
            raise decode_native_error(self._library, status, out_error)

        output_error: NativeError | None = None
        if not _descriptor_is_empty(out_error):
            output_error = _invalid_success_error(
                self._library, out_error, "callback clear"
            )
        dispatcher.native_cleared_and_join()
        with self._delivery_lock:
            if self._callbacks is dispatcher:
                self._last_callback_error = dispatcher.fatal_error
                self._callbacks = None
                self._delivery_mode = None
        if output_error is not None:
            raise output_error

    def clear_callbacks(self, timeout_ms: int = 0) -> None:
        """Quiesce native delivery, join the worker, and release callback state."""

        timeout = _validated_timeout(timeout_ms)
        self._reject_callback_reentrancy("clear native callbacks on")
        with self._callback_call_lock:
            self._clear_callbacks_locked(timeout)

    def _next_callback_event_for_test(
        self, timeout: float | None = None
    ) -> CallbackEventItem:
        """Internal raw-event consumer seam; no user handlers run in milestone 3."""

        with self._delivery_lock:
            dispatcher = self._callbacks
        if dispatcher is None:
            raise RuntimeError("native callbacks are not registered")
        return dispatcher.next_event_for_test(timeout)

    def _next_callback_event(
        self, *, diagnostic: bool, timeout: float | None = None
    ) -> CallbackEventItem:
        """Consume one raw callback stream without merging stream order."""

        with self._delivery_lock:
            dispatcher = self._callbacks
        if dispatcher is None:
            raise RuntimeError("native callbacks are not registered")
        if diagnostic:
            return dispatcher.next_diagnostic(timeout)
        return dispatcher.next_event(timeout)

    def submit(
        self,
        command_name: str,
        sdk_session_id: str,
        args: Mapping[str, Any] | None = None,
        *,
        command_id: str | None = None,
    ) -> CommandFuture:
        """Submit one low-level V1 command and return its routed future."""

        identifier, data = encode_command(
            command_name,
            sdk_session_id,
            args,
            command_id=command_id,
        )
        future = CommandFuture(identifier, command_name, self._cancel_future)
        self._reject_callback_reentrancy("submit to")
        with self._lock:
            self._reject_callback_reentrancy("submit to")
            self._require_live("submit to")
            if self._state in {"closing", "closed"}:
                raise NativeError(
                    CYNAPSA_STATUS_V1_ERROR,
                    "shutdown_in_progress",
                    "Cannot submit a command after native core shutdown has begun",
                    {"lifecycle": self._state},
                )
            self._pending.reserve(future)
            input_pointer, input_owner = _input_pointer(data)
            out_result = cynapsa_buffer_desc_v1()
            out_error = cynapsa_buffer_desc_v1()
            try:
                try:
                    native_status = int(
                        self._library.cynapsa_v1_core_submit(
                            self._handle,
                            input_pointer,
                            len(data),
                            ctypes.byref(out_result),
                            ctypes.byref(out_error),
                        )
                    )
                except Exception as exc:
                    error = NativeError(
                        CYNAPSA_STATUS_V1_ERROR,
                        "abi_call_failed",
                        "The native core submission call failed",
                        {"operation": "core_submit"},
                    )
                    error = _cleanup_untrusted_descriptors(
                        self._library, (out_result, out_error), error
                    )
                    raise error from exc
                raw = _consume_document_outputs(
                    self._library,
                    native_status,
                    out_result,
                    out_error,
                    "core submission",
                )
                assert raw is not None
                admission = decode_admission(raw)
            except BaseException:
                self._pending.abandon(
                    future,
                    retire=native_status == CYNAPSA_STATUS_V1_OK
                    if "native_status" in locals()
                    else False,
                )
                raise
            finally:
                del input_owner

            if admission.command_id != identifier:
                self._pending.abandon(future, retire=True)
                cleanup: NativeError | None = None
                if admission.accepted and admission.command_handle is not None:
                    try:
                        self._cancel_handle(admission.command_handle)
                    except NativeError as exc:
                        cleanup = exc
                error = NativeError(
                    CYNAPSA_STATUS_V1_ERROR,
                    "mismatched_admission",
                    "The native core admitted a different command identifier",
                    {"expected": identifier, "actual": admission.command_id},
                )
                if cleanup is not None:
                    error = _with_cleanup_error(error, cleanup)
                raise error
            if not admission.accepted:
                self._pending.abandon(future)
                assert admission.error is not None
                raise admission.error
            assert admission.command_handle is not None
            future._admit(admission.command_handle)
            return future

    def poll_completion(self, timeout_ms: int = 0) -> CommandCompletion | None:
        """Consume at most one native completion and route it to its future."""

        timeout = _validated_timeout(timeout_ms)
        if not self._completion_poll_lock.acquire(blocking=False):
            raise NativeError(
                CYNAPSA_STATUS_V1_ERROR,
                "poll_in_progress",
                "Another completion poll already owns this core's native queue",
            )
        try:
            with self._delivery_lock:
                if self._delivery_mode in {
                    "callback_registering", "callbacks", "callback_clearing"
                }:
                    raise NativeError(
                        CYNAPSA_STATUS_V1_ERROR,
                        "delivery_mode_conflict",
                        "Cannot poll a native queue while callbacks own this core",
                    )
                self._delivery_mode = "polling"
            with self._lock:
                self._require_live("poll completions from")
                if self._state in {"closing", "closed"}:
                    raise NativeError(
                        CYNAPSA_STATUS_V1_ERROR,
                        "core_closed",
                        "Cannot poll completions after native core shutdown has begun",
                    )
                handle = self._handle
            out_result = cynapsa_buffer_desc_v1()
            out_error = cynapsa_buffer_desc_v1()
            try:
                native_status = int(
                    self._library.cynapsa_v1_core_next_completion(
                        handle,
                        timeout,
                        ctypes.byref(out_result),
                        ctypes.byref(out_error),
                    )
                )
            except Exception as exc:
                error = NativeError(
                    CYNAPSA_STATUS_V1_ERROR,
                    "abi_call_failed",
                    "The native completion polling call failed",
                    {"operation": "core_next_completion"},
                )
                error = _cleanup_untrusted_descriptors(
                    self._library, (out_result, out_error), error
                )
                raise error from exc
            raw = _consume_document_outputs(
                self._library,
                native_status,
                out_result,
                out_error,
                "completion polling",
                allow_wait_timeout=True,
            )
            if raw is None:
                return None
            completion = decode_completion(raw)
            self._pending.route(completion)
            return completion
        finally:
            self._completion_poll_lock.release()

    def cancel(self, future: CommandFuture) -> bool:
        """Cancel a future through the direct opaque-handle native export."""

        if not isinstance(future, CommandFuture):
            raise TypeError("future must be a CommandFuture")
        return self._cancel_future(future)

    def _cancel_future(self, future: CommandFuture) -> bool:
        self._reject_callback_reentrancy("cancel a command on")
        if not self._pending.get(future):
            return False
        handle = future._claim_cancel()
        if handle is None:
            return False
        try:
            self._cancel_handle(handle)
        except NativeError:
            if future.done():
                return False
            future._cancel_failed()
            raise
        return True

    def _cancel_handle(self, command_handle: str) -> None:
        data = encode_cancel(command_handle)
        with self._lock:
            self._require_live("cancel a command on")
            handle = self._handle
        input_pointer, input_owner = _input_pointer(data)
        out_error = cynapsa_buffer_desc_v1()
        try:
            try:
                native_status = int(
                    self._library.cynapsa_v1_core_cancel(
                        handle,
                        input_pointer,
                        len(data),
                        ctypes.byref(out_error),
                    )
                )
            except Exception as exc:
                error = NativeError(
                    CYNAPSA_STATUS_V1_ERROR,
                    "abi_call_failed",
                    "The native command cancellation call failed",
                    {"operation": "core_cancel"},
                )
                error = _cleanup_untrusted_descriptors(
                    self._library, (out_error,), error
                )
                raise error from exc
        finally:
            del input_owner
        if native_status != CYNAPSA_STATUS_V1_OK:
            raise _with_detail(
                decode_native_error(self._library, native_status, out_error),
                "failure_kind",
                "abi_call",
            )
        if not _descriptor_is_empty(out_error):
            raise _invalid_success_error(
                self._library, out_error, "command cancellation"
            )

    def _fail_pending_for_shutdown(self) -> None:
        self._pending.fail_all(
            lambda command_id: NativeError(
                CYNAPSA_STATUS_V1_ERROR,
                "shutdown_in_progress",
                "The native core closed before the command completed",
                {
                    "command_id": command_id,
                    "retryable": False,
                    "stage": "shutdown",
                    "local_or_remote": "local",
                    "terminal": True,
                },
            ),
            terminal=True,
        )

    def _shutdown_completion_pump(
        self,
        handle: int,
        stop: threading.Event,
        errors: list[NativeError],
    ) -> None:
        while not stop.is_set():
            with self._completion_poll_lock:
                if stop.is_set():
                    return
                out_result = cynapsa_buffer_desc_v1()
                out_error = cynapsa_buffer_desc_v1()
                try:
                    status = int(
                        self._library.cynapsa_v1_core_next_completion(
                            handle,
                            10,
                            ctypes.byref(out_result),
                            ctypes.byref(out_error),
                        )
                    )
                    raw = _consume_document_outputs(
                        self._library,
                        status,
                        out_result,
                        out_error,
                        "shutdown completion drain",
                        allow_wait_timeout=True,
                    )
                    if raw is not None:
                        self._pending.route(decode_completion(raw))
                except NativeError as exc:
                    if exc.code in {
                        "shutdown_in_progress",
                        "core_closed",
                        "invalid_handle",
                        "unknown_completion",
                        "duplicate_completion",
                    }:
                        if stop.wait(0.001):
                            return
                        continue
                    errors.append(exc)
                    return
                except Exception as exc:
                    error = NativeError(
                        CYNAPSA_STATUS_V1_ERROR,
                        "abi_call_failed",
                        "The native shutdown completion drain failed",
                        {"operation": "core_next_completion"},
                    )
                    errors.append(
                        _cleanup_untrusted_descriptors(
                            self._library, (out_result, out_error), error
                        )
                    )
                    return

    def _shutdown_event_pump(
        self,
        handle: int,
        stop: threading.Event,
        errors: list[NativeError],
    ) -> None:
        while not stop.is_set():
            with self._event_poll_lock:
                if stop.is_set():
                    return
                out_result = cynapsa_buffer_desc_v1()
                out_error = cynapsa_buffer_desc_v1()
                try:
                    status = int(
                        self._library.cynapsa_v1_core_next_event(
                            handle,
                            10,
                            ctypes.byref(out_result),
                            ctypes.byref(out_error),
                        )
                    )
                    _consume_document_outputs(
                        self._library,
                        status,
                        out_result,
                        out_error,
                        "shutdown event drain",
                        allow_wait_timeout=True,
                    )
                except NativeError as exc:
                    if exc.code in {
                        "shutdown_in_progress",
                        "core_closed",
                        "invalid_handle",
                    }:
                        if stop.wait(0.001):
                            return
                        continue
                    errors.append(exc)
                    return
                except Exception as exc:
                    error = NativeError(
                        CYNAPSA_STATUS_V1_ERROR,
                        "abi_call_failed",
                        "The native shutdown event drain failed",
                        {"operation": "core_next_event"},
                    )
                    errors.append(
                        _cleanup_untrusted_descriptors(
                            self._library, (out_result, out_error), error
                        )
                    )
                    return

    def shutdown(self, timeout_ms: int = 0) -> None:
        timeout = _validated_timeout(timeout_ms)
        self._reject_callback_reentrancy("shut down")
        with self._shutdown_call_lock:
            with self._callback_call_lock:
                self._reject_callback_reentrancy("shut down")
                with self._lock:
                    self._require_live("shut down")
                    already_closed = self._state == "closed"
                    if not already_closed:
                        self._state = "closing"
                    handle = self._handle
                if already_closed:
                    self._clear_callbacks_locked(timeout)
                    self._fail_pending_for_shutdown()
                    return

                with self._delivery_lock:
                    callback_owned = self._callbacks is not None
                stop_pumps = threading.Event()
                pump_errors: list[NativeError] = []
                pumps: tuple[threading.Thread, ...]
                if callback_owned:
                    pumps = ()
                else:
                    pumps = (
                        threading.Thread(
                            target=self._shutdown_completion_pump,
                            args=(handle, stop_pumps, pump_errors),
                            name="cynapsa-completion-drain",
                            daemon=True,
                        ),
                        threading.Thread(
                            target=self._shutdown_event_pump,
                            args=(handle, stop_pumps, pump_errors),
                            name="cynapsa-event-drain",
                            daemon=True,
                        ),
                    )
                for pump in pumps:
                    pump.start()
                out_error = cynapsa_buffer_desc_v1()
                try:
                    try:
                        status = int(
                            self._library.cynapsa_v1_core_shutdown(
                                handle, timeout, ctypes.byref(out_error)
                            )
                        )
                    except Exception as exc:
                        error = NativeError(
                            CYNAPSA_STATUS_V1_ERROR,
                            "abi_call_failed",
                            "The native core shutdown call failed",
                            {"operation": "core_shutdown"},
                        )
                        error = _cleanup_untrusted_descriptors(
                            self._library, (out_error,), error
                        )
                        raise error from exc
                finally:
                    stop_pumps.set()
                    for pump in pumps:
                        pump.join()
                if status != CYNAPSA_STATUS_V1_OK:
                    raise decode_native_error(self._library, status, out_error)

                # Native closure is authoritative.  Callback delivery remains
                # alive until this point and is quiesced before pending futures
                # that did not receive a terminal completion are failed.
                with self._lock:
                    self._state = "closed"
                output_error: NativeError | None = None
                if not _descriptor_is_empty(out_error):
                    output_error = _invalid_success_error(
                        self._library, out_error, "core shutdown"
                    )
                try:
                    self._clear_callbacks_locked(timeout)
                except NativeError as clear_error:
                    if output_error is not None:
                        clear_error = _with_cleanup_error(
                            clear_error, output_error
                        )
                    raise _with_detail(clear_error, "shutdown_closed", True)
                self._fail_pending_for_shutdown()
                if output_error is not None:
                    raise output_error
                if pump_errors:
                    raise _with_detail(pump_errors[0], "shutdown_closed", True)

    def destroy(self) -> None:
        self._reject_callback_reentrancy("destroy")
        with self._callback_call_lock:
            self._reject_callback_reentrancy("destroy")
            with self._lock:
                if self._state == "destroyed":
                    return
                if self._state != "closed":
                    raise NativeError(
                        CYNAPSA_STATUS_V1_ERROR,
                        "core_not_closed",
                        "The native core must complete shutdown before it can be destroyed",
                        {"lifecycle": self._state},
                    )
            # A status query can discover native closure without going through
            # shutdown().  Quiesce any registration before retiring the handle.
            self._clear_callbacks_locked(0)
            with self._lock:
                out_error = cynapsa_buffer_desc_v1()
                try:
                    status = int(
                        self._library.cynapsa_v1_core_destroy(
                            self._handle, ctypes.byref(out_error)
                        )
                    )
                except Exception as exc:
                    error = NativeError(
                        CYNAPSA_STATUS_V1_ERROR,
                        "abi_call_failed",
                        "The native core destruction call failed",
                        {"operation": "core_destroy"},
                    )
                    error = _cleanup_untrusted_descriptors(
                        self._library, (out_error,), error
                    )
                    raise error from exc
                if status != CYNAPSA_STATUS_V1_OK:
                    raise decode_native_error(self._library, status, out_error)
                # OK permanently retires the handle, independently of malformed
                # auxiliary output. Never issue another native call with it.
                self._state = "destroyed"
                self._handle = 0
                self._fail_pending_for_shutdown()
                if not _descriptor_is_empty(out_error):
                    raise _invalid_success_error(
                        self._library, out_error, "core destruction"
                    )

    def close(self, timeout_ms: int = 0) -> None:
        self._reject_callback_reentrancy("close")
        with self._close_lock:
            with self._lock:
                if self._state == "destroyed":
                    return
            self.shutdown(timeout_ms)
            self.destroy()

    def __enter__(self) -> NativeCore:
        with self._lock:
            self._require_live("enter")
            return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()
