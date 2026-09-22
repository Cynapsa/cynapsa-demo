"""Exact ``ctypes`` declarations for ``cynapsacore_v1.h``."""

from __future__ import annotations

import ctypes
from collections.abc import Callable
from typing import Any

from cynapsa.exceptions import NativeError

ABI_VERSION = 1

CYNAPSA_STATUS_V1_OK = 0
CYNAPSA_STATUS_V1_ERROR = 1
CYNAPSA_STATUS_V1_WAIT_TIMEOUT = 2

CYNAPSA_CALLBACK_V1_COMPLETION = 1
CYNAPSA_CALLBACK_V1_EVENT = 2
CYNAPSA_CALLBACK_V1_DIAGNOSTIC = 3

cynapsa_core_t = ctypes.c_uint64
cynapsa_buffer_t = ctypes.c_uint64


class cynapsa_buffer_desc_v1(ctypes.Structure):
    _fields_ = [
        ("buffer_handle", ctypes.c_uint64),
        ("byte_length", ctypes.c_uint64),
    ]


cynapsa_callback_v1 = ctypes.CFUNCTYPE(
    None,
    ctypes.c_uint64,
    ctypes.c_uint32,
    cynapsa_buffer_desc_v1,
)

BufferDescV1 = cynapsa_buffer_desc_v1
CallbackV1 = cynapsa_callback_v1

_U8_P = ctypes.POINTER(ctypes.c_uint8)
_U32_P = ctypes.POINTER(ctypes.c_uint32)
_U64_P = ctypes.POINTER(ctypes.c_uint64)
_DESC_P = ctypes.POINTER(cynapsa_buffer_desc_v1)

EXPORT_SIGNATURES: dict[str, tuple[list[Any], Any]] = {
    "cynapsa_v1_abi_version": ([_U32_P, _DESC_P], ctypes.c_int32),
    "cynapsa_v1_core_create": (
        [_U8_P, ctypes.c_uint64, _U64_P, _DESC_P],
        ctypes.c_int32,
    ),
    "cynapsa_v1_core_start": ([ctypes.c_uint64, _DESC_P], ctypes.c_int32),
    "cynapsa_v1_core_submit": (
        [ctypes.c_uint64, _U8_P, ctypes.c_uint64, _DESC_P, _DESC_P],
        ctypes.c_int32,
    ),
    "cynapsa_v1_core_cancel": (
        [ctypes.c_uint64, _U8_P, ctypes.c_uint64, _DESC_P],
        ctypes.c_int32,
    ),
    "cynapsa_v1_core_next_completion": (
        [ctypes.c_uint64, ctypes.c_int64, _DESC_P, _DESC_P],
        ctypes.c_int32,
    ),
    "cynapsa_v1_core_next_event": (
        [ctypes.c_uint64, ctypes.c_int64, _DESC_P, _DESC_P],
        ctypes.c_int32,
    ),
    "cynapsa_v1_core_status": (
        [ctypes.c_uint64, _DESC_P, _DESC_P],
        ctypes.c_int32,
    ),
    "cynapsa_v1_core_shutdown": (
        [ctypes.c_uint64, ctypes.c_int64, _DESC_P],
        ctypes.c_int32,
    ),
    "cynapsa_v1_core_destroy": ([ctypes.c_uint64, _DESC_P], ctypes.c_int32),
    "cynapsa_v1_payload_open": (
        [ctypes.c_uint64, _DESC_P, _DESC_P],
        ctypes.c_int32,
    ),
    "cynapsa_v1_payload_write": (
        [ctypes.c_uint64, _U8_P, ctypes.c_uint64, _DESC_P, _DESC_P],
        ctypes.c_int32,
    ),
    "cynapsa_v1_payload_finish": (
        [ctypes.c_uint64, _U8_P, ctypes.c_uint64, _DESC_P, _DESC_P],
        ctypes.c_int32,
    ),
    "cynapsa_v1_payload_read": (
        [ctypes.c_uint64, _U8_P, ctypes.c_uint64, _DESC_P, _DESC_P],
        ctypes.c_int32,
    ),
    "cynapsa_v1_payload_cancel": (
        [ctypes.c_uint64, _U8_P, ctypes.c_uint64, _DESC_P],
        ctypes.c_int32,
    ),
    "cynapsa_v1_payload_retain": (
        [ctypes.c_uint64, _U8_P, ctypes.c_uint64, _DESC_P],
        ctypes.c_int32,
    ),
    "cynapsa_v1_payload_release": (
        [ctypes.c_uint64, _U8_P, ctypes.c_uint64, _DESC_P],
        ctypes.c_int32,
    ),
    "cynapsa_v1_callbacks_register": (
        [
            ctypes.c_uint64,
            cynapsa_callback_v1,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint32,
            _DESC_P,
        ],
        ctypes.c_int32,
    ),
    "cynapsa_v1_callbacks_clear": (
        [ctypes.c_uint64, ctypes.c_int64, _DESC_P],
        ctypes.c_int32,
    ),
    "cynapsa_v1_buffer_read": (
        [
            ctypes.c_uint64,
            ctypes.c_uint64,
            _U8_P,
            ctypes.c_uint64,
            _U64_P,
            _DESC_P,
        ],
        ctypes.c_int32,
    ),
    "cynapsa_v1_buffer_free": ([ctypes.c_uint64, _DESC_P], ctypes.c_int32),
}

EXPORTS = tuple(EXPORT_SIGNATURES)


def configure_library(library: Any) -> Any:
    """Attach the exact V1 argument and result declarations to all exports."""

    missing: list[str] = []
    for name, (argtypes, restype) in EXPORT_SIGNATURES.items():
        try:
            function = getattr(library, name)
        except AttributeError:
            missing.append(name)
            continue
        function.argtypes = argtypes
        function.restype = restype

    if missing:
        raise NativeError(
            CYNAPSA_STATUS_V1_ERROR,
            "missing_native_exports",
            "The native core library is missing required ABI V1 exports",
            {"missing": missing},
        )
    return library


def validate_abi_version(
    library: Any,
    *,
    decode_error: Callable[[int, cynapsa_buffer_desc_v1], NativeError] | None = None,
) -> None:
    """Validate that a configured native library implements ABI V1."""

    def normalized_error(
        status: int, descriptor: cynapsa_buffer_desc_v1
    ) -> NativeError:
        if decode_error is not None:
            return decode_error(status, descriptor)
        # Keep the standalone ABI helper ownership-safe too. The import is
        # deferred because core.py imports these declarations.
        from .core import decode_native_error

        return decode_native_error(library, status, descriptor)

    out_version = ctypes.c_uint32()
    out_error = cynapsa_buffer_desc_v1()
    status = int(
        library.cynapsa_v1_abi_version(
            ctypes.byref(out_version), ctypes.byref(out_error)
        )
    )
    if status != CYNAPSA_STATUS_V1_OK:
        raise normalized_error(status, out_error)
    if out_error.buffer_handle or out_error.byte_length:
        # A successful conforming call resets this descriptor. Do not silently
        # accept a library that violates ownership guarantees.
        error = normalized_error(CYNAPSA_STATUS_V1_ERROR, out_error)
        raise NativeError(
            CYNAPSA_STATUS_V1_ERROR,
            "invalid_native_output",
            "The native core returned an error buffer with a successful status",
            {"native_error": {"code": error.code, "message": error.message}},
        )
    if out_version.value != ABI_VERSION:
        raise NativeError(
            CYNAPSA_STATUS_V1_ERROR,
            "abi_version_mismatch",
            f"Native core ABI version {out_version.value} is incompatible; expected {ABI_VERSION}",
            {"expected": ABI_VERSION, "actual": int(out_version.value)},
        )
