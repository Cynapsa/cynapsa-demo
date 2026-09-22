from __future__ import annotations

import ctypes

import pytest

from cynapsa.exceptions import NativeError
from cynapsa.native.abi import (
    ABI_VERSION,
    CYNAPSA_CALLBACK_V1_COMPLETION,
    CYNAPSA_CALLBACK_V1_DIAGNOSTIC,
    CYNAPSA_CALLBACK_V1_EVENT,
    CYNAPSA_STATUS_V1_ERROR,
    CYNAPSA_STATUS_V1_OK,
    CYNAPSA_STATUS_V1_WAIT_TIMEOUT,
    EXPORT_SIGNATURES,
    configure_library,
    cynapsa_buffer_desc_v1,
    cynapsa_callback_v1,
    validate_abi_version,
)

from conftest import FakeLibrary


def test_complete_exact_abi_declarations(fake_library: FakeLibrary) -> None:
    configure_library(fake_library)

    assert len(EXPORT_SIGNATURES) == 21
    for name, (argtypes, restype) in EXPORT_SIGNATURES.items():
        function = getattr(fake_library, name)
        assert function.argtypes == argtypes
        assert function.restype is restype is ctypes.c_int32

    assert cynapsa_buffer_desc_v1._fields_ == [
        ("buffer_handle", ctypes.c_uint64),
        ("byte_length", ctypes.c_uint64),
    ]
    assert issubclass(cynapsa_callback_v1, ctypes._CFuncPtr)
    assert ABI_VERSION == 1
    assert (CYNAPSA_STATUS_V1_OK, CYNAPSA_STATUS_V1_ERROR, CYNAPSA_STATUS_V1_WAIT_TIMEOUT) == (0, 1, 2)
    assert (CYNAPSA_CALLBACK_V1_COMPLETION, CYNAPSA_CALLBACK_V1_EVENT, CYNAPSA_CALLBACK_V1_DIAGNOSTIC) == (1, 2, 3)


def test_missing_export_is_actionable(fake_library: FakeLibrary) -> None:
    del fake_library.cynapsa_v1_core_start

    with pytest.raises(NativeError, match="missing required") as raised:
        configure_library(fake_library)

    assert raised.value.code == "missing_native_exports"
    assert raised.value.details["missing"] == ("cynapsa_v1_core_start",)


def test_abi_mismatch(fake_library: FakeLibrary) -> None:
    fake_library.abi_version = 2
    configure_library(fake_library)

    with pytest.raises(NativeError) as raised:
        validate_abi_version(fake_library)

    assert raised.value.code == "abi_version_mismatch"
    assert raised.value.details == {"expected": 1, "actual": 2}


def test_abi_error_is_decoded_and_freed(fake_library: FakeLibrary) -> None:
    fake_library.queue(
        "abi_version",
        CYNAPSA_STATUS_V1_ERROR,
        {"code": "core_error", "message": "Version lookup failed"},
    )
    configure_library(fake_library)

    with pytest.raises(NativeError) as raised:
        validate_abi_version(fake_library)

    assert raised.value.code == "core_error"
    assert list(fake_library.free_counts.values()) == [1]


def test_successful_abi_call_rejects_malformed_error_descriptor(
    fake_library: FakeLibrary,
) -> None:
    original = fake_library.cynapsa_v1_abi_version.implementation

    def malformed(*args: object) -> int:
        status = original(*args)
        args[1]._obj.byte_length = 1  # type: ignore[attr-defined]
        return status

    fake_library.cynapsa_v1_abi_version.implementation = malformed
    configure_library(fake_library)
    with pytest.raises(NativeError) as raised:
        validate_abi_version(fake_library)
    assert raised.value.code == "invalid_native_output"
