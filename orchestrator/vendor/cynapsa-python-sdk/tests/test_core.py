from __future__ import annotations

import json
import threading
from collections.abc import Mapping
from typing import Any

import pytest

from cynapsa.exceptions import NativeError
from cynapsa.native.abi import (
    CYNAPSA_STATUS_V1_ERROR,
    CYNAPSA_STATUS_V1_OK,
    CYNAPSA_STATUS_V1_WAIT_TIMEOUT,
    cynapsa_buffer_desc_v1,
)
from cynapsa.native.command import PUBLIC_ERROR_MESSAGES
from cynapsa.native.core import (
    MAX_PAYLOAD_BYTES,
    MAX_QUEUE_LIMIT,
    MAX_TIMEOUT_MS,
    NativeCore,
    copy_and_free_buffer,
    decode_native_error,
)

from conftest import FakeLibrary


def test_successful_lifecycle_and_context_manager(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = NativeCore.create(core_config, library=fake_library)
    assert json.loads(fake_library.create_input or b"") == core_config
    assert core.handle == 42
    assert core.lifecycle == "created"

    with core as entered:
        assert entered is core
        core.start()
        assert core.status()["status"]["lifecycle"] == "ready"

    assert core.handle == 0
    assert core.lifecycle == "destroyed"
    assert fake_library.calls["core_start"] == 1
    assert fake_library.calls["core_status"] == 1
    assert fake_library.calls["core_shutdown"] == 1
    assert fake_library.calls["core_destroy"] == 1
    assert all(count == 1 for count in fake_library.free_counts.values())


def test_normalized_error_and_error_buffer_freed_once(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    fake_library.queue(
        "core_start",
        CYNAPSA_STATUS_V1_ERROR,
        {
            "code": "queue_full",
            "message": "The local queue is full",
            "retryable": False,
            "stage": "command",
            "local_or_remote": "local",
        },
    )
    core = NativeCore.create(core_config, library=fake_library)

    with pytest.raises(NativeError) as raised:
        core.start()

    assert raised.value.status == CYNAPSA_STATUS_V1_ERROR
    assert raised.value.code == "queue_full"
    assert raised.value.message == "The local queue is full"
    assert raised.value.details == {
        "retryable": False,
        "stage": "command",
        "local_or_remote": "local",
    }
    assert list(fake_library.free_counts.values()) == [1]


def test_error_buffer_freed_once_on_decode_failure(fake_library: FakeLibrary) -> None:
    handle = fake_library.add_buffer(b"not-json")
    from cynapsa.native.abi import cynapsa_buffer_desc_v1

    descriptor = cynapsa_buffer_desc_v1(handle, 8)
    error = decode_native_error(fake_library, CYNAPSA_STATUS_V1_ERROR, descriptor)

    assert error.code == "native_error_decode_failed"
    assert fake_library.free_counts[handle] == 1


def test_status_buffer_freed_once_on_decode_failure(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    fake_library.status_document = b"{"  # Invalid JSON after a successful copy.
    core = NativeCore.create(core_config, library=fake_library)

    with pytest.raises(NativeError) as raised:
        core.status()

    assert raised.value.code == "native_result_decode_failed"
    assert list(fake_library.free_counts.values()) == [1]


def test_wait_timeout_is_distinct_and_does_not_destroy(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    fake_library.queue("core_shutdown", CYNAPSA_STATUS_V1_WAIT_TIMEOUT)
    core = NativeCore.create(core_config, library=fake_library)
    core.start()

    with pytest.raises(NativeError) as raised:
        core.shutdown(5)

    assert raised.value.status == CYNAPSA_STATUS_V1_WAIT_TIMEOUT
    assert raised.value.code == "wait_timeout"
    assert core.lifecycle == "closing"
    assert fake_library.calls["core_destroy"] == 0
    with pytest.raises(NativeError) as destroy_error:
        core.destroy()
    assert destroy_error.value.code == "core_not_closed"

    core.shutdown(10)
    core.destroy()
    assert fake_library.calls["core_shutdown"] == 2
    assert fake_library.calls["core_destroy"] == 1


def test_would_block_does_not_destroy(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    fake_library.queue(
        "core_shutdown",
        CYNAPSA_STATUS_V1_ERROR,
        {
            "code": "shutdown_in_progress",
            "message": "Shutdown is still in progress",
            "stage": "shutdown",
        },
    )
    core = NativeCore.create(core_config, library=fake_library)

    with pytest.raises(NativeError) as raised:
        core.close(1)

    assert raised.value.code == "shutdown_in_progress"
    assert fake_library.calls["core_destroy"] == 0
    assert core.handle == 42


def test_double_lifecycle_calls_are_safe(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = NativeCore.create(core_config, library=fake_library)
    core.start()
    with pytest.raises(NativeError) as raised:
        core.start()
    assert raised.value.code == "invalid_lifecycle"
    assert fake_library.calls["core_start"] == 1

    core.shutdown()
    core.shutdown()
    core.destroy()
    core.destroy()
    core.close()
    assert fake_library.calls["core_shutdown"] == 1
    assert fake_library.calls["core_destroy"] == 1


def test_destroy_requires_confirmed_closed_state(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = NativeCore.create(core_config, library=fake_library)
    with pytest.raises(NativeError) as raised:
        core.destroy()
    assert raised.value.code == "core_not_closed"
    assert fake_library.calls["core_destroy"] == 0


def test_partial_create_cleanup_shuts_down_before_destroy(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    original_create = fake_library.cynapsa_v1_core_create.implementation

    def partial_create(*args: object) -> int:
        status = original_create(*args)
        args[2]._obj.value = 91  # type: ignore[attr-defined]
        return status

    fake_library.cynapsa_v1_core_create.implementation = partial_create
    fake_library.queue(
        "core_create",
        CYNAPSA_STATUS_V1_ERROR,
        {"code": "core_error", "message": "Creation failed"},
    )

    with pytest.raises(NativeError) as raised:
        NativeCore.create(core_config, library=fake_library)

    assert raised.value.code == "core_error"
    assert fake_library.calls["core_shutdown"] == 1
    assert fake_library.calls["core_destroy"] == 1


def test_partial_create_timeout_never_destroys(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    original_create = fake_library.cynapsa_v1_core_create.implementation

    def partial_create(*args: object) -> int:
        status = original_create(*args)
        args[2]._obj.value = 91  # type: ignore[attr-defined]
        return status

    fake_library.cynapsa_v1_core_create.implementation = partial_create
    fake_library.queue(
        "core_create",
        CYNAPSA_STATUS_V1_ERROR,
        {"code": "core_error", "message": "Creation failed"},
    )
    fake_library.queue("core_shutdown", CYNAPSA_STATUS_V1_WAIT_TIMEOUT)

    with pytest.raises(NativeError) as raised:
        NativeCore.create(core_config, library=fake_library)

    assert raised.value.details["partial_cleanup"]["code"] == "wait_timeout"
    assert fake_library.calls["core_destroy"] == 0


def _normalized_error(code: str = "core_error", message: str | None = None) -> bytes:
    return json.dumps(
        {
            "abi_version": 1,
            "error": {
                "code": code,
                "message": PUBLIC_ERROR_MESSAGES[code] if message is None else message,
                "retryable": False,
                "stage": "sdk",
                "local_or_remote": "local",
            },
        },
        separators=(",", ":"),
    ).encode()


def test_core_config_is_strict_bounded_and_deterministic(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    reversed_config = dict(reversed(tuple(core_config.items())))
    core = NativeCore.create(reversed_config, library=fake_library)
    assert fake_library.create_input == (
        b'{"abi_version":1,"command_timeout_ms":0,"rpc_timeout_ms":0,'
        b'"queue_limit":4,"payload_limit":1048576}'
    )
    core.close()

    cases: list[Mapping[str, Any] | object] = [
        {**core_config, "unknown": 1},
        {key: value for key, value in core_config.items() if key != "queue_limit"},
        {**core_config, "abi_version": True},
        {**core_config, "command_timeout_ms": MAX_TIMEOUT_MS + 1},
        {**core_config, "queue_limit": 0},
        {**core_config, "queue_limit": MAX_QUEUE_LIMIT + 1},
        {**core_config, "payload_limit": 0},
        {**core_config, "payload_limit": MAX_PAYLOAD_BYTES + 1},
        [],
    ]
    calls = fake_library.calls["core_create"]
    for value in cases:
        with pytest.raises(NativeError, match="configuration|must be"):
            NativeCore.create(value, library=fake_library)  # type: ignore[arg-type]
    assert fake_library.calls["core_create"] == calls


def test_native_error_schema_is_strict_and_exception_is_read_only(
    fake_library: FakeLibrary,
) -> None:
    malformed = fake_library.add_buffer(
        b'{"abi_version":1,"error":{"code":"core_error","message":"x"}}'
    )
    error = decode_native_error(
        fake_library,
        CYNAPSA_STATUS_V1_ERROR,
        cynapsa_buffer_desc_v1(malformed, len(fake_library.buffers[malformed])),
    )
    assert error.code == "native_error_decode_failed"
    assert fake_library.free_counts[malformed] == 1
    with pytest.raises(AttributeError):
        error.code = "changed"  # type: ignore[misc]
    with pytest.raises(TypeError):
        error.details["changed"] = True  # type: ignore[index]


def test_native_error_diagnostic_id_has_canonical_32_byte_shape(
    fake_library: FakeLibrary,
) -> None:
    valid_id = "A" * 43
    valid_document = json.dumps(
        {
            "abi_version": 1,
            "error": {
                "code": "core_error",
                "message": PUBLIC_ERROR_MESSAGES["core_error"],
                "retryable": False,
                "stage": "sdk",
                "local_or_remote": "local",
                "diagnostic_id": valid_id,
            },
        },
        separators=(",", ":"),
    ).encode()
    valid_handle = fake_library.add_buffer(valid_document)
    error = decode_native_error(
        fake_library,
        CYNAPSA_STATUS_V1_ERROR,
        cynapsa_buffer_desc_v1(valid_handle, len(valid_document)),
    )
    assert error.details["diagnostic_id"] == valid_id

    invalid_document = valid_document.replace(valid_id.encode(), b"A" * 32)
    invalid_handle = fake_library.add_buffer(invalid_document)
    invalid = decode_native_error(
        fake_library,
        CYNAPSA_STATUS_V1_ERROR,
        cynapsa_buffer_desc_v1(invalid_handle, len(invalid_document)),
    )
    assert invalid.code == "native_error_decode_failed"


def test_native_error_details_are_deeply_immutable() -> None:
    source = {"nested": {"values": [1, {"key": "value"}]}}
    error = NativeError(1, "test", "test error", source)
    source["nested"]["values"].append(2)  # type: ignore[index,union-attr]

    assert error.details["nested"]["values"] == (1, {"key": "value"})
    with pytest.raises(TypeError):
        error.details["nested"]["values"][1]["key"] = "changed"


def test_malformed_descriptor_shapes_are_rejected_and_owned_handle_is_freed(
    fake_library: FakeLibrary,
) -> None:
    with pytest.raises(NativeError) as missing_handle:
        copy_and_free_buffer(fake_library, cynapsa_buffer_desc_v1(0, 1))
    assert missing_handle.value.code == "invalid_buffer_descriptor"

    handle = fake_library.add_buffer(b"")
    with pytest.raises(NativeError) as zero_length:
        copy_and_free_buffer(fake_library, cynapsa_buffer_desc_v1(handle, 0))
    assert zero_length.value.code == "invalid_buffer_descriptor"
    assert fake_library.free_counts[handle] == 1


def test_buffer_read_failure_consumes_read_error_and_source_once(
    fake_library: FakeLibrary,
) -> None:
    source = fake_library.add_buffer(b"value")
    original_read = fake_library.cynapsa_v1_buffer_read.implementation
    error_handles: list[int] = []

    def fail_source(*args: Any) -> int:
        if int(args[0]) != source:
            return original_read(*args)
        fake_library._record("buffer_read", args)
        args[4]._obj.value = 0
        handle = fake_library._set_descriptor(args[5], _normalized_error())
        assert handle is not None
        error_handles.append(handle)
        return CYNAPSA_STATUS_V1_ERROR

    fake_library.cynapsa_v1_buffer_read.implementation = fail_source
    with pytest.raises(NativeError) as raised:
        copy_and_free_buffer(fake_library, cynapsa_buffer_desc_v1(source, 5))
    assert raised.value.code == "buffer_read_failed"
    assert fake_library.free_counts[source] == 1
    assert fake_library.free_counts[error_handles[0]] == 1


def test_buffer_read_exception_consumes_populated_error_and_source_once(
    fake_library: FakeLibrary,
) -> None:
    source = fake_library.add_buffer(b"value")
    original_read = fake_library.cynapsa_v1_buffer_read.implementation
    error_handles: list[int] = []

    def raises_after_error(*args: Any) -> int:
        if int(args[0]) != source:
            return original_read(*args)
        args[4]._obj.value = 0
        handle = fake_library._set_descriptor(args[5], _normalized_error())
        assert handle is not None
        error_handles.append(handle)
        raise RuntimeError("credential-do-not-disclose")

    fake_library.cynapsa_v1_buffer_read.implementation = raises_after_error
    with pytest.raises(NativeError) as raised:
        copy_and_free_buffer(fake_library, cynapsa_buffer_desc_v1(source, 5))
    assert raised.value.code == "buffer_read_failed"
    assert "credential-do-not-disclose" not in repr(raised.value)
    assert fake_library.free_counts[source] == 1
    assert fake_library.free_counts[error_handles[0]] == 1


def test_buffer_free_failure_consumes_cleanup_error_chain_once(
    fake_library: FakeLibrary,
) -> None:
    source = fake_library.add_buffer(b"value")
    original_free = fake_library.cynapsa_v1_buffer_free.implementation
    first_error: list[int] = []
    second_error: list[int] = []

    def chained_free(handle: int, out_error: Any) -> int:
        numeric = int(handle)
        if numeric == source:
            fake_library._record("buffer_free", (handle, out_error))
            fake_library.free_counts[numeric] += 1
            nested = fake_library._set_descriptor(out_error, _normalized_error())
            assert nested is not None
            first_error.append(nested)
            return CYNAPSA_STATUS_V1_ERROR
        if first_error and numeric == first_error[0]:
            fake_library._record("buffer_free", (handle, out_error))
            fake_library.free_counts[numeric] += 1
            fake_library.buffers.pop(numeric, None)
            nested = fake_library._set_descriptor(out_error, _normalized_error())
            assert nested is not None
            second_error.append(nested)
            return CYNAPSA_STATUS_V1_ERROR
        return original_free(handle, out_error)

    fake_library.cynapsa_v1_buffer_free.implementation = chained_free
    with pytest.raises(NativeError) as raised:
        copy_and_free_buffer(fake_library, cynapsa_buffer_desc_v1(source, 5))
    assert raised.value.code == "buffer_free_failed"
    assert fake_library.free_counts[source] == 1
    assert fake_library.free_counts[first_error[0]] == 1
    assert fake_library.free_counts[second_error[0]] == 1


@pytest.mark.parametrize("native_status", [CYNAPSA_STATUS_V1_OK, CYNAPSA_STATUS_V1_ERROR])
def test_status_result_and_error_are_both_consumed_once(
    fake_library: FakeLibrary,
    core_config: dict[str, int],
    native_status: int,
) -> None:
    result_handles: list[int] = []
    error_handles: list[int] = []

    def simultaneous(core: int, out_result: Any, out_error: Any) -> int:
        fake_library._record("core_status", (core, out_result, out_error))
        result = fake_library._set_descriptor(out_result, fake_library.status_document)
        error = fake_library._set_descriptor(out_error, _normalized_error())
        assert result is not None and error is not None
        result_handles.append(result)
        error_handles.append(error)
        return native_status

    fake_library.cynapsa_v1_core_status.implementation = simultaneous
    core = NativeCore.create(core_config, library=fake_library)
    with pytest.raises(NativeError) as raised:
        core.status()
    assert raised.value.code == (
        "invalid_native_output" if native_status == CYNAPSA_STATUS_V1_OK else "core_error"
    )
    assert fake_library.free_counts[result_handles[0]] == 1
    assert fake_library.free_counts[error_handles[0]] == 1
    core.close()


def test_foreign_call_exception_consumes_populated_outputs_without_leaking_cause(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    handles: list[int] = []

    def raises_after_outputs(core: int, out_result: Any, out_error: Any) -> int:
        result = fake_library._set_descriptor(
            out_result, fake_library.status_document
        )
        error = fake_library._set_descriptor(out_error, _normalized_error())
        assert result is not None and error is not None
        handles.extend((result, error))
        raise RuntimeError("credential-do-not-disclose")

    fake_library.cynapsa_v1_core_status.implementation = raises_after_outputs
    core = NativeCore.create(core_config, library=fake_library)
    with pytest.raises(NativeError) as raised:
        core.status()
    assert raised.value.code == "abi_call_failed"
    assert "credential-do-not-disclose" not in repr(raised.value)
    assert all(fake_library.free_counts[handle] == 1 for handle in handles)
    core.close()


def test_start_failure_can_retry_and_success_error_records_started(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    fake_library.queue(
        "core_start",
        CYNAPSA_STATUS_V1_ERROR,
        {"code": "core_error", "message": "start failed", "stage": "command"},
    )
    core = NativeCore.create(core_config, library=fake_library)
    with pytest.raises(NativeError):
        core.start()
    assert core.lifecycle == "created"
    core.start()
    assert core.lifecycle == "started"
    core.close()

    second = NativeCore.create(core_config, library=fake_library)
    original_start = fake_library.cynapsa_v1_core_start.implementation

    def successful_with_error(*args: Any) -> int:
        status = original_start(*args)
        fake_library._set_descriptor(args[-1], _normalized_error())
        return status

    fake_library.cynapsa_v1_core_start.implementation = successful_with_error
    with pytest.raises(NativeError) as raised:
        second.start()
    assert raised.value.code == "invalid_native_output"
    assert second.lifecycle == "started"
    second.close()


def test_closed_status_after_shutdown_timeout_allows_destroy(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    fake_library.queue(
        "core_shutdown",
        CYNAPSA_STATUS_V1_ERROR,
        {
            "code": "shutdown_timeout",
            "message": "shutdown timed out",
            "stage": "shutdown",
        },
    )
    core = NativeCore.create(core_config, library=fake_library)
    with pytest.raises(NativeError) as raised:
        core.shutdown(1)
    assert raised.value.code == "shutdown_timeout"
    fake_library.status_document = fake_library.status_document.replace(
        b'"lifecycle":"ready"', b'"lifecycle":"closed"'
    )
    assert core.status()["status"]["lifecycle"] == "closed"
    core.destroy()
    assert core.lifecycle == "destroyed"


def test_successful_shutdown_and_destroy_keep_confirmed_state_on_error_output(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = NativeCore.create(core_config, library=fake_library)
    original_shutdown = fake_library.cynapsa_v1_core_shutdown.implementation
    original_destroy = fake_library.cynapsa_v1_core_destroy.implementation

    def malformed_shutdown(*args: Any) -> int:
        status = original_shutdown(*args)
        fake_library._set_descriptor(args[-1], _normalized_error())
        return status

    def malformed_destroy(*args: Any) -> int:
        status = original_destroy(*args)
        fake_library._set_descriptor(args[-1], _normalized_error())
        return status

    fake_library.cynapsa_v1_core_shutdown.implementation = malformed_shutdown
    with pytest.raises(NativeError) as shutdown_error:
        core.shutdown()
    assert shutdown_error.value.code == "invalid_native_output"
    assert core.lifecycle == "closed"

    fake_library.cynapsa_v1_core_destroy.implementation = malformed_destroy
    with pytest.raises(NativeError) as destroy_error:
        core.destroy()
    assert destroy_error.value.code == "invalid_native_output"
    assert core.lifecycle == "destroyed"
    assert core.handle == 0
    assert all(count == 1 for count in fake_library.free_counts.values())


@pytest.mark.parametrize("timeout", [True, -1, 2**63])
def test_shutdown_rejects_invalid_signed_timeout_before_native_call(
    fake_library: FakeLibrary,
    core_config: dict[str, int],
    timeout: int,
) -> None:
    core = NativeCore.create(core_config, library=fake_library)
    with pytest.raises(TypeError):
        core.shutdown(timeout)
    assert fake_library.calls["core_shutdown"] == 0
    core.close()


def test_concurrent_close_is_idempotent(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = NativeCore.create(core_config, library=fake_library)
    errors: list[BaseException] = []

    def close() -> None:
        try:
            core.close()
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=close) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    assert fake_library.calls["core_shutdown"] == 1
    assert fake_library.calls["core_destroy"] == 1
