from __future__ import annotations

import gc
import json
import threading
import time
import weakref
from typing import Any

import pytest

from cynapsa.exceptions import NativeError
from cynapsa.native import _callback as callback_module
from cynapsa.native.abi import (
    CYNAPSA_CALLBACK_V1_COMPLETION,
    CYNAPSA_CALLBACK_V1_DIAGNOSTIC,
    CYNAPSA_CALLBACK_V1_EVENT,
    CYNAPSA_STATUS_V1_ERROR,
    CYNAPSA_STATUS_V1_OK,
    CYNAPSA_STATUS_V1_WAIT_TIMEOUT,
    cynapsa_buffer_desc_v1,
)
from cynapsa.native.core import NativeCore

from conftest import FakeLibrary


def _core(fake: FakeLibrary, config: dict[str, int], *, capacity: int = 4) -> NativeCore:
    core = NativeCore.create(config, library=fake)
    core.start()
    core.register_callbacks(capacity)
    return core


def _completion(command_id: str) -> bytes:
    return json.dumps(
        {
            "abi_version": 1,
            "command_id": command_id,
            "ok": True,
            "result_type": "empty",
            "result": {},
        },
        separators=(",", ":"),
    ).encode()


def _wait_until(predicate: Any, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            pytest.fail("condition did not become true before timeout")
        time.sleep(0.005)


def test_foreign_threads_route_completions_and_free_each_buffer_once(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config, capacity=8)
    futures = [
        core.submit("core.init", "session", command_id=f"foreign-{index}")
        for index in range(8)
    ]
    handles: list[int] = []
    handle_lock = threading.Lock()

    def emit(index: int) -> None:
        handle = fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_COMPLETION, _completion(f"foreign-{index}")
        )
        with handle_lock:
            handles.append(handle)

    threads = [threading.Thread(target=emit, args=(index,)) for index in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(2)
        assert not thread.is_alive()

    assert [future.result(2).command_id for future in futures] == [
        f"foreign-{index}" for index in range(8)
    ]
    assert all(fake_library.free_counts[handle] == 1 for handle in handles)
    core.close()


def test_callback_returns_promptly_while_dispatch_decode_is_blocked(
    fake_library: FakeLibrary,
    core_config: dict[str, int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _core(fake_library, core_config)
    future = core.submit("core.init", "session", command_id="slow-worker")
    entered = threading.Event()
    release = threading.Event()
    original_decode = callback_module.decode_completion

    def slow_decode(raw: bytes) -> Any:
        entered.set()
        assert release.wait(2)
        return original_decode(raw)

    monkeypatch.setattr(callback_module, "decode_completion", slow_decode)
    elapsed: list[float] = []

    def emit() -> None:
        started = time.monotonic()
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_COMPLETION, _completion("slow-worker")
        )
        elapsed.append(time.monotonic() - started)

    thread = threading.Thread(target=emit, name="foreign-native-callback")
    thread.start()
    thread.join(1)
    assert not thread.is_alive()
    assert elapsed and elapsed[0] < 0.2
    assert entered.wait(1)
    assert not future.done()
    release.set()
    assert future.result(2).command_id == "slow-worker"
    core.close()


def test_completion_decode_and_routing_never_run_on_callback_thread(
    fake_library: FakeLibrary,
    core_config: dict[str, int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _core(fake_library, core_config)
    future = core.submit("core.init", "session", command_id="thread-check")
    callback_thread_ids: list[int] = []
    decode_thread_ids: list[int] = []
    original_decode = callback_module.decode_completion

    def recording_decode(raw: bytes) -> Any:
        decode_thread_ids.append(threading.get_ident())
        return original_decode(raw)

    monkeypatch.setattr(callback_module, "decode_completion", recording_decode)

    def emit() -> None:
        callback_thread_ids.append(threading.get_ident())
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_COMPLETION, _completion("thread-check")
        )

    thread = threading.Thread(target=emit)
    thread.start()
    thread.join(1)
    assert future.result(2).command_id == "thread-check"
    assert callback_thread_ids
    assert decode_thread_ids
    assert decode_thread_ids[0] != callback_thread_ids[0]
    core.close()


def test_local_callback_queue_saturation_is_fatal_bounded_and_leak_free(
    fake_library: FakeLibrary,
    core_config: dict[str, int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _core(fake_library, core_config, capacity=1)
    futures = [
        core.submit("core.init", "session", command_id=f"saturated-{index}")
        for index in range(3)
    ]
    entered = threading.Event()
    release = threading.Event()
    original_decode = callback_module.decode_completion

    def blocked_decode(raw: bytes) -> Any:
        entered.set()
        assert release.wait(2)
        return original_decode(raw)

    monkeypatch.setattr(callback_module, "decode_completion", blocked_decode)
    handles = [
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_COMPLETION, _completion("saturated-0")
        )
    ]
    assert entered.wait(1)
    handles.append(
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_COMPLETION, _completion("saturated-1")
        )
    )
    handles.append(
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_COMPLETION, _completion("saturated-2")
        )
    )
    release.set()
    _wait_until(lambda: core.callback_dispatch_error is not None)
    assert core.callback_dispatch_error is not None
    assert core.callback_dispatch_error.code == "callback_queue_full"
    assert futures[0].result(2).command_id == "saturated-0"
    for future in futures[1:]:
        with pytest.raises(NativeError) as raised:
            future.result(2)
        assert raised.value.code == "callback_dispatch_failed"
    assert all(fake_library.free_counts[handle] == 1 for handle in handles)
    assert fake_library.calls["core_destroy"] == 0
    core.clear_callbacks()
    core.close()


def test_completion_pressure_does_not_consume_event_ingress_capacity(
    fake_library: FakeLibrary,
    core_config: dict[str, int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _core(fake_library, core_config, capacity=1)
    futures = [
        core.submit("core.init", "session", command_id=f"split-stream-{index}")
        for index in range(2)
    ]
    entered = threading.Event()
    release = threading.Event()
    original_decode = callback_module.decode_completion

    def blocked_decode(raw: bytes) -> Any:
        entered.set()
        assert release.wait(2)
        return original_decode(raw)

    monkeypatch.setattr(callback_module, "decode_completion", blocked_decode)
    fake_library.emit_callback(
        CYNAPSA_CALLBACK_V1_COMPLETION, _completion("split-stream-0")
    )
    assert entered.wait(1)
    fake_library.emit_callback(
        CYNAPSA_CALLBACK_V1_COMPLETION, _completion("split-stream-1")
    )
    event_handle = fake_library.emit_callback(
        CYNAPSA_CALLBACK_V1_EVENT, b"independent-event"
    )
    release.set()

    assert [future.result(2).command_id for future in futures] == [
        "split-stream-0",
        "split-stream-1",
    ]
    assert core._next_callback_event_for_test(2).payload == b"independent-event"
    assert fake_library.free_counts[event_handle] == 1
    assert core.callback_dispatch_error is None
    core.close()


@pytest.mark.parametrize(
    ("kind", "token", "expected"),
    [
        (CYNAPSA_CALLBACK_V1_EVENT, 1, "invalid_callback_token"),
        (99, 1, "invalid_callback_kind"),
    ],
)
def test_wrong_token_or_kind_is_fatal_and_descriptor_is_freed(
    fake_library: FakeLibrary,
    core_config: dict[str, int],
    kind: int,
    token: int,
    expected: str,
) -> None:
    core = _core(fake_library, core_config)
    future = core.submit("core.init", "session", command_id="pending-route-error")
    callback = fake_library.callback
    assert callback is not None
    descriptor = fake_library.callback_descriptor(b"{}")
    callback(token, kind, descriptor)

    _wait_until(lambda: core.callback_dispatch_error is not None)
    assert core.callback_dispatch_error is not None
    assert core.callback_dispatch_error.code == expected
    with pytest.raises(NativeError, match="callback dispatcher"):
        future.result(2)
    assert fake_library.free_counts[int(descriptor.buffer_handle)] == 1
    core.clear_callbacks()
    core.close()


def test_diagnostic_and_event_tokens_remain_separate_raw_streams(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    tokens = fake_library.callback_tokens
    assert len(set(tokens.values())) == 3
    event = b'{"abi_version":1,"event":{"event_name":"core.error"}}'
    diagnostic = b'{"abi_version":1,"event":{"event_name":"diagnostics.log"}}'
    fake_library.emit_callback(CYNAPSA_CALLBACK_V1_EVENT, event)
    fake_library.emit_callback(CYNAPSA_CALLBACK_V1_DIAGNOSTIC, diagnostic)

    first = core._next_callback_event_for_test(2)
    second = core._next_callback_event_for_test(2)
    assert (first.kind, first.payload) == (CYNAPSA_CALLBACK_V1_EVENT, event)
    assert (second.kind, second.payload) == (
        CYNAPSA_CALLBACK_V1_DIAGNOSTIC,
        diagnostic,
    )
    core.close()


def test_raw_event_handoff_is_bounded_and_saturation_is_fatal(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config, capacity=1)
    first = fake_library.emit_callback(CYNAPSA_CALLBACK_V1_EVENT, b"first")
    _wait_until(lambda: core._callbacks is not None and core._callbacks._events.qsize() == 1)
    second = fake_library.emit_callback(CYNAPSA_CALLBACK_V1_EVENT, b"second")
    _wait_until(lambda: core.callback_dispatch_error is not None)
    assert core.callback_dispatch_error is not None
    assert core.callback_dispatch_error.code == "callback_event_queue_full"
    assert core._callbacks is not None
    assert core._callbacks._events.qsize() == 1
    assert fake_library.free_counts[first] == 1
    assert fake_library.free_counts[second] == 1
    core.clear_callbacks()
    core.close()


def test_malformed_completion_fails_pending_without_destroying_open_core(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    future = core.submit("core.init", "session", command_id="malformed")
    handle = fake_library.emit_callback(CYNAPSA_CALLBACK_V1_COMPLETION, b"not-json")

    with pytest.raises(NativeError) as raised:
        future.result(2)
    assert raised.value.code == "callback_dispatch_failed"
    assert core.callback_dispatch_error is not None
    assert core.callback_dispatch_error.code == "native_completion_decode_failed"
    assert core.lifecycle == "started"
    assert fake_library.calls["core_destroy"] == 0
    assert fake_library.free_counts[handle] == 1
    submits = fake_library.calls["core_submit"]
    with pytest.raises(NativeError) as new_submit:
        core.submit("core.init", "session", command_id="after-fatal")
    assert new_submit.value.code == "callback_dispatch_failed"
    assert fake_library.calls["core_submit"] == submits
    core.clear_callbacks()
    core.close()


def test_malformed_descriptor_is_retired_once_and_never_crosses_boundary(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    callback = fake_library.callback
    assert callback is not None
    handle = fake_library.add_buffer(b"payload")
    callback(
        fake_library.callback_tokens[CYNAPSA_CALLBACK_V1_EVENT],
        CYNAPSA_CALLBACK_V1_EVENT,
        cynapsa_buffer_desc_v1(handle, 0),
    )
    _wait_until(lambda: core.callback_dispatch_error is not None)
    assert core.callback_dispatch_error is not None
    assert core.callback_dispatch_error.code == "callback_buffer_copy_failed"
    assert fake_library.free_counts[handle] == 1
    core.clear_callbacks()
    core.close()


def test_callback_read_failure_retires_nested_and_original_descriptors_once(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    nested = fake_library.add_buffer(b"nested read error")

    def failing_read(*args: Any) -> int:
        handle, _, _, _, out_copied, out_error = args
        fake_library._record("buffer_read", args)
        out_copied._obj.value = 0
        fake_library._reset_descriptor(out_error)
        out_error._obj.buffer_handle = nested
        out_error._obj.byte_length = len(fake_library.buffers[nested])
        assert int(handle) != nested
        return CYNAPSA_STATUS_V1_ERROR

    fake_library.cynapsa_v1_buffer_read.implementation = failing_read
    original = fake_library.emit_callback(CYNAPSA_CALLBACK_V1_EVENT, b"payload")
    _wait_until(lambda: core.callback_dispatch_error is not None)
    assert core.callback_dispatch_error is not None
    assert core.callback_dispatch_error.code == "callback_buffer_copy_failed"
    assert fake_library.free_counts[original] == 1
    assert fake_library.free_counts[nested] == 1
    core.clear_callbacks()
    core.close()


def test_callback_read_exception_retires_populated_error_descriptor_once(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    nested = fake_library.add_buffer(b"nested read exception")

    def raising_read(*args: Any) -> int:
        _, _, _, _, out_copied, out_error = args
        fake_library._record("buffer_read", args)
        out_copied._obj.value = 0
        fake_library._reset_descriptor(out_error)
        out_error._obj.buffer_handle = nested
        out_error._obj.byte_length = len(fake_library.buffers[nested])
        raise RuntimeError("foreign read return failed")

    fake_library.cynapsa_v1_buffer_read.implementation = raising_read
    original = fake_library.emit_callback(CYNAPSA_CALLBACK_V1_EVENT, b"payload")
    _wait_until(lambda: core.callback_dispatch_error is not None)
    assert core.callback_dispatch_error is not None
    assert core.callback_dispatch_error.code == "callback_buffer_copy_failed"
    assert fake_library.free_counts[original] == 1
    assert fake_library.free_counts[nested] == 1
    core.clear_callbacks()
    core.close()


def test_successful_callback_free_with_error_descriptor_is_fatal_and_retired(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    nested = fake_library.add_buffer(b"illegal success error")
    original_free = fake_library.cynapsa_v1_buffer_free.implementation
    original_handle = 0

    def malformed_free(handle: int, out_error: Any) -> int:
        if int(handle) != original_handle:
            return original_free(handle, out_error)
        fake_library._record("buffer_free", (handle, out_error))
        fake_library._reset_descriptor(out_error)
        fake_library.free_counts[int(handle)] += 1
        fake_library.buffers.pop(int(handle), None)
        out_error._obj.buffer_handle = nested
        out_error._obj.byte_length = len(fake_library.buffers[nested])
        return CYNAPSA_STATUS_V1_OK

    fake_library.cynapsa_v1_buffer_free.implementation = malformed_free
    descriptor = fake_library.callback_descriptor(b"payload")
    original_handle = int(descriptor.buffer_handle)
    callback = fake_library.callback
    assert callback is not None
    callback(
        fake_library.callback_tokens[CYNAPSA_CALLBACK_V1_EVENT],
        CYNAPSA_CALLBACK_V1_EVENT,
        descriptor,
    )
    _wait_until(lambda: core.callback_dispatch_error is not None)
    assert core.callback_dispatch_error is not None
    assert core.callback_dispatch_error.code == "callback_buffer_copy_failed"
    assert fake_library.free_counts[original_handle] == 1
    assert fake_library.free_counts[nested] == 1
    core.clear_callbacks()
    core.close()


def test_callback_copy_allocation_failure_still_retires_original_descriptor(
    fake_library: FakeLibrary,
    core_config: dict[str, int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _core(fake_library, core_config)
    callback = fake_library.callback
    assert callback is not None
    descriptor = fake_library.callback_descriptor(b"payload")

    class FailingArrayElement:
        def __mul__(self, length: int) -> Any:
            raise MemoryError(f"cannot allocate {length}")

    monkeypatch.setattr(callback_module.ctypes, "c_uint8", FailingArrayElement())
    callback(
        fake_library.callback_tokens[CYNAPSA_CALLBACK_V1_EVENT],
        CYNAPSA_CALLBACK_V1_EVENT,
        descriptor,
    )
    _wait_until(lambda: core.callback_dispatch_error is not None)
    assert core.callback_dispatch_error is not None
    assert core.callback_dispatch_error.code == "callback_buffer_copy_failed"
    assert fake_library.free_counts[int(descriptor.buffer_handle)] == 1
    core.clear_callbacks()
    core.close()


def test_callback_and_polling_ownership_is_mutually_exclusive(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = NativeCore.create(core_config, library=fake_library)
    core.start()
    assert core.poll_completion(0) is None
    assert core.delivery_mode == "polling"
    core.register_callbacks()
    assert core.delivery_mode == "callbacks"
    with pytest.raises(NativeError) as raised:
        core.poll_completion(0)
    assert raised.value.code == "delivery_mode_conflict"
    core.clear_callbacks()
    assert core.poll_completion(0) is None
    core.close()


@pytest.mark.parametrize("capacity", [0, 65_537, True, 1.5])
def test_callback_capacity_is_validated_before_worker_or_native_registration(
    fake_library: FakeLibrary,
    core_config: dict[str, int],
    capacity: Any,
) -> None:
    core = NativeCore.create(core_config, library=fake_library)
    core.start()
    with pytest.raises(NativeError) as raised:
        core.register_callbacks(capacity)
    assert raised.value.code == "invalid_input"
    assert fake_library.calls["callbacks_register"] == 0
    assert core._callbacks is None
    assert core.poll_completion(0) is None
    core.close()


def test_worker_start_failure_restores_polling_and_joins_partial_thread(
    fake_library: FakeLibrary,
    core_config: dict[str, int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = NativeCore.create(core_config, library=fake_library)
    core.start()
    original_start = threading.Thread.start
    failed = False

    def start_then_fail(thread: threading.Thread) -> None:
        nonlocal failed
        original_start(thread)
        if thread.name == "cynapsa-callback-dispatcher" and not failed:
            failed = True
            raise RuntimeError("thread start return failed")

    monkeypatch.setattr(threading.Thread, "start", start_then_fail)
    with pytest.raises(NativeError) as raised:
        core.register_callbacks()
    assert raised.value.code == "callback_worker_start_failed"
    assert core._callbacks is None
    assert fake_library.calls["callbacks_register"] == 0
    assert core.poll_completion(0) is None

    core.register_callbacks()
    assert core.delivery_mode == "callbacks"
    core.clear_callbacks()
    core.close()


def test_rejected_native_registration_joins_worker_and_restores_polling(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = NativeCore.create(core_config, library=fake_library)
    core.start()
    fake_library.queue(
        "callbacks_register",
        CYNAPSA_STATUS_V1_ERROR,
        {"code": "core_error"},
    )
    with pytest.raises(NativeError) as raised:
        core.register_callbacks()
    assert raised.value.code == "core_error"
    assert core._callbacks is None
    assert core.poll_completion(0) is None
    assert not [
        thread
        for thread in threading.enumerate()
        if thread.name == "cynapsa-callback-dispatcher"
    ]
    core.close()


def test_registration_rejects_an_active_poll_without_double_consumer(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = NativeCore.create(core_config, library=fake_library)
    core.start()
    entered = threading.Event()
    release = threading.Event()

    def blocking_poll(*args: Any) -> int:
        fake_library._reset_descriptor(args[-2])
        fake_library._reset_descriptor(args[-1])
        entered.set()
        assert release.wait(2)
        return CYNAPSA_STATUS_V1_WAIT_TIMEOUT

    fake_library.cynapsa_v1_core_next_completion.implementation = blocking_poll
    poller = threading.Thread(target=core.poll_completion, args=(1000,))
    poller.start()
    assert entered.wait(1)
    with pytest.raises(NativeError) as raised:
        core.register_callbacks()
    assert raised.value.code == "delivery_mode_conflict"
    assert fake_library.calls["callbacks_register"] == 0
    release.set()
    poller.join(2)
    core.close()


def test_completion_may_route_before_submit_returns_without_registry_race(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    routed = threading.Event()
    original_route = core._pending.route
    original_submit = fake_library.cynapsa_v1_core_submit.implementation

    def route_and_signal(completion: Any) -> Any:
        result = original_route(completion)
        routed.set()
        return result

    def submit_with_early_completion(*args: Any) -> int:
        status = original_submit(*args)
        command_id = json.loads(fake_library.submit_inputs[-1])["command_id"]
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_COMPLETION, _completion(command_id)
        )
        assert routed.wait(2)
        return status

    core._pending.route = route_and_signal
    fake_library.cynapsa_v1_core_submit.implementation = submit_with_early_completion
    future = core.submit("core.init", "session", command_id="early-completion")
    assert future.result(0).command_id == "early-completion"
    assert core.pending_command_count == 0
    core.close()


def test_duplicate_registration_rejected_and_duplicate_clear_is_idempotent(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    with pytest.raises(NativeError) as raised:
        core.register_callbacks()
    assert raised.value.code == "callbacks_already_registered"
    assert fake_library.calls["callbacks_register"] == 1
    core.clear_callbacks()
    core.clear_callbacks()
    assert fake_library.calls["callbacks_clear"] == 1
    core.close()


def test_worker_cannot_clear_and_join_itself(
    fake_library: FakeLibrary,
    core_config: dict[str, int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _core(fake_library, core_config)
    future = core.submit("core.init", "session", command_id="worker-self-clear")
    observed: list[str] = []
    original_decode = callback_module.decode_completion

    def reentrant_decode(raw: bytes) -> Any:
        try:
            core.clear_callbacks()
        except NativeError as exc:
            observed.append(exc.code)
        return original_decode(raw)

    monkeypatch.setattr(callback_module, "decode_completion", reentrant_decode)
    fake_library.emit_callback(
        CYNAPSA_CALLBACK_V1_COMPLETION, _completion("worker-self-clear")
    )
    assert future.result(2).command_id == "worker-self-clear"
    assert observed == ["callback_reentrant_operation"]
    assert fake_library.calls["callbacks_clear"] == 0
    core.clear_callbacks()
    core.close()


def test_uncertain_registration_keeps_cfunctype_until_rollback_clear_joins(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = NativeCore.create(core_config, library=fake_library)
    core.start()
    original_register = fake_library.cynapsa_v1_callbacks_register.implementation

    def installs_then_raises(*args: Any) -> int:
        assert original_register(*args) == CYNAPSA_STATUS_V1_OK
        raise RuntimeError("foreign registration return failure")

    fake_library.cynapsa_v1_callbacks_register.implementation = installs_then_raises
    fake_library.queue("callbacks_clear", CYNAPSA_STATUS_V1_WAIT_TIMEOUT)
    with pytest.raises(NativeError) as raised:
        core.register_callbacks()
    assert raised.value.code == "abi_call_failed"
    assert raised.value.details["callback_state_retained"] is True
    assert core._callbacks is not None
    assert core._callbacks.callback is fake_library.callback
    assert core.delivery_mode == "callbacks"

    core.clear_callbacks(100)
    assert core._callbacks is None
    core.close()


def test_successful_registration_with_malformed_error_retains_owned_callback(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = NativeCore.create(core_config, library=fake_library)
    core.start()
    original_register = fake_library.cynapsa_v1_callbacks_register.implementation
    error_document = fake_library._error_document({"code": "core_error"})
    error_handle = 0

    def success_with_error(*args: Any) -> int:
        nonlocal error_handle
        status = original_register(*args)
        error_handle = fake_library._set_descriptor(args[-1], error_document) or 0
        return status

    fake_library.cynapsa_v1_callbacks_register.implementation = success_with_error
    with pytest.raises(NativeError) as raised:
        core.register_callbacks()
    assert raised.value.code == "invalid_native_output"
    assert raised.value.details["callback_state_retained"] is True
    assert core._callbacks is not None
    assert core.delivery_mode == "callbacks"
    assert fake_library.free_counts[error_handle] == 1
    core.clear_callbacks()
    core.close()


def test_callback_reentrancy_rejects_before_concurrent_clear_mutex(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    callback_in_read = threading.Event()
    clear_entered = threading.Event()
    callback_done = threading.Event()
    clear_done = threading.Event()
    errors: list[str] = []
    original_read = fake_library.cynapsa_v1_buffer_read.implementation
    original_clear = fake_library.cynapsa_v1_callbacks_clear.implementation

    def reentrant_read(*args: Any) -> int:
        callback_in_read.set()
        if not clear_entered.wait(2):
            errors.append("clear did not enter")
        try:
            core.shutdown()
        except NativeError as exc:
            errors.append(exc.code)
        else:
            errors.append("shutdown was not rejected")
        return original_read(*args)

    def clear_waiting_for_callback(*args: Any) -> int:
        clear_entered.set()
        if not callback_done.wait(2):
            errors.append("callback did not return")
        return original_clear(*args)

    fake_library.cynapsa_v1_buffer_read.implementation = reentrant_read
    fake_library.cynapsa_v1_callbacks_clear.implementation = clear_waiting_for_callback

    def emit() -> None:
        try:
            fake_library.emit_callback(CYNAPSA_CALLBACK_V1_EVENT, b"event")
        finally:
            callback_done.set()

    def clear() -> None:
        core.clear_callbacks(1_000)
        clear_done.set()

    emitter = threading.Thread(target=emit)
    clearer = threading.Thread(target=clear)
    emitter.start()
    assert callback_in_read.wait(1)
    clearer.start()
    emitter.join(2)
    clearer.join(2)
    assert not emitter.is_alive()
    assert not clearer.is_alive()
    assert clear_done.is_set()
    assert errors == ["callback_reentrant_operation"]
    core.close()


def test_clear_timeout_retains_callback_state_for_later_join(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    callback = fake_library.callback
    fake_library.queue("callbacks_clear", CYNAPSA_STATUS_V1_WAIT_TIMEOUT)
    with pytest.raises(NativeError) as raised:
        core.clear_callbacks(1)
    assert raised.value.code == "wait_timeout"
    assert core.delivery_mode == "callbacks"
    assert fake_library.callback is callback
    with pytest.raises(NativeError) as poll_error:
        core.poll_completion(0)
    assert poll_error.value.code == "delivery_mode_conflict"
    core.clear_callbacks(100)
    assert fake_library.calls["callbacks_clear"] == 2
    core.close()


def test_clear_waits_for_inflight_callback_before_releasing_python_state(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    dispatcher = core._callbacks
    assert dispatcher is not None
    entered = threading.Event()
    release = threading.Event()
    callback_done = threading.Event()
    clear_done = threading.Event()
    original_read = fake_library.cynapsa_v1_buffer_read.implementation
    original_clear = fake_library.cynapsa_v1_callbacks_clear.implementation

    def blocking_read(*args: Any) -> int:
        entered.set()
        assert release.wait(2)
        return original_read(*args)

    def quiescing_clear(*args: Any) -> int:
        assert callback_done.wait(2)
        return original_clear(*args)

    fake_library.cynapsa_v1_buffer_read.implementation = blocking_read
    fake_library.cynapsa_v1_callbacks_clear.implementation = quiescing_clear

    def emit() -> None:
        try:
            fake_library.emit_callback(CYNAPSA_CALLBACK_V1_EVENT, b"inflight")
        finally:
            callback_done.set()

    emitter = threading.Thread(target=emit)
    clearer = threading.Thread(
        target=lambda: (core.clear_callbacks(1000), clear_done.set())
    )
    emitter.start()
    assert entered.wait(1)
    clearer.start()
    time.sleep(0.03)
    assert not clear_done.is_set()
    assert core._callbacks is dispatcher
    assert dispatcher.callback is not None
    release.set()
    emitter.join(2)
    clearer.join(2)
    assert clear_done.is_set()
    assert core._callbacks is None
    assert dispatcher.callback is None
    core.close()


def test_same_core_shutdown_is_rejected_from_callback_context(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    observed: list[str] = []
    original_read = fake_library.cynapsa_v1_buffer_read.implementation

    def reentrant_read(*args: Any) -> int:
        with pytest.raises(NativeError) as raised:
            core.shutdown()
        observed.append(raised.value.code)
        return original_read(*args)

    fake_library.cynapsa_v1_buffer_read.implementation = reentrant_read
    thread = threading.Thread(
        target=fake_library.emit_callback,
        args=(CYNAPSA_CALLBACK_V1_EVENT, b"event"),
    )
    thread.start()
    thread.join(2)
    assert not thread.is_alive()
    assert observed == ["callback_reentrant_operation"]
    assert core.lifecycle == "started"
    core.close()


def test_cfunctype_and_owned_state_live_until_clear_then_are_collectible(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    callback = fake_library.callback
    dispatcher = core._callbacks
    assert callback is not None and dispatcher is not None
    callback_ref = weakref.ref(callback)
    dispatcher_ref = weakref.ref(dispatcher)
    del callback, dispatcher
    gc.collect()
    assert callback_ref() is not None
    assert dispatcher_ref() is not None

    core.clear_callbacks()
    # The fake call recorder is an external owner unlike a real C library.
    fake_library.call_args["callbacks_register"].clear()
    gc.collect()
    assert callback_ref() is None
    assert dispatcher_ref() is None
    core.close()


def test_late_delivery_after_successful_clear_is_freed_and_recorded(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    callback = fake_library.callback
    dispatcher = core._callbacks
    token = fake_library.callback_tokens[CYNAPSA_CALLBACK_V1_EVENT]
    assert callback is not None and dispatcher is not None
    core.clear_callbacks()

    descriptor = fake_library.callback_descriptor(b"late")
    callback(token, CYNAPSA_CALLBACK_V1_EVENT, descriptor)
    assert fake_library.free_counts[int(descriptor.buffer_handle)] == 1
    assert dispatcher.late_callbacks == 1
    core.close()


def test_shutdown_uses_callbacks_until_closed_then_clears_and_destroys(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    future = core.submit("core.init", "session", command_id="shutdown-race")

    def shutdown_with_completion(core_handle: int, timeout: int, out_error: Any) -> int:
        fake_library._record("core_shutdown", (core_handle, timeout, out_error))
        fake_library._reset_descriptor(out_error)
        fake_library.emit_callback(
            CYNAPSA_CALLBACK_V1_COMPLETION, _completion("shutdown-race")
        )
        return CYNAPSA_STATUS_V1_OK

    fake_library.cynapsa_v1_core_shutdown.implementation = shutdown_with_completion
    core.shutdown(1000)
    assert future.result(0).command_id == "shutdown-race"
    assert fake_library.calls["core_next_completion"] == 0
    assert fake_library.calls["core_next_event"] == 0
    assert fake_library.calls["callbacks_clear"] == 1
    assert core.lifecycle == "closed"
    core.destroy()
    assert core.lifecycle == "destroyed"


def test_context_manager_exception_quiesces_callback_worker_and_destroys_core(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = NativeCore.create(core_config, library=fake_library)
    core.start()
    with pytest.raises(RuntimeError, match="application failure"):
        with core:
            core.register_callbacks()
            raise RuntimeError("application failure")
    assert core.lifecycle == "destroyed"
    assert core._callbacks is None
    assert fake_library.callback is None
