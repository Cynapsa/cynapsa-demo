"""Private native-callback handoff and dispatcher machinery.

The C callback in this module is intentionally a very small Stage-B shim.  It
does not decode payloads or call SDK/user operations; all semantic work happens
on the dispatcher thread.
"""

from __future__ import annotations

import ctypes
import queue
import secrets
import threading
import time
from dataclasses import dataclass
from typing import Any

from cynapsa.exceptions import NativeError

from .abi import (
    CYNAPSA_CALLBACK_V1_COMPLETION,
    CYNAPSA_CALLBACK_V1_DIAGNOSTIC,
    CYNAPSA_CALLBACK_V1_EVENT,
    CYNAPSA_STATUS_V1_ERROR,
    CYNAPSA_STATUS_V1_OK,
    cynapsa_buffer_desc_v1,
    cynapsa_callback_v1,
)
from .command import decode_completion

MAX_CALLBACK_BUFFER_BYTES = 64 * 1024 * 1024

_CALLBACK_CONTEXT = threading.local()


def _callback_error(code: str, message: str, **details: Any) -> NativeError:
    details.setdefault("retryable", False)
    details.setdefault("stage", "sdk")
    details.setdefault("local_or_remote", "local")
    details.setdefault("terminal", True)
    return NativeError(CYNAPSA_STATUS_V1_ERROR, code, message, details)


@dataclass(frozen=True, slots=True)
class _IngressItem:
    kind: int
    payload: bytes | None
    error: NativeError | None


@dataclass(frozen=True, slots=True)
class CallbackEventItem:
    """Private raw event handoff retained for the milestone-6 decoder."""

    kind: int
    payload: bytes


def _new_tokens() -> tuple[int, int, int]:
    values: set[int] = set()
    while len(values) != 3:
        token = secrets.randbits(64)
        if token:
            values.add(token)
    completion, event, diagnostic = values
    return completion, event, diagnostic


def _descriptor_empty(value: cynapsa_buffer_desc_v1) -> bool:
    return not value.buffer_handle and not value.byte_length


def _raw_retire(
    library: Any,
    initial_handles: list[int],
    retired: set[int],
) -> list[dict[str, int | str]]:
    """Free descriptors without decoding any native error JSON.

    A failed ``buffer_free`` may itself return an owned error descriptor.  The
    iterative worklist retires that descriptor too, while rejecting cycles or
    duplicate handles without ever issuing a second free for one handle.
    """

    failures: list[dict[str, int | str]] = []
    work = list(initial_handles)
    while work:
        handle = work.pop()
        if not handle:
            continue
        if handle in retired:
            failures.append({"operation": "buffer_free", "buffer_handle": handle,
                             "reason": "duplicate descriptor handle"})
            continue
        retired.add(handle)
        out_error = cynapsa_buffer_desc_v1()
        status: int | None = None
        try:
            status = int(
                library.cynapsa_v1_buffer_free(handle, ctypes.byref(out_error))
            )
        except BaseException:
            failures.append({"operation": "buffer_free", "buffer_handle": handle,
                             "reason": "foreign call raised"})
        else:
            if status != CYNAPSA_STATUS_V1_OK:
                failures.append({"operation": "buffer_free", "buffer_handle": handle,
                                 "native_status": status})
        nested_handle = int(out_error.buffer_handle)
        nested_length = int(out_error.byte_length)
        if status == CYNAPSA_STATUS_V1_OK and (nested_handle or nested_length):
            failures.append({"operation": "buffer_free", "buffer_handle": handle,
                             "reason": "error descriptor on successful free"})
        if nested_handle:
            work.append(nested_handle)
            if not nested_length:
                failures.append({"operation": "buffer_free_error",
                                 "buffer_handle": nested_handle,
                                 "reason": "zero byte length"})
        elif nested_length:
            failures.append({"operation": "buffer_free_error",
                             "byte_length": nested_length,
                             "reason": "missing buffer handle"})
    return failures


def _copy_and_retire_callback_buffer(
    library: Any, descriptor: cynapsa_buffer_desc_v1
) -> tuple[bytes | None, NativeError | None]:
    """Copy and free one callback descriptor without JSON decoding."""

    handle = int(descriptor.buffer_handle)
    length = int(descriptor.byte_length)
    retired: set[int] = set()
    failures: list[dict[str, int | str]] = []
    payload: bytes | None = None
    out_error: cynapsa_buffer_desc_v1 | None = None

    if not handle:
        return None, _callback_error(
            "invalid_callback_descriptor",
            "The native callback returned a buffer length without a handle"
            if length else "The native callback returned an empty buffer descriptor",
            byte_length=length,
        )

    try:
        if not length:
            failures.append({"operation": "descriptor_validation",
                             "buffer_handle": handle, "reason": "zero byte length"})
        elif length > MAX_CALLBACK_BUFFER_BYTES:
            failures.append({"operation": "descriptor_validation",
                             "buffer_handle": handle, "byte_length": length,
                             "reason": "callback buffer exceeds binding limit"})
        else:
            destination = (ctypes.c_uint8 * length)()
            copied = ctypes.c_uint64()
            out_error = cynapsa_buffer_desc_v1()
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
            if status != CYNAPSA_STATUS_V1_OK:
                failures.append({"operation": "buffer_read", "buffer_handle": handle,
                                 "native_status": status})
            elif int(copied.value) != length:
                failures.append({"operation": "buffer_read", "buffer_handle": handle,
                                 "expected": length, "actual": int(copied.value),
                                 "reason": "short copy"})
            elif _descriptor_empty(out_error):
                payload = bytes(destination)
            else:
                failures.append({"operation": "buffer_read", "buffer_handle": handle,
                                 "reason": "error descriptor on successful copy"})
    except BaseException:
        failures.append({"operation": "buffer_read", "buffer_handle": handle,
                         "reason": "callback copy raised"})
    finally:
        if out_error is not None:
            try:
                nested_handle = int(out_error.buffer_handle)
                nested_length = int(out_error.byte_length)
                if nested_handle:
                    failures.extend(_raw_retire(library, [nested_handle], retired))
                    if not nested_length:
                        failures.append({"operation": "buffer_read_error",
                                         "buffer_handle": nested_handle,
                                         "reason": "zero byte length"})
                elif nested_length:
                    failures.append({"operation": "buffer_read_error",
                                     "byte_length": nested_length,
                                     "reason": "missing buffer handle"})
            except BaseException:
                failures.append({"operation": "buffer_read_error",
                                 "reason": "nested descriptor retirement raised"})
        failures.extend(_raw_retire(library, [handle], retired))
    if failures:
        return None, _callback_error(
            "callback_buffer_copy_failed",
            "The native callback buffer could not be copied and retired safely",
            failures=tuple(failures),
        )
    return payload, None


class CallbackDispatcher:
    """Own one CFUNCTYPE callback, bounded queues, and one decoder worker."""

    def __init__(self, library: Any, pending: Any, capacity: int) -> None:
        self.library = library
        self.pending = pending
        self.capacity = capacity
        self.completion_token, self.event_token, self.diagnostic_token = _new_tokens()
        self._tokens = {
            CYNAPSA_CALLBACK_V1_COMPLETION: self.completion_token,
            CYNAPSA_CALLBACK_V1_EVENT: self.event_token,
            CYNAPSA_CALLBACK_V1_DIAGNOSTIC: self.diagnostic_token,
        }
        # The native core owns independent bounded completion and event queues.
        # Preserve those ownership domains here as well: merging both streams
        # into one capacity-sized handoff lets a completion consume the only
        # slot needed by the next event when capacity is one. Diagnostics share
        # the native event queue, so they intentionally share its handoff.
        self._completion_ingress: queue.Queue[_IngressItem] = queue.Queue(
            maxsize=capacity
        )
        self._event_ingress: queue.Queue[_IngressItem] = queue.Queue(
            maxsize=capacity
        )
        self._ingress_ready = threading.Event()
        self._events: queue.Queue[CallbackEventItem] = queue.Queue(maxsize=capacity)
        self._diagnostics: queue.Queue[CallbackEventItem] = queue.Queue(maxsize=capacity)
        self._fatal_handoff: queue.Queue[NativeError] = queue.Queue(maxsize=1)
        self._stop = threading.Event()
        self._worker_done = threading.Event()
        self._active = True
        self._fatal: NativeError | None = None
        self._late_callbacks = 0
        self._saturated_callbacks = 0
        self.callback: Any | None = cynapsa_callback_v1(
            self._native_callback
        )
        self._worker = threading.Thread(
            target=self._dispatch,
            name="cynapsa-callback-dispatcher",
            daemon=True,
        )
        try:
            self._worker.start()
        except BaseException:
            # Thread.start() can fail after a custom threading implementation
            # has already launched the worker.  Retire either partial shape so
            # construction never leaks a live dispatcher or callback cycle.
            self._active = False
            self._stop.set()
            try:
                if self._worker is not threading.current_thread():
                    self._worker.join()
            except BaseException:
                pass
            self.callback = None
            raise

    @property
    def fatal_error(self) -> NativeError | None:
        return self._fatal

    @property
    def late_callbacks(self) -> int:
        return self._late_callbacks

    @property
    def saturated_callbacks(self) -> int:
        return self._saturated_callbacks

    def in_callback_thread(self) -> bool:
        return getattr(_CALLBACK_CONTEXT, "dispatcher", None) is self

    def in_worker_thread(self) -> bool:
        return threading.current_thread() is self._worker

    def _note_fatal(self, error: NativeError) -> None:
        try:
            self._fatal_handoff.put_nowait(error)
        except queue.Full:
            pass

    def _validate_route(self, token: int, kind: int) -> NativeError | None:
        expected = self._tokens.get(kind)
        if expected is None:
            return _callback_error(
                "invalid_callback_kind",
                "The native callback used an unknown delivery kind",
                callback_kind=kind,
                callback_token=token,
            )
        if token != expected:
            return _callback_error(
                "invalid_callback_token",
                "The native callback token did not match its delivery stream",
                callback_kind=kind,
                callback_token=token,
            )
        return None

    def _native_callback(
        self, token: int, kind: int, descriptor: cynapsa_buffer_desc_v1
    ) -> None:
        # No exception may cross ctypes' C callback trampoline.
        try:
            _CALLBACK_CONTEXT.dispatcher = self
            try:
                route_error = self._validate_route(int(token), int(kind))
                payload, buffer_error = _copy_and_retire_callback_buffer(
                    self.library, descriptor
                )
                error = route_error or buffer_error
                if not self._active:
                    self._late_callbacks += 1
                    error = error or _callback_error(
                        "late_callback_delivery",
                        "The native core delivered a callback after clear completed",
                        callback_kind=int(kind),
                    )
                item = _IngressItem(int(kind), payload, error)
                ingress = (
                    self._completion_ingress
                    if int(kind) == CYNAPSA_CALLBACK_V1_COMPLETION
                    else self._event_ingress
                )
                try:
                    ingress.put_nowait(item)
                    self._ingress_ready.set()
                except queue.Full:
                    self._saturated_callbacks += 1
                    self._note_fatal(
                        _callback_error(
                            "callback_queue_full",
                            "The bounded native callback handoff queue is full",
                            capacity=self.capacity,
                        )
                    )
            except BaseException:
                try:
                    self._note_fatal(
                        _callback_error(
                            "callback_boundary_failure",
                            "The native callback boundary failed unexpectedly",
                        )
                    )
                except BaseException:
                    pass
            finally:
                try:
                    del _CALLBACK_CONTEXT.dispatcher
                except BaseException:
                    pass
        except BaseException:
            # This is the outermost Stage-B containment fence.  In particular,
            # TLS and fatal-handoff failures during interpreter teardown must
            # not escape into foreign code.
            pass

    def _adopt_fatal(self, error: NativeError) -> None:
        if self._fatal is not None:
            return
        self._fatal = error
        self.pending.fail_all(
            lambda command_id: _callback_error(
                "callback_dispatch_failed",
                "The callback dispatcher failed before the command completed",
                command_id=command_id,
                dispatcher_error=error.code,
            ),
            terminal=True,
        )

    def _check_fatal_handoff(self) -> None:
        if self._fatal is not None:
            return
        try:
            error = self._fatal_handoff.get_nowait()
        except queue.Empty:
            return
        self._adopt_fatal(error)

    def _dispatch_item(self, item: _IngressItem) -> None:
        if item.error is not None:
            self._adopt_fatal(item.error)
            return
        if self._fatal is not None:
            return
        assert item.payload is not None
        if item.kind == CYNAPSA_CALLBACK_V1_COMPLETION:
            self.pending.route(decode_completion(item.payload))
            return
        stream = (
            self._diagnostics
            if item.kind == CYNAPSA_CALLBACK_V1_DIAGNOSTIC
            else self._events
        )
        try:
            stream.put_nowait(CallbackEventItem(item.kind, item.payload))
        except queue.Full:
            self._adopt_fatal(
                _callback_error(
                    "callback_event_queue_full",
                    "The bounded raw callback event handoff queue is full",
                    capacity=self.capacity,
                    callback_kind=item.kind,
                )
            )

    def _dispatch(self) -> None:
        prefer_completion = True
        try:
            while True:
                self._check_fatal_handoff()
                selected: queue.Queue[_IngressItem] | None = None
                item: _IngressItem | None = None
                streams = (
                    (self._completion_ingress, self._event_ingress)
                    if prefer_completion
                    else (self._event_ingress, self._completion_ingress)
                )
                for candidate in streams:
                    try:
                        item = candidate.get_nowait()
                    except queue.Empty:
                        continue
                    selected = candidate
                    prefer_completion = candidate is self._event_ingress
                    break
                if item is None:
                    if self._stop.is_set():
                        break
                    self._ingress_ready.clear()
                    # Close the clear/wait race: a callback may have enqueued
                    # after the empty checks but before Event.clear().
                    if (
                        not self._completion_ingress.empty()
                        or not self._event_ingress.empty()
                    ):
                        self._ingress_ready.set()
                        continue
                    self._ingress_ready.wait(0.01)
                    continue
                try:
                    try:
                        self._dispatch_item(item)
                    except NativeError as exc:
                        self._adopt_fatal(exc)
                    except BaseException:
                        self._adopt_fatal(
                            _callback_error(
                                "callback_worker_failure",
                                "The callback dispatcher worker failed unexpectedly",
                            )
                        )
                finally:
                    assert selected is not None
                    selected.task_done()
                if (
                    self._stop.is_set()
                    and self._completion_ingress.empty()
                    and self._event_ingress.empty()
                ):
                    break
            self._check_fatal_handoff()
        finally:
            self._worker_done.set()

    def native_cleared_and_join(self) -> None:
        """Release callback state only after the native clear has quiesced."""

        if self.in_worker_thread():
            raise RuntimeError("callback dispatcher cannot join itself")
        self._active = False
        self._stop.set()
        self._ingress_ready.set()
        self._worker.join()
        self.callback = None

    def abandon_rejected_registration(self) -> None:
        if self.in_worker_thread():
            raise RuntimeError("callback dispatcher cannot join itself")
        self._active = False
        self._stop.set()
        self._ingress_ready.set()
        self._worker.join()
        self.callback = None

    def next_event_for_test(self, timeout: float | None = None) -> CallbackEventItem:
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            try:
                return self._events.get_nowait()
            except queue.Empty:
                try:
                    return self._diagnostics.get_nowait()
                except queue.Empty:
                    if deadline is not None and time.monotonic() >= deadline:
                        raise
                    wait = 0.005
                    if deadline is not None:
                        wait = min(wait, max(0.0, deadline - time.monotonic()))
                    time.sleep(wait)

    def next_event(self, timeout: float | None = None) -> CallbackEventItem:
        return self._events.get(timeout=timeout)

    def next_diagnostic(self, timeout: float | None = None) -> CallbackEventItem:
        return self._diagnostics.get(timeout=timeout)
