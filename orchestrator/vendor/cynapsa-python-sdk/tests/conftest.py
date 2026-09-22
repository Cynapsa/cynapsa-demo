from __future__ import annotations

import ctypes
import json
from collections import Counter, defaultdict, deque
from typing import Any

import pytest

from cynapsa.native.abi import (
    CYNAPSA_CALLBACK_V1_COMPLETION,
    CYNAPSA_CALLBACK_V1_DIAGNOSTIC,
    CYNAPSA_CALLBACK_V1_EVENT,
    CYNAPSA_STATUS_V1_ERROR,
    CYNAPSA_STATUS_V1_OK,
    CYNAPSA_STATUS_V1_WAIT_TIMEOUT,
    EXPORTS,
)
from cynapsa.native.command import PUBLIC_ERROR_MESSAGES

VALID_COMMAND_HANDLE = "cmdh_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"


class FakeFunction:
    def __init__(self, implementation: Any) -> None:
        self.implementation = implementation
        self.argtypes = None
        self.restype = None

    def __call__(self, *args: Any) -> Any:
        return self.implementation(*args)


class FakeLibrary:
    def __init__(self, *, abi_version: int = 1) -> None:
        self.abi_version = abi_version
        self.calls: Counter[str] = Counter()
        self.call_args: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
        self.responses: dict[str, deque[tuple[int, bytes | None]]] = defaultdict(deque)
        self.buffers: dict[int, bytes] = {}
        self.free_counts: Counter[int] = Counter()
        self.next_buffer = 100
        self.next_core = 42
        self.create_input: bytes | None = None
        self.status_document: bytes = json.dumps(
            {
                "abi_version": 1,
                "status": {
                    "lifecycle": "ready",
                    "connectivity": "available",
                    "personality": "unset",
                    "agent_id": "",
                    "mesh_id": "",
                    "mesh_endpoint": "",
                    "queued_message_count": 0,
                },
            },
            separators=(",", ":"),
        ).encode()
        self.submit_inputs: list[bytes] = []
        self.cancel_inputs: list[bytes] = []
        self.submit_responses: deque[tuple[int, bytes | None, bytes | None]] = deque()
        self.completion_responses: deque[tuple[int, bytes | None, bytes | None]] = deque()
        self.event_responses: deque[tuple[int, bytes | None, bytes | None]] = deque()
        self.callback: Any | None = None
        self.callback_tokens: dict[int, int] = {}
        self.callback_capacity: int | None = None

        for export in EXPORTS:
            setattr(self, export, FakeFunction(self._default(export)))
        self.cynapsa_v1_abi_version = FakeFunction(self._abi_version)
        self.cynapsa_v1_core_create = FakeFunction(self._core_create)
        self.cynapsa_v1_core_start = FakeFunction(self._simple("core_start"))
        self.cynapsa_v1_core_submit = FakeFunction(self._core_submit)
        self.cynapsa_v1_core_cancel = FakeFunction(self._core_cancel)
        self.cynapsa_v1_core_next_completion = FakeFunction(self._core_next_completion)
        self.cynapsa_v1_core_next_event = FakeFunction(self._core_next_event)
        self.cynapsa_v1_core_status = FakeFunction(self._core_status)
        self.cynapsa_v1_core_shutdown = FakeFunction(self._simple("core_shutdown"))
        self.cynapsa_v1_core_destroy = FakeFunction(self._simple("core_destroy"))
        self.cynapsa_v1_callbacks_register = FakeFunction(self._callbacks_register)
        self.cynapsa_v1_callbacks_clear = FakeFunction(self._callbacks_clear)
        self.cynapsa_v1_buffer_read = FakeFunction(self._buffer_read)
        self.cynapsa_v1_buffer_free = FakeFunction(self._buffer_free)

    def _record(self, name: str, args: tuple[Any, ...]) -> None:
        self.calls[name] += 1
        self.call_args[name].append(args)

    @staticmethod
    def _reset_descriptor(pointer: Any) -> None:
        pointer._obj.buffer_handle = 0
        pointer._obj.byte_length = 0

    def add_buffer(self, data: bytes) -> int:
        handle = self.next_buffer
        self.next_buffer += 1
        self.buffers[handle] = data
        return handle

    def _set_descriptor(self, pointer: Any, data: bytes | None) -> int | None:
        self._reset_descriptor(pointer)
        if data is None:
            return None
        handle = self.add_buffer(data)
        pointer._obj.buffer_handle = handle
        pointer._obj.byte_length = len(data)
        return handle

    def queue(
        self, operation: str, status: int, error: dict[str, Any] | bytes | None = None
    ) -> None:
        if isinstance(error, dict):
            code = error.get("code", "core_error")
            error = {
                "retryable": False,
                "stage": "sdk",
                "local_or_remote": "local",
                **error,
                "code": code,
                "message": PUBLIC_ERROR_MESSAGES[code],
            }
            error = json.dumps(
                {"abi_version": 1, "error": error}, separators=(",", ":")
            ).encode()
        self.responses[operation].append((status, error))

    @staticmethod
    def document(value: dict[str, Any]) -> bytes:
        return json.dumps(value, separators=(",", ":")).encode()

    def queue_submit(
        self,
        status: int,
        *,
        result: dict[str, Any] | bytes | None = None,
        error: dict[str, Any] | bytes | None = None,
    ) -> None:
        self.submit_responses.append(
            (status, self.document(result) if isinstance(result, dict) else result,
             self._error_document(error) if isinstance(error, dict) else error)
        )

    def queue_completion(
        self,
        status: int,
        *,
        result: dict[str, Any] | bytes | None = None,
        error: dict[str, Any] | bytes | None = None,
    ) -> None:
        self.completion_responses.append(
            (status, self.document(result) if isinstance(result, dict) else result,
             self._error_document(error) if isinstance(error, dict) else error)
        )

    def queue_event(
        self,
        status: int,
        *,
        result: dict[str, Any] | bytes | None = None,
        error: dict[str, Any] | bytes | None = None,
    ) -> None:
        self.event_responses.append(
            (status, self.document(result) if isinstance(result, dict) else result,
             self._error_document(error) if isinstance(error, dict) else error)
        )

    def _error_document(self, error: dict[str, Any]) -> bytes:
        code = error.get("code", "core_error")
        normalized = {
            "retryable": False,
            "stage": "sdk",
            "local_or_remote": "local",
            **error,
            "code": code,
            "message": PUBLIC_ERROR_MESSAGES[code],
        }
        return self.document({"abi_version": 1, "error": normalized})

    def _response(self, operation: str) -> tuple[int, bytes | None]:
        if self.responses[operation]:
            return self.responses[operation].popleft()
        return CYNAPSA_STATUS_V1_OK, None

    def _default(self, export: str) -> Any:
        def implementation(*args: Any) -> int:
            self._record(export, args)
            return CYNAPSA_STATUS_V1_OK

        return implementation

    def _abi_version(self, out_version: Any, out_error: Any) -> int:
        self._record("abi_version", (out_version, out_error))
        out_version._obj.value = 0
        self._reset_descriptor(out_error)
        status, error = self._response("abi_version")
        if status == CYNAPSA_STATUS_V1_OK:
            out_version._obj.value = self.abi_version
        else:
            self._set_descriptor(out_error, error)
        return status

    def _core_create(
        self, input_pointer: Any, input_length: int, out_core: Any, out_error: Any
    ) -> int:
        self._record("core_create", (input_pointer, input_length, out_core, out_error))
        out_core._obj.value = 0
        self._reset_descriptor(out_error)
        self.create_input = ctypes.string_at(input_pointer, input_length)
        status, error = self._response("core_create")
        if status == CYNAPSA_STATUS_V1_OK:
            out_core._obj.value = self.next_core
        else:
            self._set_descriptor(out_error, error)
        return status

    def _simple(self, operation: str) -> Any:
        def implementation(*args: Any) -> int:
            self._record(operation, args)
            out_error = args[-1]
            self._reset_descriptor(out_error)
            status, error = self._response(operation)
            if status != CYNAPSA_STATUS_V1_OK:
                self._set_descriptor(out_error, error)
            return status

        return implementation

    def _core_status(self, core: int, out_result: Any, out_error: Any) -> int:
        self._record("core_status", (core, out_result, out_error))
        self._reset_descriptor(out_result)
        self._reset_descriptor(out_error)
        status, error = self._response("core_status")
        if status == CYNAPSA_STATUS_V1_OK:
            self._set_descriptor(out_result, self.status_document)
        else:
            self._set_descriptor(out_error, error)
        return status

    def _core_submit(
        self,
        core: int,
        input_pointer: Any,
        input_length: int,
        out_result: Any,
        out_error: Any,
    ) -> int:
        self._record("core_submit", (core, input_pointer, input_length, out_result, out_error))
        self._reset_descriptor(out_result)
        self._reset_descriptor(out_error)
        data = ctypes.string_at(input_pointer, input_length)
        self.submit_inputs.append(data)
        command = json.loads(data)
        if self.submit_responses:
            status, result, error = self.submit_responses.popleft()
        else:
            status = CYNAPSA_STATUS_V1_OK
            result = self.document(
                {
                    "abi_version": 1,
                    "command_id": command["command_id"],
                    "command_handle": VALID_COMMAND_HANDLE,
                    "accepted": True,
                }
            )
            error = None
        self._set_descriptor(out_result, result)
        self._set_descriptor(out_error, error)
        return status

    def _core_cancel(
        self, core: int, input_pointer: Any, input_length: int, out_error: Any
    ) -> int:
        self._record("core_cancel", (core, input_pointer, input_length, out_error))
        self._reset_descriptor(out_error)
        self.cancel_inputs.append(ctypes.string_at(input_pointer, input_length))
        status, error = self._response("core_cancel")
        if status != CYNAPSA_STATUS_V1_OK:
            self._set_descriptor(out_error, error)
        return status

    def _core_next_completion(
        self, core: int, timeout_ms: int, out_result: Any, out_error: Any
    ) -> int:
        self._record("core_next_completion", (core, timeout_ms, out_result, out_error))
        self._reset_descriptor(out_result)
        self._reset_descriptor(out_error)
        if self.completion_responses:
            status, result, error = self.completion_responses.popleft()
        else:
            status, result, error = CYNAPSA_STATUS_V1_WAIT_TIMEOUT, None, None
        self._set_descriptor(out_result, result)
        self._set_descriptor(out_error, error)
        return status

    def _core_next_event(
        self, core: int, timeout_ms: int, out_result: Any, out_error: Any
    ) -> int:
        self._record("core_next_event", (core, timeout_ms, out_result, out_error))
        self._reset_descriptor(out_result)
        self._reset_descriptor(out_error)
        if self.event_responses:
            status, result, error = self.event_responses.popleft()
        else:
            status, result, error = CYNAPSA_STATUS_V1_WAIT_TIMEOUT, None, None
        self._set_descriptor(out_result, result)
        self._set_descriptor(out_error, error)
        return status

    def _callbacks_register(
        self,
        core: int,
        callback: Any,
        completion_token: int,
        event_token: int,
        diagnostic_token: int,
        capacity: int,
        out_error: Any,
    ) -> int:
        self._record(
            "callbacks_register",
            (
                core,
                callback,
                completion_token,
                event_token,
                diagnostic_token,
                capacity,
                out_error,
            ),
        )
        self._reset_descriptor(out_error)
        status, error = self._response("callbacks_register")
        if status == CYNAPSA_STATUS_V1_OK:
            self.callback = callback
            self.callback_tokens = {
                CYNAPSA_CALLBACK_V1_COMPLETION: int(completion_token),
                CYNAPSA_CALLBACK_V1_EVENT: int(event_token),
                CYNAPSA_CALLBACK_V1_DIAGNOSTIC: int(diagnostic_token),
            }
            self.callback_capacity = int(capacity)
        else:
            self._set_descriptor(out_error, error)
        return status

    def _callbacks_clear(
        self, core: int, timeout_ms: int, out_error: Any
    ) -> int:
        self._record("callbacks_clear", (core, timeout_ms, out_error))
        self._reset_descriptor(out_error)
        status, error = self._response("callbacks_clear")
        if status == CYNAPSA_STATUS_V1_OK:
            self.callback = None
        else:
            self._set_descriptor(out_error, error)
        return status

    def callback_descriptor(self, data: bytes) -> Any:
        from cynapsa.native.abi import cynapsa_buffer_desc_v1

        handle = self.add_buffer(data)
        return cynapsa_buffer_desc_v1(handle, len(data))

    def emit_callback(
        self, kind: int, data: bytes, *, token: int | None = None
    ) -> int:
        callback = self.callback
        if callback is None:
            raise RuntimeError("callbacks are not registered")
        descriptor = self.callback_descriptor(data)
        callback(
            self.callback_tokens[kind] if token is None else token,
            kind,
            descriptor,
        )
        return int(descriptor.buffer_handle)

    def _buffer_read(
        self,
        handle: int,
        offset: int,
        destination: Any,
        destination_length: int,
        out_copied: Any,
        out_error: Any,
    ) -> int:
        self._record(
            "buffer_read",
            (handle, offset, destination, destination_length, out_copied, out_error),
        )
        out_copied._obj.value = 0
        self._reset_descriptor(out_error)
        data = self.buffers[int(handle)][int(offset) : int(offset) + int(destination_length)]
        if data:
            ctypes.memmove(destination, data, len(data))
        out_copied._obj.value = len(data)
        return CYNAPSA_STATUS_V1_OK

    def _buffer_free(self, handle: int, out_error: Any) -> int:
        self._record("buffer_free", (handle, out_error))
        self._reset_descriptor(out_error)
        numeric_handle = int(handle)
        self.free_counts[numeric_handle] += 1
        self.buffers.pop(numeric_handle, None)
        return CYNAPSA_STATUS_V1_OK


@pytest.fixture
def fake_library() -> FakeLibrary:
    return FakeLibrary()


@pytest.fixture
def core_config() -> dict[str, int]:
    return {
        "abi_version": 1,
        "command_timeout_ms": 0,
        "rpc_timeout_ms": 0,
        "queue_limit": 4,
        "payload_limit": 1048576,
    }
