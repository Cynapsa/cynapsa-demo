"""Private copy-only binding to the Cynapsa V1 shared-library ABI."""

from __future__ import annotations

import ctypes
import json
import os
from pathlib import Path
from threading import Event, Thread
from typing import Callable

from .errors import CynapsaError, error_from_document, sdk_error

_OK = 0
_ERROR = 1
_WAIT_TIMEOUT = 2
_MAX_BUFFER_BYTES = 64 << 20


class _BufferDescriptor(ctypes.Structure):
    _fields_ = [
        ("buffer_handle", ctypes.c_uint64),
        ("byte_length", ctypes.c_uint64),
    ]


class NativeCore:
    """Private owner of one native core handle."""

    def __init__(self, library_path: str | os.PathLike[str], config: bytes) -> None:
        path = _absolute_library_path(library_path)
        try:
            self._library = ctypes.CDLL(str(path))
        except OSError:
            raise sdk_error("The Cynapsa Core library could not be loaded") from None
        self._bind()
        self.abi_version = self._read_abi_version()
        if self.abi_version != 1:
            raise sdk_error("The Cynapsa Core library uses an unsupported ABI version", code="unsupported_version")
        output = ctypes.c_uint64()
        error = _BufferDescriptor()
        data, length = _input(config)
        status = self._core_create(data, length, ctypes.byref(output), ctypes.byref(error))
        self._check(status, error)
        if output.value == 0:
            raise sdk_error("The Cynapsa Core library returned an invalid core handle")
        self._handle = output.value
        self._destroyed = False

    def start(self) -> None:
        self._mutation(self._core_start, self._handle)

    def submit(self, document: bytes) -> object:
        return self._json_input_result(self._core_submit, document)

    def cancel(self, document: bytes) -> None:
        self._json_input_mutation(self._core_cancel, document)

    def next_completion(self, timeout_ms: int) -> object | None:
        return self._wait_result(self._core_next_completion, timeout_ms)

    def next_event(self, timeout_ms: int) -> object | None:
        return self._wait_result(self._core_next_event, timeout_ms)

    def status(self) -> object:
        result = _BufferDescriptor()
        error = _BufferDescriptor()
        status = self._core_status(self._handle, ctypes.byref(result), ctypes.byref(error))
        self._check(status, error)
        return self._read_json(result)

    def shutdown(self, timeout_ms: int) -> None:
        finished = Event()
        failure: list[BaseException] = []

        def run_shutdown() -> None:
            try:
                self._mutation(self._core_shutdown, self._handle, timeout_ms)
            except BaseException as error:
                failure.append(error)
            finally:
                finished.set()

        worker = Thread(target=run_shutdown, name="cynapsa-core-shutdown", daemon=True)
        worker.start()
        while not finished.is_set():
            try:
                self._wait_result(self._core_next_event, 50)
            except CynapsaError:
                if not finished.is_set():
                    finished.wait(0.01)
        worker.join()
        if failure:
            raise failure[0]

    def destroy(self) -> None:
        if self._destroyed:
            return
        self._mutation(self._core_destroy, self._handle)
        self._destroyed = True
        self._handle = 0

    def _bind(self) -> None:
        descriptor_pointer = ctypes.POINTER(_BufferDescriptor)
        byte_pointer = ctypes.POINTER(ctypes.c_uint8)

        self._abi_version = self._function(
            "cynapsa_v1_abi_version",
            [ctypes.POINTER(ctypes.c_uint32), descriptor_pointer],
        )
        self._core_create = self._function(
            "cynapsa_v1_core_create",
            [byte_pointer, ctypes.c_uint64, ctypes.POINTER(ctypes.c_uint64), descriptor_pointer],
        )
        self._core_start = self._function("cynapsa_v1_core_start", [ctypes.c_uint64, descriptor_pointer])
        self._core_submit = self._function(
            "cynapsa_v1_core_submit",
            [ctypes.c_uint64, byte_pointer, ctypes.c_uint64, descriptor_pointer, descriptor_pointer],
        )
        self._core_cancel = self._function(
            "cynapsa_v1_core_cancel",
            [ctypes.c_uint64, byte_pointer, ctypes.c_uint64, descriptor_pointer],
        )
        wait_arguments = [ctypes.c_uint64, ctypes.c_int64, descriptor_pointer, descriptor_pointer]
        self._core_next_completion = self._function("cynapsa_v1_core_next_completion", wait_arguments)
        self._core_next_event = self._function("cynapsa_v1_core_next_event", wait_arguments)
        self._core_status = self._function(
            "cynapsa_v1_core_status",
            [ctypes.c_uint64, descriptor_pointer, descriptor_pointer],
        )
        self._core_shutdown = self._function(
            "cynapsa_v1_core_shutdown",
            [ctypes.c_uint64, ctypes.c_int64, descriptor_pointer],
        )
        self._core_destroy = self._function("cynapsa_v1_core_destroy", [ctypes.c_uint64, descriptor_pointer])
        self._buffer_read = self._function(
            "cynapsa_v1_buffer_read",
            [
                ctypes.c_uint64,
                ctypes.c_uint64,
                byte_pointer,
                ctypes.c_uint64,
                ctypes.POINTER(ctypes.c_uint64),
                descriptor_pointer,
            ],
        )
        self._buffer_free = self._function("cynapsa_v1_buffer_free", [ctypes.c_uint64, descriptor_pointer])

    def _function(self, name: str, arguments: list[object]) -> Callable[..., int]:
        try:
            function = getattr(self._library, name)
        except AttributeError:
            raise sdk_error("The Cynapsa Core library is missing a required V1 ABI function") from None
        function.argtypes = arguments
        function.restype = ctypes.c_int32
        return function

    def _read_abi_version(self) -> int:
        version = ctypes.c_uint32()
        error = _BufferDescriptor()
        status = self._abi_version(ctypes.byref(version), ctypes.byref(error))
        self._check(status, error)
        return version.value

    def _json_input_result(self, function: Callable[..., int], document: bytes) -> object:
        data, length = _input(document)
        result = _BufferDescriptor()
        error = _BufferDescriptor()
        status = function(self._handle, data, length, ctypes.byref(result), ctypes.byref(error))
        self._check(status, error)
        return self._read_json(result)

    def _json_input_mutation(self, function: Callable[..., int], document: bytes) -> None:
        data, length = _input(document)
        error = _BufferDescriptor()
        status = function(self._handle, data, length, ctypes.byref(error))
        self._check(status, error)

    def _wait_result(self, function: Callable[..., int], timeout_ms: int) -> object | None:
        result = _BufferDescriptor()
        error = _BufferDescriptor()
        status = function(self._handle, timeout_ms, ctypes.byref(result), ctypes.byref(error))
        if status == _WAIT_TIMEOUT:
            if result.buffer_handle or result.byte_length or error.buffer_handle or error.byte_length:
                raise sdk_error("The Cynapsa Core library returned an invalid wait result")
            return None
        self._check(status, error)
        return self._read_json(result)

    def _mutation(self, function: Callable[..., int], *arguments: object) -> None:
        error = _BufferDescriptor()
        status = function(*arguments, ctypes.byref(error))
        self._check(status, error)

    def _check(self, status: int, error: _BufferDescriptor) -> None:
        if status == _OK:
            if error.buffer_handle or error.byte_length:
                self._discard(error)
                raise sdk_error("The Cynapsa Core library returned an invalid success result")
            return
        if status == _ERROR and error.buffer_handle:
            try:
                document = json.loads(self._copy_and_free(error).decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                raise sdk_error("The native core returned an invalid error document") from None
            raise error_from_document(document)
        self._discard(error)
        raise sdk_error("The Cynapsa Core library returned an invalid ABI status")

    def _read_json(self, descriptor: _BufferDescriptor) -> object:
        try:
            return json.loads(self._copy_and_free(descriptor).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise sdk_error("The native core returned an invalid V1 document") from None

    def _copy_and_free(self, descriptor: _BufferDescriptor) -> bytes:
        handle = int(descriptor.buffer_handle)
        length = int(descriptor.byte_length)
        if handle == 0 or length <= 0 or length > _MAX_BUFFER_BYTES:
            self._discard(descriptor)
            raise sdk_error("The Cynapsa Core library returned an invalid buffer descriptor")
        destination = (ctypes.c_uint8 * length)()
        copied = ctypes.c_uint64()
        read_error = _BufferDescriptor()
        read_status = self._buffer_read(
            handle,
            0,
            destination,
            length,
            ctypes.byref(copied),
            ctypes.byref(read_error),
        )
        if read_status != _OK or copied.value != length:
            self._discard(read_error)
            self._discard(descriptor)
            raise sdk_error("The Cynapsa Core library could not copy an ABI buffer")
        data = bytes(destination)
        free_error = _BufferDescriptor()
        free_status = self._buffer_free(handle, ctypes.byref(free_error))
        if free_status != _OK:
            self._discard(free_error)
            raise sdk_error("The Cynapsa Core library could not retire an ABI buffer")
        return data

    def _discard(self, descriptor: _BufferDescriptor) -> None:
        if not descriptor.buffer_handle:
            return
        nested = _BufferDescriptor()
        self._buffer_free(descriptor.buffer_handle, ctypes.byref(nested))
        if nested.buffer_handle and nested.buffer_handle != descriptor.buffer_handle:
            final = _BufferDescriptor()
            self._buffer_free(nested.buffer_handle, ctypes.byref(final))


def _absolute_library_path(value: str | os.PathLike[str]) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise sdk_error("The Cynapsa Core library path must be absolute")
    try:
        resolved = path.resolve(strict=True)
    except OSError:
        raise sdk_error("The Cynapsa Core library path does not exist") from None
    if not resolved.is_file():
        raise sdk_error("The Cynapsa Core library path is not a file")
    return resolved


def _input(value: bytes) -> tuple[ctypes.Array[ctypes.c_uint8], int]:
    if not value:
        raise sdk_error("A native ABI input document cannot be empty")
    data = (ctypes.c_uint8 * len(value)).from_buffer_copy(value)
    return data, len(value)
