"""Public Python client backed only by the V1 shared-library ABI."""

from __future__ import annotations

import json
import os
from typing import Mapping, get_args
from uuid import uuid4

from ._native import NativeCore
from .errors import sdk_error
from .models import (
    Admission,
    CommandName,
    Completion,
    CoreConfig,
    CoreEvent,
    CoreStatus,
    JsonObject,
    JsonValue,
    MAX_TIMEOUT_MS,
)


class CynapsaClient:
    """Owns one native core and exposes only the versioned public contract."""

    def __init__(
        self,
        library_path: str | os.PathLike[str] | None = None,
        *,
        config: CoreConfig | None = None,
        sdk_session_id: str | None = None,
    ) -> None:
        selected_path = library_path or os.environ.get("CYNAPSA_CORE_LIBRARY")
        if not selected_path:
            raise sdk_error("An absolute Cynapsa Core library path is required")
        self._session_id = sdk_session_id or f"session-{uuid4()}"
        _validate_identifier(self._session_id, "sdk_session_id")
        self._native = NativeCore(selected_path, _encode((config or CoreConfig()).to_document()))
        self._state = "created"

    @property
    def abi_version(self) -> int:
        return self._native.abi_version

    @property
    def sdk_session_id(self) -> str:
        return self._session_id

    def start(self) -> None:
        self._require_state("created")
        self._native.start()
        self._state = "started"

    def initialize(self, *, command_id: str | None = None) -> Admission:
        return self._submit("core.init", command_id=command_id)

    def request_capabilities(self, *, command_id: str | None = None) -> Admission:
        return self._submit("core.capabilities", command_id=command_id)

    def request_status(self, *, command_id: str | None = None) -> Admission:
        return self._submit("core.status", command_id=command_id)

    def token_login(
        self, token: str, mesh_id: str, *, profile_id: str | None = None, command_id: str | None = None,
    ) -> Admission:
        """Enroll and authenticate for HTTP Bridge mode; await its completion."""
        args: JsonObject = {"token": token, "mesh_id": mesh_id}
        if profile_id is not None:
            args["profile_id"] = profile_id
        return self._submit("auth.token_login", args, command_id=command_id)

    def token_connect(
        self, token: str, mesh_id: str, *, profile_id: str | None = None, command_id: str | None = None,
    ) -> Admission:
        """Enroll and authenticate for native mode; await its completion."""
        args: JsonObject = {"token": token, "mesh_id": mesh_id}
        if profile_id is not None:
            args["profile_id"] = profile_id
        return self._submit("auth.token_connect", args, command_id=command_id)

    def installation_login(
        self, profile_id: str, mesh_id: str, *, command_id: str | None = None,
    ) -> Admission:
        """Authenticate an enrolled local installation for HTTP Bridge mode."""
        return self._submit("auth.installation_login", {"profile_id": profile_id, "mesh_id": mesh_id}, command_id=command_id)

    def installation_connect(
        self, profile_id: str, mesh_id: str, *, command_id: str | None = None,
    ) -> Admission:
        """Authenticate an enrolled local installation for native mode."""
        return self._submit("auth.installation_connect", {"profile_id": profile_id, "mesh_id": mesh_id}, command_id=command_id)

    def _submit(
        self,
        command_name: CommandName,
        args: Mapping[str, JsonValue] | None = None,
        *,
        command_id: str | None = None,
    ) -> Admission:
        self._require_state("started")
        if command_name not in get_args(CommandName):
            raise sdk_error("The SDK command name is not part of the V1 contract")
        selected_command_id = command_id or f"command-{uuid4()}"
        _validate_identifier(selected_command_id, "command_id")
        document: JsonObject = {
            "abi_version": 1,
            "command_id": selected_command_id,
            "command_name": command_name,
            "sdk_session_id": self._session_id,
            "args": dict(args or {}),
        }
        return Admission.from_document(self._native.submit(_encode(document)))

    def cancel(self, command_handle: str) -> None:
        self._require_state("started")
        self._native.cancel(_encode({"abi_version": 1, "handle": command_handle}))

    def next_completion(self, timeout_ms: int = 0) -> Completion | None:
        self._require_state("started")
        _validate_timeout(timeout_ms)
        value = self._native.next_completion(timeout_ms)
        return None if value is None else Completion.from_document(value)

    def next_event(self, timeout_ms: int = 0) -> CoreEvent | None:
        self._require_state("started")
        _validate_timeout(timeout_ms)
        value = self._native.next_event(timeout_ms)
        return None if value is None else CoreEvent.from_document(value)

    def status(self) -> CoreStatus:
        self._require_open()
        return CoreStatus.from_document(self._native.status())

    def shutdown(self, timeout_ms: int = 0) -> None:
        if self._state in {"shutdown", "closed"}:
            return
        self._require_state("started")
        _validate_timeout(timeout_ms)
        try:
            self._native.shutdown(timeout_ms)
        except Exception:
            self._state = "closing"
            raise
        self._state = "shutdown"

    def close(self, timeout_ms: int = 0) -> None:
        if self._state == "closed":
            return
        shutdown_error: Exception | None = None
        if self._state in {"created", "started", "closing"}:
            try:
                _validate_timeout(timeout_ms)
                self._native.shutdown(timeout_ms)
                self._state = "shutdown"
            except Exception as error:
                self._state = "closing"
                shutdown_error = error
        try:
            self._native.destroy()
        except Exception as destroy_error:
            if shutdown_error is not None:
                raise ExceptionGroup("Cynapsa client teardown failed", [shutdown_error, destroy_error])
            raise
        else:
            self._state = "closed"
        if shutdown_error is not None:
            raise shutdown_error

    def __enter__(self) -> "CynapsaClient":
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        self.close()

    def _require_state(self, expected: str) -> None:
        if self._state != expected:
            raise sdk_error(f"This operation requires a client in the {expected} state")

    def _require_open(self) -> None:
        if self._state == "closed":
            raise sdk_error("This operation requires an open client")


def _encode(document: object) -> bytes:
    try:
        return json.dumps(
            document,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise sdk_error("The SDK command contains a value that is not valid V1 JSON") from None


def _validate_timeout(value: int) -> None:
    if type(value) is not int or value < 0 or value > MAX_TIMEOUT_MS:
        raise sdk_error("timeout_ms is outside the V1 range")


def _validate_identifier(value: str, name: str) -> None:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 512:
        raise sdk_error(f"{name} is outside the V1 range")
