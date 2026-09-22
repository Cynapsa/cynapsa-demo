"""Native authentication and application-level Session lifecycle."""

from __future__ import annotations

import asyncio
import base64
import binascii
import os
import queue
import secrets
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, NoReturn, TypeVar

from cynapsa._inbound import AztmLocalDiagnostic, InboundRuntime
from cynapsa.events import AztmEvent
from cynapsa.exceptions import NativeError, SdkSafetyTimeout
from cynapsa.http import CynapsaRequest, CynapsaResponse, HTTPRequestPayload, HTTPResponsePayload
from cynapsa.messaging import (
    AztmResponse,
    AztmSendResult,
    NativePayload,
    _PayloadTooLarge,
    _canonical_size,
    _native_payload_from_wire,
    _request_handle,
    RequestPayload,
    ResponsePayload,
)
from cynapsa.native.abi import ABI_VERSION, CYNAPSA_STATUS_V1_ERROR
from cynapsa.native.command import (
    COMMAND_NAMES,
    MAX_MESH_ID_BYTES,
    MAX_PUBLIC_STRING_BYTES,
    PUBLIC_ERROR_MESSAGES,
    CommandCompletion,
    CommandFuture,
    _validate_agent_id,
    _validate_enrollment_token,
    _validate_mesh_endpoint,
    _validate_profile_id,
)
from cynapsa.native.core import (
    MAX_PAYLOAD_BYTES,
    MAX_QUEUE_LIMIT,
    MAX_TIMEOUT_MS,
    NativeCore,
)

_DEFAULT_COMMAND_TIMEOUT_MS = 30_000
_DEFAULT_QUEUE_LIMIT = 1_024
_DEFAULT_PAYLOAD_LIMIT = MAX_PAYLOAD_BYTES
_RPC_COMPLETION_SAFETY_WINDOW_MS = 5_000
_PERSONALITY_NATIVE = "native"
_PERSONALITY_HTTP_BRIDGE = "http_bridge"
_CORE_CAPABILITIES = frozenset(
    {
        "native_messaging",
        "rpc",
        "http_bridge",
        "offline_delivery",
        "large_payloads",
        "payload_streaming_handles",
        "local_command_cancellation",
        "bounded_queues",
        "event_stream",
    }
)
_SUPPORTED_CAPABILITIES = _CORE_CAPABILITIES - {
    "large_payloads",
    "payload_streaming_handles",
}
_LIFECYCLES = frozenset(
    {"created", "connecting", "ready", "degraded", "closing", "closed", "failed"}
)
_CONNECTIVITY_STATES = frozenset(
    {"unknown", "available", "degraded", "unavailable"}
)
_PUBLIC_ERROR_STAGES = frozenset(
    {
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
)


class _OperationCancelled(Exception):
    pass


def _sdk_error(
    code: str,
    message: str,
    *,
    retryable: bool = False,
    stage: str = "sdk",
    local_or_remote: str = "local",
) -> NativeError:
    return NativeError(
        CYNAPSA_STATUS_V1_ERROR,
        code,
        message,
        {
            "retryable": retryable,
            "stage": stage,
            "local_or_remote": local_or_remote,
        },
    )


def _contract_error(operation: str, reason: str) -> NativeError:
    del operation, reason
    return _sdk_error(
        "core_error",
        PUBLIC_ERROR_MESSAGES["core_error"],
    )


def _authentication_error(reason: str) -> NativeError:
    del reason
    return _sdk_error(
        "authentication_failed",
        PUBLIC_ERROR_MESSAGES["authentication_failed"],
        stage="auth",
    )


def _valid_diagnostic_id(value: object) -> bool:
    if type(value) is not str:
        return False
    try:
        decoded = base64.b64decode(
            value + "=" * (-len(value) % 4), altchars=b"-_", validate=True
        )
    except (ValueError, binascii.Error):
        return False
    return (
        len(decoded) == 32
        and base64.urlsafe_b64encode(decoded).rstrip(b"=").decode("ascii") == value
    )


def _public_exception(error: BaseException) -> BaseException:
    """Project one internal failure into the closed Session error surface."""

    if isinstance(error, NativeError):
        if (
            error.code not in PUBLIC_ERROR_MESSAGES
            or error.message != PUBLIC_ERROR_MESSAGES[error.code]
        ):
            return _contract_error("session", "unmapped internal failure")
        details = error.details
        retryable = details.get("retryable", False)
        stage = details.get("stage", "sdk")
        location = details.get("local_or_remote", "local")
        if type(retryable) is not bool:
            retryable = False
        if stage not in _PUBLIC_ERROR_STAGES:
            stage = "sdk"
        if location not in {"local", "remote"}:
            location = "local"
        projected: dict[str, Any] = {
            "retryable": retryable,
            "stage": stage,
            "local_or_remote": location,
        }
        diagnostic_id = details.get("diagnostic_id")
        if _valid_diagnostic_id(diagnostic_id):
            projected["diagnostic_id"] = diagnostic_id
        return NativeError(
            CYNAPSA_STATUS_V1_ERROR,
            error.code,
            error.message,
            projected,
        )
    if isinstance(error, SdkSafetyTimeout):
        return SdkSafetyTimeout(
            "the SDK did not receive a terminal RPC completion before the safety deadline"
        )
    if isinstance(error, TimeoutError):
        return TimeoutError("the AZTM command did not complete before its timeout")
    if isinstance(error, ValueError):
        # Clone field-only validation failures so nested causes and frames
        # cannot retain authentication input.
        return ValueError(str(error))
    if isinstance(error, TypeError):
        return TypeError(str(error))
    if isinstance(error, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
        return error
    return _contract_error("session", "unexpected SDK failure")


def _raise_public(error: BaseException) -> NoReturn:
    projected = _public_exception(error)
    del error
    projected.__cause__ = None
    projected.__context__ = None
    raise projected.with_traceback(None) from None


def _utf8_length(value: str) -> int:
    try:
        return len(value.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise ValueError("invalid Unicode") from exc


def _public_string(
    value: object,
    field: str,
    *,
    maximum: int,
    allow_empty: bool = False,
) -> str:
    if type(value) is not str or (not allow_empty and not value):
        raise ValueError(f"{field} must be a nonempty string")
    if _utf8_length(value) > maximum:
        raise ValueError(f"{field} exceeds its {maximum}-byte limit")
    return value


def _integer(value: object, field: str, minimum: int, maximum: int) -> int:
    if (
        type(value) is not int
        or not minimum <= value <= maximum
    ):
        raise ValueError(f"{field} must be an integer in {minimum}..{maximum}")
    return value


def _new_identifier(prefix: str) -> str:
    # Exactly 32 random bytes produce 43 unpadded base64url characters. The
    # complete internal identifiers therefore have one fixed, testable shape.
    encoded = base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=")
    return f"{prefix}_{encoded.decode('ascii')}"


@dataclass(frozen=True, slots=True)
class _RuntimeConfig:
    command_timeout_ms: int
    rpc_timeout_ms: int
    queue_limit: int
    payload_limit: int

    @property
    def effective_command_timeout(self) -> float:
        milliseconds = self.command_timeout_ms or _DEFAULT_COMMAND_TIMEOUT_MS
        return milliseconds / 1_000

    @property
    def effective_rpc_timeout_ms(self) -> int:
        return self.rpc_timeout_ms or _DEFAULT_COMMAND_TIMEOUT_MS

    def core_create(self) -> dict[str, int]:
        return {
            "abi_version": ABI_VERSION,
            "command_timeout_ms": self.command_timeout_ms,
            "rpc_timeout_ms": self.rpc_timeout_ms,
            "queue_limit": self.queue_limit,
            "payload_limit": self.payload_limit,
        }


@dataclass(frozen=True, slots=True)
class _AuthenticationConfig:
    mode: str
    mesh_id: str
    mesh_endpoint: str
    username: str
    profile_id: str | None
    agent_instance_id: str


@dataclass(frozen=True, slots=True)
class _ConnectConfig:
    runtime: _RuntimeConfig
    authentication: _AuthenticationConfig

    @property
    def mesh_endpoint(self) -> str:
        return self.authentication.mesh_endpoint

    @property
    def username(self) -> str:
        return self.authentication.username

    @property
    def mesh_id(self) -> str:
        return self.authentication.mesh_id

    @property
    def profile_id(self) -> str | None:
        return self.authentication.profile_id

    @property
    def agent_instance_id(self) -> str:
        return self.authentication.agent_instance_id

    @property
    def auth_mode(self) -> str:
        return self.authentication.mode

    @property
    def command_timeout_ms(self) -> int:
        return self.runtime.command_timeout_ms

    @property
    def rpc_timeout_ms(self) -> int:
        return self.runtime.rpc_timeout_ms

    @property
    def queue_limit(self) -> int:
        return self.runtime.queue_limit

    @property
    def payload_limit(self) -> int:
        return self.runtime.payload_limit

    @property
    def effective_command_timeout(self) -> float:
        return self.runtime.effective_command_timeout

    @property
    def effective_rpc_timeout_ms(self) -> int:
        return self.runtime.effective_rpc_timeout_ms

    def core_create(self) -> dict[str, int]:
        # Authentication material deliberately cannot be represented here.
        return self.runtime.core_create()


def _validate_config(
    *,
    mesh_id: object,
    mesh_endpoint: object = None,
    username: object = None,
    password: object = None,
    enrollment_token: object = None,
    profile_id: object = "default",
    command_timeout_ms: object,
    rpc_timeout_ms: object,
    queue_limit: object,
    payload_limit: object,
) -> _ConnectConfig:
    # Validate all public input before loading or creating a native Core. Error
    # text identifies fields but never includes caller-provided values.
    try:
        mesh = _public_string(mesh_id, "mesh_id", maximum=MAX_MESH_ID_BYTES)
        legacy_fields = (mesh_endpoint, username, password)
        legacy_selected = any(value is not None for value in legacy_fields)
        if legacy_selected:
            if enrollment_token is not None:
                raise ValueError(
                    "enrollment_token cannot be combined with legacy credentials"
                )
            if not all(value is not None for value in legacy_fields):
                raise ValueError(
                    "mesh_endpoint, username, and password must be supplied together"
                )
            if type(mesh_endpoint) is not str:
                raise ValueError("mesh_endpoint must be a nonempty string")
            endpoint = _validate_mesh_endpoint(mesh_endpoint)
            user = _validate_agent_id(username, "username")
            _public_string(
                password, "password", maximum=MAX_PUBLIC_STRING_BYTES
            )
            if profile_id != "default":
                raise ValueError("profile_id is unavailable with legacy credentials")
            auth = _AuthenticationConfig(
                "legacy", mesh, endpoint, user, None, _new_identifier("agentinst")
            )
        elif enrollment_token is not None:
            _validate_enrollment_token(enrollment_token)
            selected_profile = _validate_profile_id(profile_id)
            auth = _AuthenticationConfig(
                "token", mesh, "", "", selected_profile, ""
            )
        else:
            selected_profile = _validate_profile_id(profile_id)
            auth = _AuthenticationConfig(
                "installation", mesh, "", "", selected_profile, ""
            )
        command_timeout = _integer(
            command_timeout_ms, "command_timeout_ms", 0, MAX_TIMEOUT_MS
        )
        rpc_timeout = _integer(rpc_timeout_ms, "rpc_timeout_ms", 0, MAX_TIMEOUT_MS)
        queue = _integer(queue_limit, "queue_limit", 1, MAX_QUEUE_LIMIT)
        payload = _integer(payload_limit, "payload_limit", 1, MAX_PAYLOAD_BYTES)
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError("AZTM connection configuration is invalid") from exc
    return _ConnectConfig(
        _RuntimeConfig(command_timeout, rpc_timeout, queue, payload),
        auth,
    )


@dataclass(frozen=True, slots=True)
class AztmStatus:
    """Immutable, application-level Session status."""

    lifecycle: str
    connectivity: str
    personality: str
    agent_id: str
    mesh_id: str
    mesh_endpoint: str
    queued_message_count: int


@dataclass(frozen=True, slots=True)
class AztmCapabilities:
    """Immutable allowlisted commands and application capabilities."""

    commands: tuple[str, ...]
    features: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AztmAuthInfo:
    """Immutable authentication metadata returned by the Core."""

    profile_id: str | None = None
    installation_id: str | None = None
    credential_expires_at: str | None = None
    offline_start_deadline: str | None = None
    offline_cold_start_target_seconds: int | None = None
    offline_target_satisfied: bool | None = None
    policy_revision: int | None = None
    session_expiry_mode: str | None = None
    preparation_status: str | None = None


class _LifecycleOwner:
    """Recoverable owner shared by sync and async public personalities."""

    __slots__ = (
        "core",
        "sdk_session_id",
        "command_timeout",
        "rpc_timeout_ms",
        "payload_limit",
        "shutdown_timeout_ms",
        "_lock",
        "_state_changed",
        "_close_lock",
        "_close_started",
        "_active_operations",
        "_authenticated",
        "_logout_future",
    )

    def __init__(self, core: NativeCore, config: _ConnectConfig) -> None:
        self.core: NativeCore | None = core
        self.sdk_session_id = _new_identifier("sdk")
        self.command_timeout = config.effective_command_timeout
        self.rpc_timeout_ms = config.effective_rpc_timeout_ms
        self.payload_limit = config.payload_limit
        self.shutdown_timeout_ms = config.command_timeout_ms
        self._lock = threading.Lock()
        self._state_changed = threading.Condition(self._lock)
        self._close_lock = threading.Lock()
        self._close_started = False
        self._active_operations = 0
        self._authenticated = False
        self._logout_future: CommandFuture | None = None

    def _submit(self, name: str, args: Mapping[str, Any] | None = None) -> CommandFuture:
        core = self.core
        if core is None:
            raise _sdk_error(
                "shutdown_in_progress", PUBLIC_ERROR_MESSAGES["shutdown_in_progress"]
            )
        return core.submit(
            name,
            self.sdk_session_id,
            args,
            command_id=_new_identifier("cmd"),
        )

    def require_open(self) -> None:
        with self._state_changed:
            if self._close_started or self.core is None:
                raise _sdk_error(
                    "shutdown_in_progress",
                    PUBLIC_ERROR_MESSAGES["shutdown_in_progress"],
                )

    def _wait(
        self,
        future: CommandFuture,
        cancel_event: threading.Event | None = None,
        *,
        timeout: float | None = None,
        timeout_error: BaseException | None = None,
    ) -> CommandCompletion:
        wait_timeout = self.command_timeout if timeout is None else timeout
        deadline = time.monotonic() + wait_timeout
        while True:
            if cancel_event is not None and cancel_event.is_set():
                try:
                    future.cancel()
                except NativeError:
                    pass
                # Direct cancellation publishes exactly one terminal Core
                # completion. Drain it before the worker exits so async task
                # cancellation cannot strand a pending future. The caller's
                # asyncio cancellation remains authoritative.
                try:
                    future.completion(self.command_timeout)
                except BaseException:
                    pass
                raise _OperationCancelled
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                # A completion that won the exact deadline race remains
                # authoritative and must not be followed by cancellation.
                try:
                    return future.result(0)
                except TimeoutError:
                    pass
                except BaseException:
                    raise
                try:
                    future.cancel()
                except NativeError:
                    pass
                if timeout_error is not None:
                    # Keep the future registered after direct cancellation.
                    # Core's terminal cancellation completion still owns and
                    # drains the pending command even though this caller has
                    # reached the SDK-only safety guard.
                    raise timeout_error
                # The Core deadline can win just before the callback reaches
                # this binding. Join the terminal completion after direct
                # cancellation: a successful response or a normalized Core
                # failure is authoritative; request_cancelled means this local
                # timeout won.
                try:
                    completion = future.result(self.command_timeout)
                except NativeError as exc:
                    if exc.code != "request_cancelled":
                        raise
                except TimeoutError:
                    pass
                else:
                    return completion
                raise TimeoutError("the AZTM command did not complete before its timeout")
            try:
                return future.result(min(remaining, 0.05))
            except TimeoutError:
                continue

    def execute(
        self,
        name: str,
        args: Mapping[str, Any] | None = None,
        *,
        cancel_event: threading.Event | None = None,
        timeout: float | None = None,
        timeout_error: BaseException | None = None,
    ) -> CommandCompletion:
        with self._state_changed:
            if self._close_started or self.core is None:
                raise _sdk_error(
                    "shutdown_in_progress",
                    PUBLIC_ERROR_MESSAGES["shutdown_in_progress"],
                )
            self._active_operations += 1
        try:
            return self._wait(
                self._submit(name, args),
                cancel_event,
                timeout=timeout,
                timeout_error=timeout_error,
            )
        finally:
            with self._state_changed:
                self._active_operations -= 1
                self._state_changed.notify_all()

    def _begin_logout(self) -> CommandFuture | None:
        if not self._authenticated:
            return None
        if self._logout_future is None:
            self._logout_future = self._submit("auth.logout")
        return self._logout_future

    def close(self) -> None:
        """Close in protocol order; retain ownership after any unsafe phase."""

        with self._close_lock:
            with self._state_changed:
                self._close_started = True
                while self._active_operations:
                    self._state_changed.wait()
            core = self.core
            if core is None:
                return
            logout_error: BaseException | None = None
            if core.lifecycle not in {"closing", "closed", "destroyed"}:
                try:
                    logout = self._begin_logout()
                    if logout is not None:
                        completion = self._wait(logout)
                        _require_result(completion, "empty", "auth.logout")
                        self._authenticated = False
                except BaseException as exc:
                    logout_error = exc

            # Do not overtake a still-pending logout with shutdown. A later
            # close joins the same future; the Session remains its recoverable
            # owner in the meantime.
            if isinstance(logout_error, TimeoutError):
                raise logout_error

            # A terminal logout can close the runtime itself. NativeCore's
            # shutdown remains the authoritative join/drain operation.
            if core.lifecycle not in {"closed", "destroyed"}:
                core.shutdown(self.shutdown_timeout_ms)
            if core.lifecycle == "closed":
                core.destroy()
            if core.lifecycle == "destroyed":
                self.core = None
            if logout_error is not None:
                raise logout_error


_RETAINED_OWNERS: set[_LifecycleOwner] = set()
_RETAINED_OWNERS_LOCK = threading.Lock()


def _retain_owner(owner: _LifecycleOwner) -> None:
    with _RETAINED_OWNERS_LOCK:
        _RETAINED_OWNERS.add(owner)


def _release_owner(owner: _LifecycleOwner) -> None:
    with _RETAINED_OWNERS_LOCK:
        _RETAINED_OWNERS.discard(owner)


def _cleanup_setup(owner: _LifecycleOwner) -> None:
    try:
        owner.close()
    except BaseException:
        # Shutdown/clear timeouts explicitly require the callback, Core, and
        # library owner to remain strongly reachable for a later safe join.
        _retain_owner(owner)
    else:
        _release_owner(owner)


@dataclass(frozen=True, slots=True)
class _Identity:
    username: str
    agent_id: str
    mesh_endpoint: str
    mesh_id: str
    agent_instance_id: str
    personality: str
    auth_info: AztmAuthInfo


def _require_result(
    completion: CommandCompletion, expected: str, operation: str
) -> Mapping[str, Any]:
    if (
        not completion.ok
        or completion.error is not None
        or completion.result_type != expected
        or completion.result is None
    ):
        raise _contract_error(operation, "unexpected result type")
    return completion.result


def _optional_auth_value(
    result: Mapping[str, Any], field: str, expected_type: type[Any]
) -> Any | None:
    """Copy one already strict-decoded optional authentication value."""

    value = result.get(field)
    if value is None:
        return None
    if type(value) is not expected_type:
        raise TypeError(f"{field} has an invalid type")
    return value


def _validate_auth_result(
    result: Mapping[str, Any], config: _ConnectConfig, personality: str
) -> _Identity:
    if result.get("personality") != personality:
        raise _authentication_error("personality mismatch")
    if result.get("mesh_id") != config.mesh_id:
        raise _authentication_error("mesh identity mismatch")
    agent_id = result.get("agent_id")
    if not isinstance(agent_id, str) or not agent_id:
        raise _authentication_error("authenticated agent identity is missing")
    returned_instance = result.get("agent_instance_id")
    if config.auth_mode == "legacy":
        if returned_instance != config.agent_instance_id:
            raise _authentication_error("agent instance identity mismatch")
        instance = config.agent_instance_id
        auth_info = AztmAuthInfo()
    else:
        if not isinstance(returned_instance, str) or not returned_instance:
            raise _authentication_error("installation identity is missing")
        if result.get("profile_id") != config.profile_id:
            raise _authentication_error("profile identity mismatch")
        instance = returned_instance
        try:
            auth_info = AztmAuthInfo(
                profile_id=_optional_auth_value(result, "profile_id", str),
                installation_id=instance,
                credential_expires_at=_optional_auth_value(
                    result, "credential_expires_at", str
                ),
                offline_start_deadline=_optional_auth_value(
                    result, "offline_start_deadline", str
                ),
                offline_cold_start_target_seconds=_optional_auth_value(
                    result, "offline_cold_start_target_seconds", int
                ),
                offline_target_satisfied=_optional_auth_value(
                    result, "offline_target_satisfied", bool
                ),
                policy_revision=_optional_auth_value(
                    result, "policy_revision", int
                ),
                session_expiry_mode=_optional_auth_value(
                    result, "session_expiry_mode", str
                ),
                preparation_status=_optional_auth_value(
                    result, "preparation_status", str
                ),
            )
        except (TypeError, ValueError) as exc:
            raise _authentication_error("authentication metadata is invalid") from exc
    return _Identity(
        config.username,
        agent_id,
        config.mesh_endpoint,
        config.mesh_id,
        instance,
        personality,
        auth_info,
    )


def _status_model(result: Mapping[str, Any], identity: _Identity) -> AztmStatus:
    if result.get("lifecycle") not in _LIFECYCLES:
        raise _contract_error("core.status", "unknown lifecycle")
    if result.get("connectivity") not in _CONNECTIVITY_STATES:
        raise _contract_error("core.status", "unknown connectivity state")
    for field, expected in (
        ("personality", identity.personality),
        ("agent_id", identity.agent_id),
        ("mesh_id", identity.mesh_id),
        ("mesh_endpoint", identity.mesh_endpoint),
    ):
        if result.get(field) != expected:
            raise _contract_error("core.status", f"{field} identity mismatch")
    queued = result.get("queued_message_count")
    if isinstance(queued, bool) or not isinstance(queued, int) or not 0 <= queued <= MAX_QUEUE_LIMIT:
        raise _contract_error("core.status", "invalid queued message count")
    return AztmStatus(
        lifecycle=result["lifecycle"],
        connectivity=result["connectivity"],
        personality=result["personality"],
        agent_id=result["agent_id"],
        mesh_id=result["mesh_id"],
        mesh_endpoint=result["mesh_endpoint"],
        queued_message_count=queued,
    )


def _capabilities_model(result: Mapping[str, Any]) -> AztmCapabilities:
    commands = result.get("commands")
    features = result.get("features")
    if not isinstance(commands, tuple) or not isinstance(features, tuple):
        raise _contract_error("core.capabilities", "invalid collection shape")
    if (
        len(commands) != len(set(commands))
        or any(not isinstance(value, str) or value not in COMMAND_NAMES for value in commands)
    ):
        raise _contract_error("core.capabilities", "invalid command catalog")
    if (
        len(features) != len(set(features))
        or any(not isinstance(value, str) or value not in _CORE_CAPABILITIES for value in features)
    ):
        raise _contract_error("core.capabilities", "invalid feature catalog")
    return AztmCapabilities(
        tuple(commands),
        tuple(value for value in features if value in _SUPPORTED_CAPABILITIES),
    )


def _payload_too_large() -> NativeError:
    return _sdk_error(
        "payload_too_large",
        PUBLIC_ERROR_MESSAGES["payload_too_large"],
        stage="payload",
    )


def _prepare_payload(
    owner: _LifecycleOwner,
    value: object,
    *,
    path: str | None,
    content_type: str | None,
) -> NativePayload:
    try:
        payload = NativePayload.from_value(
            value, path=path, content_type=content_type
        )
    except _PayloadTooLarge:
        raise _payload_too_large() from None
    if (
        _canonical_size(payload.content_type, payload.path, payload.body)
        > owner.payload_limit
    ):
        raise _payload_too_large()
    return payload


def _prepare_outbound_payload(
    owner: _LifecycleOwner,
    value: object,
    *,
    path: str | None,
    content_type: str | None,
) -> NativePayload | HTTPRequestPayload:
    if type(value) is HTTPResponsePayload:
        raise TypeError("HTTPResponsePayload is not an outbound request payload")
    if type(value) is HTTPRequestPayload:
        if path is not None or content_type is not None:
            raise ValueError("path and content_type cannot override an HTTPRequestPayload")
        try:
            value.validate_payload_limit(owner.payload_limit)
        except ValueError:
            raise _payload_too_large() from None
        return value
    native = _prepare_payload(owner, value, path=path, content_type=content_type)
    headers = (("content-type", native.content_type),) if native.content_type else ()
    request = CynapsaRequest("POST", native.path, "", headers, native.body)
    try:
        request.validate_payload_limit(owner.payload_limit)
    except ValueError:
        raise _payload_too_large() from None
    return request


def _prepare_reply_payload(
    owner: _LifecycleOwner,
    value: ResponsePayload,
) -> ResponsePayload:
    if type(value) is NativePayload:
        return _prepare_payload(owner, value, path=None, content_type=None)
    if type(value) is HTTPResponsePayload:
        try:
            value.validate_payload_limit(owner.payload_limit)
        except ValueError:
            raise _payload_too_large() from None
        return value
    raise TypeError("reply payload must be NativePayload or HTTPResponsePayload")


def _send_result_model(
    completion: CommandCompletion, operation: str
) -> AztmSendResult:
    result = _require_result(completion, "send", operation)
    if set(result) != {"message_id", "conversation_id", "accepted"}:
        raise _contract_error(operation, "invalid send result fields")
    try:
        return AztmSendResult(
            message_id=result["message_id"],
            conversation_id=result["conversation_id"],
            accepted=result["accepted"],
        )
    except (TypeError, ValueError) as exc:
        raise _contract_error(operation, "invalid send result") from exc


def _response_model(
    completion: CommandCompletion,
    payload_limit: int,
    *,
    expected_payload: type[NativePayload] | type[HTTPResponsePayload],
) -> AztmResponse:
    result = _require_result(completion, "response", "message.request")
    expected = {
        "message_id",
        "conversation_id",
        "from_agent_id",
        "mesh_id",
        "payload",
    }
    if set(result) != expected:
        raise _contract_error("message.request", "invalid response fields")
    try:
        if expected_payload is NativePayload:
            native_payload = _native_payload_from_wire(result["payload"])
            if (
                _canonical_size(
                    native_payload.content_type,
                    native_payload.path,
                    native_payload.body,
                )
                > payload_limit
            ):
                raise ValueError("response payload exceeds the configured limit")
            payload: ResponsePayload = native_payload
        elif expected_payload is HTTPResponsePayload:
            wire = result["payload"]
            if not isinstance(wire, Mapping) or set(wire) != {"http_response"}:
                raise ValueError("response is not an HTTP response payload")
            payload = HTTPResponsePayload.from_wire(wire["http_response"])
            payload.validate_payload_limit(payload_limit)
        else:
            raise TypeError("unknown expected response payload variant")
        return AztmResponse(
            message_id=result["message_id"],
            conversation_id=result["conversation_id"],
            from_agent_id=result["from_agent_id"],
            mesh_id=result["mesh_id"],
            payload=payload,
        )
    except (TypeError, ValueError) as exc:
        raise _contract_error("message.request", "invalid response result") from exc


def _request_ttl(value: object) -> int:
    return _integer(value, "ttl_ms", 0, MAX_TIMEOUT_MS)


def _effective_request_ttl_ms(owner: _LifecycleOwner, ttl_ms: int) -> int:
    return ttl_ms or owner.rpc_timeout_ms


def _request_wait_timeout_seconds(effective_ttl_ms: int) -> float:
    return (effective_ttl_ms + _RPC_COMPLETION_SAFETY_WINDOW_MS) / 1_000


def _request_safety_timeout() -> SdkSafetyTimeout:
    return SdkSafetyTimeout(
        "the SDK did not receive a terminal RPC completion before the safety deadline"
    )


class _SessionBase:
    __slots__ = ("_owner", "_identity", "_inbound")

    def __init__(
        self, owner: _LifecycleOwner, identity: _Identity, *, asynchronous: bool
    ) -> None:
        self._owner = owner
        self._identity = identity
        self._inbound = InboundRuntime(
            owner, self, asynchronous=asynchronous
        )

    def on(
        self,
        path: str,
        handler: Callable[[Any], Any] | None = None,
    ) -> Any:
        """Register one route for both RPC and one-way message delivery."""

        if handler is not None:
            return self._inbound.register(path, handler)

        def decorator(selected: Callable[[Any], Any]) -> Callable[[Any], Any]:
            return self._inbound.register(path, selected)

        return decorator

    def off(
        self,
        path: str,
        handler: Callable[[Any], Any] | None = None,
    ) -> bool:
        """Remove one Python route; missing or mismatched routes return false."""

        return self._inbound.unregister(path, handler)

    @property
    def username(self) -> str:
        return self._identity.username

    @property
    def agent_id(self) -> str:
        return self._identity.agent_id

    @property
    def mesh_endpoint(self) -> str:
        return self._identity.mesh_endpoint

    @property
    def mesh_id(self) -> str:
        return self._identity.mesh_id

    @property
    def personality(self) -> str:
        return self._identity.personality

    @property
    def auth_info(self) -> AztmAuthInfo:
        return self._identity.auth_info

    @property
    def profile_id(self) -> str | None:
        return self._identity.auth_info.profile_id

    @property
    def installation_id(self) -> str | None:
        return self._identity.auth_info.installation_id

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(username={self.username!r}, "
            f"agent_id={self.agent_id!r}, mesh_endpoint={self.mesh_endpoint!r}, "
            f"mesh_id={self.mesh_id!r}, personality={self.personality!r})"
        )


class AztmSession(_SessionBase):
    """Synchronous native AZTM Session."""

    def send(
        self,
        to: str,
        payload: object,
        *,
        path: str | None = None,
        content_type: str | None = None,
    ) -> AztmSendResult:
        failure: BaseException | None = None
        try:
            self._owner.require_open()
            recipient = _validate_agent_id(to, "to")
            prepared = _prepare_outbound_payload(
                self._owner,
                payload,
                path=path,
                content_type=content_type,
            )
            completion = self._owner.execute(
                "message.send", {"to": recipient, "payload": prepared._wire()}
            )
            return _send_result_model(completion, "message.send")
        except BaseException as error:
            failure = error
        assert failure is not None
        public_failure = _public_exception(failure)
        failure = None
        _raise_public(public_failure)

    def request(
        self,
        to: str,
        payload: object,
        *,
        path: str | None = None,
        content_type: str | None = None,
        ttl_ms: int = 0,
    ) -> CynapsaResponse:
        failure: BaseException | None = None
        try:
            self._owner.require_open()
            recipient = _validate_agent_id(to, "to")
            ttl = _request_ttl(ttl_ms)
            prepared = _prepare_outbound_payload(
                self._owner,
                payload,
                path=path,
                content_type=content_type,
            )
            effective_ms = _effective_request_ttl_ms(self._owner, ttl)
            completion = self._owner.execute(
                "message.request",
                {"to": recipient, "payload": prepared._wire(), "ttl_ms": ttl},
                timeout=_request_wait_timeout_seconds(effective_ms),
                timeout_error=_request_safety_timeout(),
            )
            response = _response_model(
                completion,
                self._owner.payload_limit,
                expected_payload=HTTPResponsePayload,
            )
            assert type(response.payload) is CynapsaResponse
            return response.payload
        except BaseException as error:
            failure = error
        assert failure is not None
        public_failure = _public_exception(failure)
        failure = None
        _raise_public(public_failure)

    def _reply(self, request_handle: str, payload: ResponsePayload) -> None:
        failure: BaseException | None = None
        try:
            self._owner.require_open()
            handle = _request_handle(request_handle)
            prepared = _prepare_reply_payload(self._owner, payload)
            completion = self._owner.execute(
                "message.reply",
                {"request_handle": handle, "payload": prepared._wire()},
            )
            _send_result_model(completion, "message.reply")
            return
        except BaseException as error:
            failure = error
        assert failure is not None
        public_failure = _public_exception(failure)
        failure = None
        _raise_public(public_failure)

    def _new_request(
        self,
        *,
        message_id: str,
        conversation_id: str,
        from_agent_id: str,
        mesh_id: str,
        payload: RequestPayload,
        request_handle: str | None,
        mode: str = "rpc",
    ) -> CynapsaRequest:
        """Create the universal immutable request exposed to a handler."""

        self._owner.require_open()
        del request_handle
        if type(payload) is NativePayload:
            headers = (("content-type", payload.content_type),) if payload.content_type else ()
            method, path, query, body = "POST", payload.path, "", payload.body
        else:
            method, path, query = payload.method, payload.path, payload.query
            headers, body = payload.headers, payload.body
        return CynapsaRequest(
            method,
            path,
            query,
            headers,
            body,
            message_id,
            conversation_id,
            from_agent_id,
            mesh_id,
            mode,
        )

    def next_event(self, timeout: float | None = None) -> AztmEvent:
        self._owner.require_open()
        return self._inbound.next_event(timeout)

    def next_diagnostic(self, timeout: float | None = None) -> AztmEvent:
        self._owner.require_open()
        return self._inbound.next_diagnostic(timeout)

    def next_local_diagnostic(
        self, timeout: float | None = None
    ) -> AztmLocalDiagnostic:
        self._owner.require_open()
        return self._inbound.next_local_diagnostic(timeout)

    def status(self) -> AztmStatus:
        failure: BaseException | None = None
        try:
            result = _require_result(
                self._owner.execute("core.status"), "status", "core.status"
            )
            return _status_model(result, self._identity)
        except BaseException as error:
            failure = error
        assert failure is not None
        public_failure = _public_exception(failure)
        failure = None
        _raise_public(public_failure)

    def capabilities(self) -> AztmCapabilities:
        failure: BaseException | None = None
        try:
            result = _require_result(
                self._owner.execute("core.capabilities"),
                "capabilities",
                "core.capabilities",
            )
            return _capabilities_model(result)
        except BaseException as error:
            failure = error
        assert failure is not None
        public_failure = _public_exception(failure)
        failure = None
        _raise_public(public_failure)

    def close(self) -> None:
        failure: BaseException | None = None
        try:
            self._inbound.close(self._owner.command_timeout)
            self._owner.close()
        except BaseException as error:
            if self._owner.core is not None:
                _retain_owner(self._owner)
            failure = error
        else:
            _release_owner(self._owner)
        if failure is not None:
            public_failure = _public_exception(failure)
            failure = None
            _raise_public(public_failure)

    def __enter__(self) -> AztmSession:
        self._owner.require_open()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if exc_type is None:
            self.close()
            return
        try:
            self.close()
        except BaseException:
            # The body exception is primary. close() has retained any owner
            # that needs a later safe join.
            pass


_T = TypeVar("_T")


async def _cancellable_offload(
    operation: Callable[[threading.Event], _T],
    *,
    cleanup_result: Callable[[_T], None] | None = None,
) -> _T:
    cancelled = threading.Event()
    task = asyncio.create_task(asyncio.to_thread(operation, cancelled))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        cancelled.set()
        try:
            result = await task
        except _OperationCancelled:
            pass
        except BaseException:
            pass
        else:
            if cleanup_result is not None:
                await asyncio.to_thread(cleanup_result, result)
        raise


async def _uncancellable_offload(operation: Callable[[], _T]) -> _T:
    task = asyncio.create_task(asyncio.to_thread(operation))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        # Teardown owns foreign pointers and must reach a safe terminal or
        # recoverable state before cancellation is delivered to the caller.
        try:
            await task
        except BaseException:
            # The caller's cancellation stays primary; the owner is retained
            # by AsyncAztmSession.close() if teardown did not reach terminal.
            pass
        raise


async def _async_bounded_get(
    operation: Callable[[float | None], _T], timeout: float | None
) -> _T:
    if timeout is not None and timeout < 0:
        raise ValueError("timeout must be nonnegative or None")
    deadline = None if timeout is None else time.monotonic() + timeout
    while True:
        wait = 0.05
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return await asyncio.to_thread(operation, 0)
            wait = min(wait, remaining)
        try:
            return await asyncio.to_thread(operation, wait)
        except queue.Empty:
            if deadline is not None and time.monotonic() >= deadline:
                raise


class AsyncAztmSession(_SessionBase):
    """Asynchronous native AZTM Session backed by callback completions."""

    async def send(
        self,
        to: str,
        payload: object,
        *,
        path: str | None = None,
        content_type: str | None = None,
    ) -> AztmSendResult:
        def operation(cancelled: threading.Event) -> AztmSendResult:
            self._owner.require_open()
            recipient = _validate_agent_id(to, "to")
            prepared = _prepare_outbound_payload(
                self._owner,
                payload,
                path=path,
                content_type=content_type,
            )
            completion = self._owner.execute(
                "message.send",
                {"to": recipient, "payload": prepared._wire()},
                cancel_event=cancelled,
            )
            return _send_result_model(completion, "message.send")

        failure: BaseException | None = None
        try:
            return await _cancellable_offload(operation)
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            failure = error
        assert failure is not None
        public_failure = _public_exception(failure)
        failure = None
        _raise_public(public_failure)

    async def request(
        self,
        to: str,
        payload: object,
        *,
        path: str | None = None,
        content_type: str | None = None,
        ttl_ms: int = 0,
    ) -> CynapsaResponse:
        def operation(cancelled: threading.Event) -> CynapsaResponse:
            self._owner.require_open()
            recipient = _validate_agent_id(to, "to")
            ttl = _request_ttl(ttl_ms)
            prepared = _prepare_outbound_payload(
                self._owner,
                payload,
                path=path,
                content_type=content_type,
            )
            effective_ms = _effective_request_ttl_ms(self._owner, ttl)
            completion = self._owner.execute(
                "message.request",
                {"to": recipient, "payload": prepared._wire(), "ttl_ms": ttl},
                cancel_event=cancelled,
                timeout=_request_wait_timeout_seconds(effective_ms),
                timeout_error=_request_safety_timeout(),
            )
            response = _response_model(
                completion,
                self._owner.payload_limit,
                expected_payload=HTTPResponsePayload,
            )
            assert type(response.payload) is CynapsaResponse
            return response.payload

        failure: BaseException | None = None
        try:
            return await _cancellable_offload(operation)
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            failure = error
        assert failure is not None
        public_failure = _public_exception(failure)
        failure = None
        _raise_public(public_failure)

    async def _reply(self, request_handle: str, payload: ResponsePayload) -> None:
        def operation(cancelled: threading.Event) -> None:
            self._owner.require_open()
            handle = _request_handle(request_handle)
            prepared = _prepare_reply_payload(self._owner, payload)
            completion = self._owner.execute(
                "message.reply",
                {"request_handle": handle, "payload": prepared._wire()},
                cancel_event=cancelled,
            )
            _send_result_model(completion, "message.reply")

        failure: BaseException | None = None
        try:
            await _cancellable_offload(operation)
            return
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            failure = error
        assert failure is not None
        public_failure = _public_exception(failure)
        failure = None
        _raise_public(public_failure)

    def _new_request(
        self,
        *,
        message_id: str,
        conversation_id: str,
        from_agent_id: str,
        mesh_id: str,
        payload: RequestPayload,
        request_handle: str | None,
        mode: str = "rpc",
    ) -> CynapsaRequest:
        """Create the universal immutable request exposed to a handler."""

        self._owner.require_open()
        del request_handle
        if type(payload) is NativePayload:
            headers = (("content-type", payload.content_type),) if payload.content_type else ()
            method, path, query, body = "POST", payload.path, "", payload.body
        else:
            method, path, query = payload.method, payload.path, payload.query
            headers, body = payload.headers, payload.body
        return CynapsaRequest(
            method,
            path,
            query,
            headers,
            body,
            message_id,
            conversation_id,
            from_agent_id,
            mesh_id,
            mode,
        )

    async def next_event(self, timeout: float | None = None) -> AztmEvent:
        self._owner.require_open()
        return await _async_bounded_get(self._inbound.next_event, timeout)

    async def next_diagnostic(self, timeout: float | None = None) -> AztmEvent:
        self._owner.require_open()
        return await _async_bounded_get(self._inbound.next_diagnostic, timeout)

    async def next_local_diagnostic(
        self, timeout: float | None = None
    ) -> AztmLocalDiagnostic:
        self._owner.require_open()
        return await _async_bounded_get(
            self._inbound.next_local_diagnostic, timeout
        )

    async def status(self) -> AztmStatus:
        def operation(cancelled: threading.Event) -> AztmStatus:
            result = _require_result(
                self._owner.execute("core.status", cancel_event=cancelled),
                "status",
                "core.status",
            )
            return _status_model(result, self._identity)

        failure: BaseException | None = None
        try:
            return await _cancellable_offload(operation)
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            failure = error
        assert failure is not None
        public_failure = _public_exception(failure)
        failure = None
        _raise_public(public_failure)

    async def capabilities(self) -> AztmCapabilities:
        def operation(cancelled: threading.Event) -> AztmCapabilities:
            result = _require_result(
                self._owner.execute("core.capabilities", cancel_event=cancelled),
                "capabilities",
                "core.capabilities",
            )
            return _capabilities_model(result)

        failure: BaseException | None = None
        try:
            return await _cancellable_offload(operation)
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            failure = error
        assert failure is not None
        public_failure = _public_exception(failure)
        failure = None
        _raise_public(public_failure)

    async def close(self) -> None:
        failure: BaseException | None = None
        try:
            await _uncancellable_offload(
                lambda: self._inbound.close(self._owner.command_timeout)
            )
            await _uncancellable_offload(self._owner.close)
        except asyncio.CancelledError:
            if self._owner.core is not None:
                _retain_owner(self._owner)
            else:
                _release_owner(self._owner)
            raise
        except BaseException as error:
            if self._owner.core is not None:
                _retain_owner(self._owner)
            failure = error
        else:
            _release_owner(self._owner)
        if failure is not None:
            public_failure = _public_exception(failure)
            failure = None
            _raise_public(public_failure)

    async def __aenter__(self) -> AsyncAztmSession:
        self._owner.require_open()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if exc_type is None:
            await self.close()
            return
        try:
            await self.close()
        except BaseException:
            # Preserve the async body exception under the same close policy as
            # the synchronous context manager.
            pass


CoreFactory = Callable[[_ConnectConfig], NativeCore]


def _default_core_factory(
    config: _ConnectConfig,
    *,
    library: Any | None = None,
    library_path: str | os.PathLike[str] | None = None,
) -> NativeCore:
    return NativeCore.create(
        config.core_create(), library=library, library_path=library_path
    )


def _authenticate_sync(
    config: _ConnectConfig,
    password: str,
    personality: str,
    *,
    core_factory: CoreFactory,
    cancel_event: threading.Event | None = None,
) -> tuple[_LifecycleOwner, _Identity]:
    owner: _LifecycleOwner | None = None
    try:
        core = core_factory(config)
        owner = _LifecycleOwner(core, config)
        core.start()
        core.register_callbacks(config.queue_limit)
        init_result = _require_result(
            owner.execute("core.init", cancel_event=cancel_event),
            "core_init",
            "core.init",
        )
        if init_result.get("sdk_session_id") != owner.sdk_session_id:
            raise _contract_error("core.init", "SDK session identity mismatch")

        auth_suffix = (
            "connect" if personality == _PERSONALITY_NATIVE else "login"
        )
        if config.auth_mode == "legacy":
            auth_name = f"auth.{auth_suffix}"
            auth_args: dict[str, Any] = {
                "mesh_endpoint": config.mesh_endpoint,
                "username": config.username,
                "password": password,
                "mesh_id": config.mesh_id,
                "agent_instance_id": config.agent_instance_id,
            }
        elif config.auth_mode == "token":
            auth_name = f"auth.token_{auth_suffix}"
            auth_args = {
                "token": password,
                "mesh_id": config.mesh_id,
                "profile_id": config.profile_id,
            }
        else:
            auth_name = f"auth.installation_{auth_suffix}"
            auth_args = {
                "profile_id": config.profile_id,
                "mesh_id": config.mesh_id,
            }
        try:
            auth_completion = owner.execute(
                auth_name, auth_args, cancel_event=cancel_event
            )
        finally:
            auth_args.clear()
            password = ""
        auth_result = _require_result(auth_completion, "auth", auth_name)
        # A conforming successful auth result is the first point at which the
        # SDK knows logout is valid. Admission, timeout, cancellation, failure,
        # or an ambiguous result must be cleaned up by shutdown only.
        owner._authenticated = True
        identity = _validate_auth_result(auth_result, config, personality)
        return owner, identity
    except BaseException:
        password = ""
        if owner is not None:
            _cleanup_setup(owner)
        raise


def _connect_sync_internal(
    *,
    config: _ConnectConfig,
    password: str,
    personality: str,
    core_factory: CoreFactory,
    cancel_event: threading.Event | None = None,
) -> tuple[_LifecycleOwner, _Identity]:
    try:
        return _authenticate_sync(
            config,
            password,
            personality,
            core_factory=core_factory,
            cancel_event=cancel_event,
        )
    except BaseException as exc:
        password = ""
        raise exc.with_traceback(None) from None


def connect(
    *,
    mesh_id: str,
    enrollment_token: str | None = None,
    profile_id: str = "default",
    mesh_endpoint: str | None = None,
    username: str | None = None,
    password: str | None = None,
    command_timeout_ms: int = 0,
    rpc_timeout_ms: int = 0,
    queue_limit: int = _DEFAULT_QUEUE_LIMIT,
    payload_limit: int = _DEFAULT_PAYLOAD_LIMIT,
) -> AztmSession:
    """Authenticate and return a ready synchronous native Session."""

    connected: AztmSession | None = None
    failure: BaseException | None = None
    secret = ""
    try:
        config = _validate_config(
            mesh_id=mesh_id,
            mesh_endpoint=mesh_endpoint,
            username=username,
            password=password,
            enrollment_token=enrollment_token,
            profile_id=profile_id,
            command_timeout_ms=command_timeout_ms,
            rpc_timeout_ms=rpc_timeout_ms,
            queue_limit=queue_limit,
            payload_limit=payload_limit,
        )
        secret = enrollment_token if config.auth_mode == "token" else password or ""
        enrollment_token = None
        password = None
        owner, identity = _connect_sync_internal(
            config=config,
            password=secret,
            personality=_PERSONALITY_NATIVE,
            core_factory=_default_core_factory,
        )
        secret = ""
        connected = AztmSession(owner, identity, asynchronous=False)
    except BaseException as error:
        failure = error
    enrollment_token = None
    password = None
    secret = ""
    if failure is not None:
        public_failure = _public_exception(failure)
        failure = None
        _raise_public(public_failure)
    assert connected is not None
    return connected


async def connect_async(
    *,
    mesh_id: str,
    enrollment_token: str | None = None,
    profile_id: str = "default",
    mesh_endpoint: str | None = None,
    username: str | None = None,
    password: str | None = None,
    command_timeout_ms: int = 0,
    rpc_timeout_ms: int = 0,
    queue_limit: int = _DEFAULT_QUEUE_LIMIT,
    payload_limit: int = _DEFAULT_PAYLOAD_LIMIT,
) -> AsyncAztmSession:
    """Authenticate without blocking the caller's event loop."""

    connected: AsyncAztmSession | None = None
    failure: BaseException | None = None
    secret_box: list[str] = []
    try:
        config = _validate_config(
            mesh_id=mesh_id,
            mesh_endpoint=mesh_endpoint,
            username=username,
            password=password,
            enrollment_token=enrollment_token,
            profile_id=profile_id,
            command_timeout_ms=command_timeout_ms,
            rpc_timeout_ms=rpc_timeout_ms,
            queue_limit=queue_limit,
            payload_limit=payload_limit,
        )
        secret_box.append(
            enrollment_token if config.auth_mode == "token" else password or ""
        )
        enrollment_token = None
        password = None

        def setup(cancelled: threading.Event) -> tuple[_LifecycleOwner, _Identity]:
            secret = secret_box.pop()
            try:
                return _connect_sync_internal(
                    config=config,
                    password=secret,
                    personality=_PERSONALITY_NATIVE,
                    core_factory=_default_core_factory,
                    cancel_event=cancelled,
                )
            finally:
                secret = ""

        def cleanup(result: tuple[_LifecycleOwner, _Identity]) -> None:
            _cleanup_setup(result[0])

        owner, identity = await _cancellable_offload(setup, cleanup_result=cleanup)
        connected = AsyncAztmSession(owner, identity, asynchronous=True)
    except BaseException as error:
        failure = error
    enrollment_token = None
    password = None
    secret_box.clear()
    if failure is not None:
        public_failure = _public_exception(failure)
        failure = None
        _raise_public(public_failure)
    assert connected is not None
    return connected


def _http_bridge_authentication_factory(
    *,
    config: _ConnectConfig,
    password: str,
    core_factory: CoreFactory = _default_core_factory,
) -> tuple[_LifecycleOwner, _Identity]:
    """Milestone-7 seam: authenticate only; installs no HTTP hooks."""

    return _connect_sync_internal(
        config=config,
        password=password,
        personality=_PERSONALITY_HTTP_BRIDGE,
        core_factory=core_factory,
    )
