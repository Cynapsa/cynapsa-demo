from __future__ import annotations

import asyncio
import inspect
import json
import os
import re
import threading
import time
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any

import pytest

import cynapsa
from cynapsa.exceptions import NativeError
from cynapsa.native.abi import (
    CYNAPSA_CALLBACK_V1_COMPLETION,
    CYNAPSA_STATUS_V1_ERROR,
)
from cynapsa.native.command import PUBLIC_ERROR_MESSAGES, decode_completion, encode_command
from cynapsa.native.core import NativeCore
from cynapsa import session as session_module

from conftest import FakeLibrary


AUTH = {
    "mesh_endpoint": "Mesh.Example.TEST:05222",
    "username": "input-agent@example.test",
    "password": "credential-do-not-retain",
    "mesh_id": "mesh-one",
    "command_timeout_ms": 500,
    "queue_limit": 8,
    "payload_limit": 1_048_576,
}

ENROLLMENT_TOKEN = (
    "cpsa_e1.01234567-89ab-4def-8123-456789abcdef."
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
)


def _success(command: dict[str, Any], result_type: str, result: dict[str, Any]) -> bytes:
    return json.dumps(
        {
            "abi_version": 1,
            "command_id": command["command_id"],
            "ok": True,
            "result_type": result_type,
            "result": result,
        },
        separators=(",", ":"),
    ).encode()


def _failure(command: dict[str, Any], code: str = "authentication_failed") -> bytes:
    return json.dumps(
        {
            "abi_version": 1,
            "command_id": command["command_id"],
            "ok": False,
            "error": {
                "code": code,
                "message": PUBLIC_ERROR_MESSAGES[code],
                "retryable": False,
                "stage": "auth" if code == "authentication_failed" else "sdk",
                "local_or_remote": "local",
            },
        },
        separators=(",", ":"),
    ).encode()


class CompletionDriver:
    def __init__(self, library: FakeLibrary) -> None:
        self.library = library
        self.stalled: set[str] = set()
        self.overrides: dict[str, bytes | tuple[str, dict[str, Any]]] = {}
        self.commands: list[dict[str, Any]] = []
        self._seen = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> CompletionDriver:
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(2)

    def _document(self, command: dict[str, Any]) -> bytes:
        name = command["command_name"]
        override = self.overrides.get(name)
        if isinstance(override, bytes):
            return override.replace(b"$COMMAND_ID", command["command_id"].encode())
        if override is not None:
            return _success(command, override[0], override[1])
        if name == "core.init":
            return _success(
                command, "core_init", {"sdk_session_id": command["sdk_session_id"]}
            )
        if name == "auth.connect":
            args = command["args"]
            return _success(
                command,
                "auth",
                {
                    "agent_id": "authenticated-agent@example.test",
                    "mesh_id": args["mesh_id"],
                    "agent_instance_id": args["agent_instance_id"],
                    "personality": "native",
                },
            )
        if name == "auth.login":
            args = command["args"]
            return _success(
                command,
                "auth",
                {
                    "agent_id": "authenticated-agent@example.test",
                    "mesh_id": args["mesh_id"],
                    "agent_instance_id": args["agent_instance_id"],
                    "personality": "http_bridge",
                },
            )
        if name in {"auth.token_connect", "auth.installation_connect"}:
            args = command["args"]
            return _success(
                command,
                "auth",
                {
                    "agent_id": "authenticated-agent@example.test",
                    "mesh_id": args["mesh_id"],
                    "agent_instance_id": "installation-uuid",
                    "personality": "native",
                    "profile_id": args.get("profile_id", "default"),
                    "credential_expires_at": "2026-09-12T12:00:00Z",
                    "offline_start_deadline": "2026-09-12T11:55:00Z",
                    "offline_cold_start_target_seconds": 86400,
                    "offline_target_satisfied": True,
                    "policy_revision": 42,
                    "session_expiry_mode": "continue",
                    "preparation_status": "ready",
                },
            )
        if name == "core.status":
            return _success(
                command,
                "status",
                {
                    "lifecycle": "ready",
                    "connectivity": "available",
                    "personality": "native",
                    "agent_id": "authenticated-agent@example.test",
                    "mesh_id": "mesh-one",
                    "mesh_endpoint": "mesh.example.test:5222",
                    "queued_message_count": 0,
                },
            )
        if name == "core.capabilities":
            return _success(
                command,
                "capabilities",
                {
                    "commands": ["core.status", "core.capabilities"],
                    "features": ["native_messaging", "rpc", "bounded_queues"],
                },
            )
        return _success(command, "empty", {})

    def _run(self) -> None:
        while not self._stop.is_set():
            if self._seen >= len(self.library.submit_inputs):
                self._stop.wait(0.001)
                continue
            command = json.loads(self.library.submit_inputs[self._seen])
            self._seen += 1
            self.commands.append(command)
            if command["command_name"] in self.stalled:
                continue
            deadline = time.monotonic() + 1
            while self.library.callback is None and not self._stop.is_set():
                if time.monotonic() >= deadline:
                    return
                self._stop.wait(0.001)
            if not self._stop.is_set():
                self.library.emit_callback(
                    CYNAPSA_CALLBACK_V1_COMPLETION, self._document(command)
                )


@pytest.fixture
def native_factory(monkeypatch: pytest.MonkeyPatch, fake_library: FakeLibrary) -> None:
    monkeypatch.setattr(
        session_module,
        "_default_core_factory",
        lambda config: NativeCore.create(config.core_create(), library=fake_library),
    )


def _connect(driver: CompletionDriver) -> cynapsa.AztmSession:
    driver.start()
    return cynapsa.connect(**AUTH)


def test_public_api_exposes_universal_models() -> None:
    assert {
        "CynapsaApplicationError",
        "CynapsaRequest",
        "CynapsaResponse",
        "RPCException",
        "RemoteApplicationError",
    } <= set(cynapsa.__all__)
    assert not {
        "AsyncAztmRequest",
        "AztmRequest",
        "AztmResponse",
        "HTTPRequestPayload",
        "HTTPResponsePayload",
        "NativePayload",
    } & set(cynapsa.__all__)
    for partial in (
        "on", "NativeCore",
        "CommandFuture", "CommandCompletion",
    ):
        assert not hasattr(cynapsa, partial)


def test_native_auth_pipeline_returns_only_after_valid_success(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library)
    session = _connect(driver)
    try:
        assert isinstance(session, cynapsa.AztmSession)
        assert [item["command_name"] for item in driver.commands[:2]] == [
            "core.init",
            "auth.connect",
        ]
        assert session.username == AUTH["username"]
        assert session.agent_id == "authenticated-agent@example.test"
        assert session.mesh_endpoint == "mesh.example.test:5222"
        assert session.mesh_id == AUTH["mesh_id"]
        assert session.personality == "native"
        assert not hasattr(session, "password")
        assert not hasattr(session, "agent_instance_id")
        assert hasattr(session, "send")
        assert hasattr(session, "request")
        with pytest.raises(AttributeError):
            session.username = "changed"  # type: ignore[misc]
        create = json.loads(fake_library.create_input or b"{}")
        assert set(create) == {
            "abi_version",
            "command_timeout_ms",
            "rpc_timeout_ms",
            "queue_limit",
            "payload_limit",
        }
        assert AUTH["password"] not in repr(session)
        assert AUTH["password"] not in repr(session._owner.core)
    finally:
        session.close()
        driver.stop()
    assert [item["command_name"] for item in driver.commands][-1] == "auth.logout"
    assert fake_library.calls["callbacks_clear"] == 1
    assert fake_library.calls["core_destroy"] == 1


def test_installation_connect_reopens_default_profile_and_retains_auth_metadata(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library).start()
    session = cynapsa.connect(mesh_id="mesh-one")
    try:
        auth = next(
            item
            for item in driver.commands
            if item["command_name"] == "auth.installation_connect"
        )
        assert auth["args"] == {"profile_id": "default", "mesh_id": "mesh-one"}
        assert session.username == ""
        assert session.mesh_endpoint == ""
        assert session.profile_id == "default"
        assert session.installation_id == "installation-uuid"
        assert session.auth_info.profile_id == "default"
        assert session.auth_info.installation_id == "installation-uuid"
        assert session.auth_info.credential_expires_at == "2026-09-12T12:00:00Z"
        assert session.auth_info.offline_start_deadline == "2026-09-12T11:55:00Z"
        assert session.auth_info.offline_cold_start_target_seconds == 86400
        assert session.auth_info.offline_target_satisfied is True
        assert session.auth_info.policy_revision == 42
        assert session.auth_info.session_expiry_mode == "continue"
        assert session.auth_info.preparation_status == "ready"
        with pytest.raises(FrozenInstanceError):
            session.auth_info.profile_id = "changed"  # type: ignore[misc]
    finally:
        session.close()
        driver.stop()


def test_token_connect_uses_selected_profile_without_sdk_instance(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library).start()
    session = cynapsa.connect(
        mesh_id="mesh-one",
        enrollment_token=ENROLLMENT_TOKEN,
        profile_id="replica-a",
    )
    try:
        auth = next(
            item
            for item in driver.commands
            if item["command_name"] == "auth.token_connect"
        )
        assert auth["args"] == {
            "token": ENROLLMENT_TOKEN,
            "mesh_id": "mesh-one",
            "profile_id": "replica-a",
        }
        assert "agent_instance_id" not in auth["args"]
        assert session.profile_id == "replica-a"
        assert session.installation_id == "installation-uuid"
    finally:
        session.close()
        driver.stop()


def test_force_token_connect_sends_flag(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library).start()
    session = cynapsa.connect(
        mesh_id="mesh-one", enrollment_token=ENROLLMENT_TOKEN, force_enroll=True
    )
    try:
        auth = next(item for item in driver.commands if item["command_name"] == "auth.token_connect")
        assert auth["args"]["force_enroll"] is True
        assert not any(item["command_name"] == "auth.installation_connect" for item in driver.commands)
    finally:
        session.close()
        driver.stop()


@pytest.mark.asyncio
async def test_force_token_connect_async_sends_flag(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library).start()
    session = await cynapsa.connect_async(
        mesh_id="mesh-one", enrollment_token=ENROLLMENT_TOKEN, force_enroll=True
    )
    try:
        auth = next(item for item in driver.commands if item["command_name"] == "auth.token_connect")
        assert auth["args"]["force_enroll"] is True
    finally:
        await session.close()
        driver.stop()


@pytest.mark.parametrize("credentials", [{}, AUTH])
def test_force_connect_requires_token_and_rejects_legacy_before_core_create(
    credentials: dict[str, object], fake_library: FakeLibrary, native_factory: None
) -> None:
    with pytest.raises(ValueError, match="force_enroll requires enrollment_token"):
        cynapsa.connect(mesh_id="mesh-one", force_enroll=True, **{k: v for k, v in credentials.items() if k != "mesh_id"})
    assert fake_library.calls["core_create"] == 0


def test_force_connect_rejects_mixed_credentials_before_core_create(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    with pytest.raises(ValueError, match="cannot be combined"):
        cynapsa.connect(**AUTH, enrollment_token=ENROLLMENT_TOKEN, force_enroll=True)
    assert fake_library.calls["core_create"] == 0


def test_legacy_connect_has_empty_v2_auth_metadata(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library)
    session = _connect(driver)
    try:
        assert session.profile_id is None
        assert session.installation_id is None
        assert all(
            value is None
            for value in (
                session.auth_info.profile_id,
                session.auth_info.installation_id,
                session.auth_info.credential_expires_at,
                session.auth_info.offline_start_deadline,
                session.auth_info.offline_cold_start_target_seconds,
                session.auth_info.offline_target_satisfied,
                session.auth_info.policy_revision,
                session.auth_info.session_expiry_mode,
                session.auth_info.preparation_status,
            )
        )
    finally:
        session.close()
        driver.stop()


@pytest.mark.parametrize(
    "credentials",
    [
        {"mesh_endpoint": "mesh.example.test:5222"},
        {"username": "agent@example.test"},
        {"password": "secret"},
        {
            "mesh_endpoint": "mesh.example.test:5222",
            "username": "agent@example.test",
        },
        {
            "mesh_endpoint": "mesh.example.test:5222",
            "password": "secret",
        },
        {"username": "agent@example.test", "password": "secret"},
    ],
)
def test_partial_legacy_credentials_fail_before_core_create(
    credentials: dict[str, str],
    fake_library: FakeLibrary,
    native_factory: None,
) -> None:
    with pytest.raises(ValueError, match="must be supplied together"):
        cynapsa.connect(mesh_id="mesh-one", **credentials)
    assert fake_library.calls["core_create"] == 0


def test_token_and_legacy_credentials_are_mutually_exclusive(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    with pytest.raises(ValueError, match="cannot be combined"):
        cynapsa.connect(**AUTH, enrollment_token=ENROLLMENT_TOKEN)
    assert fake_library.calls["core_create"] == 0


@pytest.mark.parametrize(
    ("kwargs", "secret"),
    [
        ({"enrollment_token": "not-a-token"}, "not-a-token"),
        ({"profile_id": "../escape"}, "../escape"),
        (
            {"enrollment_token": ENROLLMENT_TOKEN, "profile_id": "../escape"},
            ENROLLMENT_TOKEN,
        ),
    ],
)
def test_v2_authentication_validation_happens_before_core_creation(
    kwargs: dict[str, str],
    secret: str,
    fake_library: FakeLibrary,
    native_factory: None,
) -> None:
    with pytest.raises(ValueError) as raised:
        cynapsa.connect(mesh_id="mesh-one", **kwargs)
    assert secret not in str(raised.value)
    assert secret not in repr(raised.value)
    assert fake_library.calls["core_create"] == 0


@pytest.mark.parametrize(
    "endpoint",
    [
        "mesh.example.test",
        "https://mesh.example.test:5222",
        "user@mesh.example.test:5222",
        "mesh.example.test:5222/path",
        "mesh.example.test:5222?query",
        "mesh.example.test:5222#fragment",
        "mésh.example.test:5222",
        "mesh.example.test.:5222",
        "[fe80::1%en0]:5222",
        "fe80::1:5222",
        "[127.0.0.1]:5222",
        "[::ffff:192.0.2.1]:5222",
        "127.0.0.1:0",
        "127.0.0.1:65536",
        "127.0.0.1:+5222",
        " 127.0.0.1:5222",
    ],
)
def test_invalid_endpoint_matrix_fails_before_core_create(
    endpoint: str, fake_library: FakeLibrary, native_factory: None
) -> None:
    with pytest.raises(ValueError):
        cynapsa.connect(**{**AUTH, "mesh_endpoint": endpoint})
    assert fake_library.calls["core_create"] == 0


@pytest.mark.parametrize(
    ("endpoint", "canonical"),
    [
        ("Mesh-01.Example.TEST:05222", "mesh-01.example.test:5222"),
        ("localhost:00080", "localhost:80"),
        ("192.0.2.10:443", "192.0.2.10:443"),
        ("192.168.001.010:05222", "192.168.001.010:5222"),
        ("[2001:0DB8:0:0:0:0:0:1]:05222", "[2001:db8::1]:5222"),
    ],
)
def test_endpoint_canonicalization_matches_go(
    endpoint: str,
    canonical: str,
    fake_library: FakeLibrary,
    native_factory: None,
) -> None:
    driver = CompletionDriver(fake_library).start()
    session = cynapsa.connect(**{**AUTH, "mesh_endpoint": endpoint})
    try:
        auth = next(c for c in driver.commands if c["command_name"] == "auth.connect")
        assert auth["args"]["mesh_endpoint"] == canonical
        assert session.mesh_endpoint == canonical
    finally:
        try:
            session.close()
        except NativeError:
            pass
        driver.stop()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("username", ""),
        ("username", "u" * 257),
        ("username", "agent\n@example.test"),
        ("mesh_id", ""),
        ("mesh_id", "m" * 256),
        ("password", ""),
        ("password", "p" * 8193),
        ("command_timeout_ms", -1),
        ("queue_limit", 0),
        ("queue_limit", 65_537),
        ("payload_limit", 0),
        ("payload_limit", 134_217_697),
    ],
)
def test_invalid_public_config_fails_before_native_load(
    field: str, value: Any, fake_library: FakeLibrary, native_factory: None
) -> None:
    with pytest.raises(ValueError) as raised:
        cynapsa.connect(**{**AUTH, field: value})
    assert AUTH["password"] not in str(raised.value)
    assert fake_library.calls["core_create"] == 0


def test_generated_ids_are_strong_bounded_and_not_user_controlled(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library)
    driver.start()
    session = cynapsa.connect(**AUTH)
    try:
        auth = next(c for c in driver.commands if c["command_name"] == "auth.connect")
        shapes = {
            "agentinst": auth["args"]["agent_instance_id"],
            "sdk": auth["sdk_session_id"],
        }
        for prefix, value in shapes.items():
            assert re.fullmatch(prefix + r"_[A-Za-z0-9_-]{43}", value)
        assert all(
            re.fullmatch(r"cmd_[A-Za-z0-9_-]{43}", c["command_id"])
            for c in driver.commands
        )
        assert len({c["command_id"] for c in driver.commands}) == len(driver.commands)
    finally:
        session.close()
        driver.stop()

    with pytest.raises(TypeError):
        cynapsa.connect(**AUTH, agent_instance_id="caller-controlled")  # type: ignore[call-arg]
    assert "agent_instance_id" not in inspect.signature(cynapsa.connect).parameters
    assert "agent_instance_id" not in inspect.signature(cynapsa.connect_async).parameters


@pytest.mark.parametrize(
    "override",
    [
        (
            "auth",
            {
                "agent_id": "agent",
                "mesh_id": "mesh-one",
                "agent_instance_id": "instance-one",
                "personality": "http_bridge",
            },
        ),
        (
            "auth",
            {
                "agent_id": "agent",
                "mesh_id": "other-mesh",
                "agent_instance_id": "instance-one",
                "personality": "native",
            },
        ),
        (
            "auth",
            {
                "agent_id": "agent",
                "mesh_id": "mesh-one",
                "agent_instance_id": "other-instance",
                "personality": "native",
            },
        ),
    ],
)
def test_auth_result_identity_mismatches_logout_and_cleanup(
    override: tuple[str, dict[str, Any]],
    fake_library: FakeLibrary,
    native_factory: None,
) -> None:
    driver = CompletionDriver(fake_library)
    driver.overrides["auth.connect"] = override
    driver.start()
    with pytest.raises(NativeError) as raised:
        cynapsa.connect(**AUTH)
    driver.stop()
    assert raised.value.code == "authentication_failed"
    assert set(raised.value.details) == {
        "retryable", "stage", "local_or_remote"
    }
    assert "auth.logout" in [item["command_name"] for item in driver.commands]
    assert fake_library.calls["core_destroy"] == 1


@pytest.mark.parametrize(
    "result",
    [
        {
            "agent_id": "agent",
            "mesh_id": "mesh-one",
            "agent_instance_id": "",
            "personality": "native",
            "profile_id": "replica-a",
        },
        {
            "agent_id": "agent",
            "mesh_id": "mesh-one",
            "agent_instance_id": "installation-uuid",
            "personality": "native",
            "profile_id": "other-profile",
        },
    ],
)
def test_v2_auth_result_identity_mismatches_logout_and_cleanup(
    result: dict[str, Any],
    fake_library: FakeLibrary,
    native_factory: None,
) -> None:
    driver = CompletionDriver(fake_library)
    driver.overrides["auth.installation_connect"] = ("auth", result)
    driver.start()
    with pytest.raises(NativeError) as raised:
        cynapsa.connect(mesh_id="mesh-one", profile_id="replica-a")
    driver.stop()
    assert raised.value.code == "authentication_failed"
    assert "auth.logout" in [item["command_name"] for item in driver.commands]
    assert fake_library.calls["core_destroy"] == 1


def test_v2_auth_result_with_absent_optional_metadata_uses_none_defaults(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library)
    driver.overrides["auth.installation_connect"] = (
        "auth",
        {
            "agent_id": "agent",
            "mesh_id": "mesh-one",
            "agent_instance_id": "installation-uuid",
            "personality": "native",
            "profile_id": "default",
        },
    )
    driver.start()
    session = cynapsa.connect(mesh_id="mesh-one")
    try:
        assert session.profile_id == "default"
        assert session.installation_id == "installation-uuid"
        assert session.auth_info.credential_expires_at is None
        assert session.auth_info.offline_start_deadline is None
        assert session.auth_info.policy_revision is None
    finally:
        session.close()
        driver.stop()


def test_auth_completion_failure_is_secret_free_and_cleans_everything(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library)
    marker = b'"ok":false'
    driver.overrides["auth.connect"] = (
        b'{"abi_version":1,"command_id":"$COMMAND_ID",' + marker
        + b',"error":{"code":"authentication_failed","message":"Authentication failed",'
        b'"retryable":false,"stage":"auth","local_or_remote":"local"}}'
    )
    driver.start()
    with pytest.raises(NativeError) as raised:
        cynapsa.connect(**AUTH)
    driver.stop()
    assert raised.value.code == "authentication_failed"
    assert AUTH["password"] not in str(raised.value)
    assert AUTH["password"] not in repr(raised.value)
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    traceback = raised.value.__traceback__
    while traceback is not None:
        if "/src/cynapsa/" in traceback.tb_frame.f_code.co_filename:
            assert AUTH["password"] not in repr(dict(traceback.tb_frame.f_locals))
        traceback = traceback.tb_next
    assert fake_library.calls["core_destroy"] == 1
    assert "auth.logout" not in [item["command_name"] for item in driver.commands]


@pytest.mark.parametrize("phase", ["core_start", "callbacks_register"])
def test_partial_setup_native_phase_failures_cleanup(
    phase: str, fake_library: FakeLibrary, native_factory: None
) -> None:
    fake_library.queue(phase, CYNAPSA_STATUS_V1_ERROR, {"code": "core_error"})
    with pytest.raises(NativeError):
        cynapsa.connect(**AUTH)
    assert fake_library.calls["core_shutdown"] == 1
    assert fake_library.calls["core_destroy"] == 1
    assert fake_library.callback is None


def test_core_init_timeout_cancels_and_cleans(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library)
    driver.stalled.add("core.init")
    driver.start()
    with pytest.raises(TimeoutError):
        cynapsa.connect(**{**AUTH, "command_timeout_ms": 20})
    driver.stop()
    assert fake_library.calls["core_cancel"] == 1
    assert fake_library.calls["core_destroy"] == 1


def test_callback_fatal_during_setup_is_reported_and_cleaned(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library)
    driver.overrides["core.init"] = b"not-json-$COMMAND_ID"
    driver.start()
    with pytest.raises(NativeError) as raised:
        cynapsa.connect(**AUTH)
    driver.stop()
    assert raised.value.code == "core_error"
    assert "callback" not in str(raised.value).lower()
    assert set(raised.value.details) == {
        "retryable", "stage", "local_or_remote"
    }
    assert fake_library.calls["core_destroy"] == 1


def test_status_and_capabilities_are_strict_immutable_command_results(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library)
    session = _connect(driver)
    try:
        status = session.status()
        capabilities = session.capabilities()
        assert status.lifecycle == "ready"
        assert status.connectivity == "available"
        assert capabilities.commands == ("core.status", "core.capabilities")
        assert capabilities.features == ("native_messaging", "rpc", "bounded_queues")
        with pytest.raises(FrozenInstanceError):
            status.lifecycle = "failed"  # type: ignore[misc]
        with pytest.raises(FrozenInstanceError):
            capabilities.features = ()  # type: ignore[misc]
    finally:
        session.close()
        driver.stop()
    assert [c["command_name"] for c in driver.commands][2:4] == [
        "core.status",
        "core.capabilities",
    ]


def test_high_level_capabilities_hide_deferred_features(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library)
    driver.overrides["core.capabilities"] = (
        "capabilities",
        {
            "commands": ["core.capabilities", "payload.open"],
            "features": [
                "native_messaging",
                "large_payloads",
                "payload_streaming_handles",
            ],
        },
    )
    session = _connect(driver)
    try:
        capabilities = session.capabilities()
        assert capabilities.features == ("native_messaging",)
        assert "payload.open" in capabilities.commands
        assert not hasattr(session, "payload_open")
    finally:
        session.close()
        driver.stop()


def test_status_identity_mismatch_fails_closed_at_mapping_boundary(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library)
    driver.overrides["core.status"] = (
        "status",
        {
            "lifecycle": "ready",
            "connectivity": "available",
            "personality": "native",
            "agent_id": "wrong-agent",
            "mesh_id": "mesh-one",
            "mesh_endpoint": "mesh.example.test:5222",
            "queued_message_count": 0,
        },
    )
    session = _connect(driver)
    try:
        with pytest.raises(NativeError) as raised:
            session.status()
        assert raised.value.code == "core_error"
        assert "connectivity implementation" not in str(raised.value)
    finally:
        session.close()
        driver.stop()


@pytest.mark.parametrize(
    ("command_name", "result_type", "result"),
    [
        (
            "core.status",
            "status",
            {
                "lifecycle": "future-private-state",
                "connectivity": "available",
                "personality": "native",
                "agent_id": "authenticated-agent@example.test",
                "mesh_id": "mesh-one",
                "mesh_endpoint": "mesh.example.test:5222",
                "queued_message_count": 0,
            },
        ),
        (
            "core.capabilities",
            "capabilities",
            {"commands": ["core.status"], "features": ["future-private-feature"]},
        ),
    ],
)
def test_unknown_status_and_capability_values_map_to_generic_core_error(
    command_name: str,
    result_type: str,
    result: dict[str, Any],
    fake_library: FakeLibrary,
    native_factory: None,
) -> None:
    driver = CompletionDriver(fake_library)
    driver.overrides[command_name] = (result_type, result)
    session = _connect(driver)
    try:
        method = session.status if command_name == "core.status" else session.capabilities
        with pytest.raises(NativeError) as raised:
            method()
        assert raised.value.code == "core_error"
        assert set(raised.value.details) == {
            "retryable", "stage", "local_or_remote"
        }
    finally:
        try:
            session.close()
        except NativeError:
            pass
        driver.stop()


def test_double_and_concurrent_close_submit_one_logout(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library)
    session = _connect(driver)
    errors: list[BaseException] = []
    barrier = threading.Barrier(3)

    def closer() -> None:
        barrier.wait()
        try:
            session.close()
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=closer) for _ in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(2)
    session.close()
    driver.stop()
    assert not errors
    assert [c["command_name"] for c in driver.commands].count("auth.logout") == 1
    assert fake_library.calls["core_shutdown"] == 1
    assert fake_library.calls["core_destroy"] == 1


def test_sync_context_manager_closes_session(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library).start()
    with cynapsa.connect(**AUTH) as session:
        assert session.personality == "native"
    driver.stop()
    assert fake_library.calls["core_destroy"] == 1


def test_context_body_exception_is_not_masked_by_close_failure(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library).start()
    fake_library.queue(
        "core_shutdown", CYNAPSA_STATUS_V1_ERROR, {"code": "shutdown_timeout"}
    )
    with pytest.raises(RuntimeError, match="body failure"):
        with cynapsa.connect(**AUTH) as session:
            raise RuntimeError("body failure")
    driver.stop()
    session.close()


def test_logout_failure_still_cleans_and_surfaces_normalized_error(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library)
    driver.overrides["auth.logout"] = _failure(
        {"command_id": "$COMMAND_ID"}, "authentication_failed"
    )
    session = _connect(driver)
    with pytest.raises(NativeError) as raised:
        session.close()
    driver.stop()
    assert raised.value.code == "authentication_failed"
    assert fake_library.calls["core_destroy"] == 1
    assert [c["command_name"] for c in driver.commands].count("auth.logout") == 1
    session.close()


def test_method_after_close_fails_with_public_lifecycle_error(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library)
    session = _connect(driver)
    session.close()
    driver.stop()
    with pytest.raises(NativeError) as raised:
        session.status()
    assert raised.value.code == "shutdown_in_progress"
    session.close()


def test_shutdown_timeout_retains_recoverable_owner(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library)
    driver.overrides["auth.connect"] = _failure(
        {"command_id": "$COMMAND_ID"}, "authentication_failed"
    )
    fake_library.queue(
        "core_shutdown", CYNAPSA_STATUS_V1_ERROR, {"code": "shutdown_timeout"}
    )
    driver.start()
    with pytest.raises(NativeError):
        cynapsa.connect(**AUTH)
    driver.stop()
    with session_module._RETAINED_OWNERS_LOCK:
        retained = tuple(session_module._RETAINED_OWNERS)
    assert retained
    owner = retained[-1]
    owner.close()
    session_module._release_owner(owner)
    assert fake_library.calls["core_destroy"] == 1


def test_auth_command_and_result_match_vendored_conformance_vectors() -> None:
    vectors = Path(__file__).parents[1] / "src/cynapsa/_vendor/conformance/v1"
    commands = json.loads((vectors / "commands.json").read_text())
    command = next(item["json"] for item in commands if item["name"] == "auth.connect")
    _, encoded = encode_command(
        command["command_name"],
        command["sdk_session_id"],
        command["args"],
        command_id=command["command_id"],
    )
    assert json.loads(encoded) == command
    results = json.loads((vectors / "results.json").read_text())
    result = next(item["json"] for item in results if item["name"] == "auth")
    completion = decode_completion(json.dumps(result, separators=(",", ":")).encode())
    assert completion.result_type == "auth"
    assert completion.result is not None
    assert completion.result["personality"] == "native"


@pytest.mark.asyncio
async def test_async_connect_status_context_and_close_do_not_block_loop(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library).start()
    ticks = 0
    running = True

    async def ticker() -> None:
        nonlocal ticks
        while running:
            ticks += 1
            await asyncio.sleep(0)

    ticker_task = asyncio.create_task(ticker())
    session = await cynapsa.connect_async(**AUTH)
    async with session:
        assert isinstance(session, cynapsa.AsyncAztmSession)
        assert (await session.status()).lifecycle == "ready"
        assert "rpc" in (await session.capabilities()).features
    running = False
    await ticker_task
    driver.stop()
    assert ticks > 5
    assert fake_library.calls["core_destroy"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kwargs", "command_name", "profile_id"),
    [
        ({}, "auth.installation_connect", "default"),
        (
            {
                "enrollment_token": ENROLLMENT_TOKEN,
                "profile_id": "async-replica",
            },
            "auth.token_connect",
            "async-replica",
        ),
    ],
)
async def test_async_v2_authentication_modes(
    kwargs: dict[str, str],
    command_name: str,
    profile_id: str,
    fake_library: FakeLibrary,
    native_factory: None,
) -> None:
    driver = CompletionDriver(fake_library).start()
    session = await cynapsa.connect_async(mesh_id="mesh-one", **kwargs)
    try:
        assert any(
            command["command_name"] == command_name for command in driver.commands
        )
        assert session.profile_id == profile_id
        assert session.installation_id == "installation-uuid"
        assert session.username == ""
        assert session.mesh_endpoint == ""
    finally:
        await session.close()
        driver.stop()


@pytest.mark.asyncio
async def test_async_setup_cancellation_cancels_command_and_cleans(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library)
    driver.stalled.add("core.init")
    driver.start()
    task = asyncio.create_task(cynapsa.connect_async(**AUTH))
    deadline = time.monotonic() + 1
    while not fake_library.submit_inputs:
        assert time.monotonic() < deadline
        await asyncio.sleep(0.001)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    driver.stop()
    assert fake_library.calls["core_cancel"] == 1
    assert fake_library.calls["core_destroy"] == 1


@pytest.mark.asyncio
async def test_async_status_cancellation_uses_native_command_cancel(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library)
    session = None
    driver.start()
    session = await cynapsa.connect_async(**AUTH)
    driver.stalled.add("core.status")
    task = asyncio.create_task(session.status())
    deadline = time.monotonic() + 1
    while len(driver.commands) < 3:
        assert time.monotonic() < deadline
        await asyncio.sleep(0.001)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await session.close()
    driver.stop()
    assert fake_library.calls["core_cancel"] >= 1


@pytest.mark.asyncio
async def test_async_close_cancellation_finishes_cleanup_then_propagates(
    fake_library: FakeLibrary, native_factory: None
) -> None:
    driver = CompletionDriver(fake_library)
    session = None
    driver.start()
    session = await cynapsa.connect_async(**AUTH)
    driver.stalled.add("auth.logout")
    task = asyncio.create_task(session.close())
    deadline = time.monotonic() + 1
    while not any(c["command_name"] == "auth.logout" for c in driver.commands):
        assert time.monotonic() < deadline
        await asyncio.sleep(0.001)
    logout = next(c for c in driver.commands if c["command_name"] == "auth.logout")
    task.cancel()
    fake_library.emit_callback(
        CYNAPSA_CALLBACK_V1_COMPLETION, _success(logout, "empty", {})
    )
    with pytest.raises(asyncio.CancelledError):
        await task
    driver.stop()
    assert fake_library.calls["core_destroy"] == 1


@pytest.mark.skipif(
    not os.environ.get("CYNAPSA_CORE_SMOKE_LIBRARY"),
    reason="set CYNAPSA_CORE_SMOKE_LIBRARY to run the real native auth smoke",
)
def test_repeated_real_providerless_auth_failure_cleans_safely(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = os.environ["CYNAPSA_CORE_SMOKE_LIBRARY"]
    monkeypatch.setenv("CYNAPSA_CORE_LIBRARY", path)
    for _ in range(12):
        with pytest.raises(NativeError) as raised:
            cynapsa.connect(
                mesh_endpoint="mesh.example.test:5222",
                username="agent@example.test",
                password="not-a-production-secret",
                mesh_id="mesh-one",
                command_timeout_ms=5_000,
                queue_limit=16,
                payload_limit=1_048_576,
            )
        assert raised.value.code in {
            "authentication_failed", "connectivity_unavailable", "core_error"
        }
        assert set(raised.value.details) <= {
            "retryable", "stage", "local_or_remote", "diagnostic_id"
        }
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None
    assert not any(
        thread.name == "cynapsa-callback-dispatcher"
        for thread in threading.enumerate()
    )
    with session_module._RETAINED_OWNERS_LOCK:
        assert not session_module._RETAINED_OWNERS
