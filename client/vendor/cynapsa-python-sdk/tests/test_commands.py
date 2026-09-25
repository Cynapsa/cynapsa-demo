from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path
from typing import Any

import pytest

from cynapsa.exceptions import NativeError
from cynapsa.native.abi import (
    CYNAPSA_STATUS_V1_ERROR,
    CYNAPSA_STATUS_V1_OK,
    CYNAPSA_STATUS_V1_WAIT_TIMEOUT,
)
from cynapsa.native.command import (
    PUBLIC_ERROR_MESSAGES,
    CommandFuture,
    decode_completion,
    encode_command,
)
from cynapsa.native.core import NativeCore

from conftest import FakeLibrary, VALID_COMMAND_HANDLE

VECTORS = Path(__file__).parents[1] / "src/cynapsa/_vendor/conformance/v1"
VALID_REQUEST_HANDLE = "reqh_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"


def _error(code: str = "core_error", message: str | None = None) -> dict[str, Any]:
    return {
        "code": code,
        "message": PUBLIC_ERROR_MESSAGES[code] if message is None else message,
        "retryable": False,
        "stage": "command",
        "local_or_remote": "local",
    }


def _admission(command_id: str, *, accepted: bool = True) -> dict[str, Any]:
    if accepted:
        return {
            "abi_version": 1,
            "command_id": command_id,
            "command_handle": VALID_COMMAND_HANDLE,
            "accepted": True,
        }
    return {
        "abi_version": 1,
        "command_id": command_id,
        "accepted": False,
        "error": _error("queue_full", "The local queue is full"),
    }


def _success(command_id: str, result_type: str = "empty", result: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "abi_version": 1,
        "command_id": command_id,
        "ok": True,
        "result_type": result_type,
        "result": {} if result is None else result,
    }


def _failed(command_id: str) -> dict[str, Any]:
    return {
        "abi_version": 1,
        "command_id": command_id,
        "ok": False,
        "error": _error("connectivity_unavailable", "Connectivity is unavailable"),
    }


def _application_error_response(details_json: str) -> dict[str, Any]:
    return {
        "status_code": 400,
        "reason": "Bad Request",
        "headers": [],
        "body": "",
        "error": {
            "code": "bad_request",
            "detail": "Invalid request",
            "details_json": details_json,
        },
    }


def _response_result(details_json: str) -> dict[str, Any]:
    return {
        "message_id": "response-one",
        "conversation_id": "conversation-one",
        "from_agent_id": "agent-b@example.test",
        "mesh_id": "mesh-one",
        "payload": {
            "http_response": _application_error_response(details_json),
        },
    }


def _core(fake_library: FakeLibrary, core_config: dict[str, int]) -> NativeCore:
    core = NativeCore.create(core_config, library=fake_library)
    core.start()
    return core


def test_all_command_vectors_encode_with_exact_top_level_and_deterministic_bytes() -> None:
    vectors = json.loads((VECTORS / "commands.json").read_text())
    for vector in vectors:
        wire = vector["json"]
        command_id, encoded = encode_command(
            wire["command_name"],
            wire["sdk_session_id"],
            wire["args"],
            command_id=wire["command_id"],
        )
        assert command_id == wire["command_id"]
        assert list(json.loads(encoded)) == [
            "abi_version", "command_id", "command_name", "sdk_session_id", "args"
        ]
        assert encoded == json.dumps(
            wire, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        ).encode()


def test_command_args_are_canonical_independent_of_mapping_insertion_order() -> None:
    vectors = json.loads((VECTORS / "commands.json").read_text())
    wire = next(
        vector["json"]
        for vector in vectors
        if vector["json"]["command_name"] == "auth.login"
    )
    reversed_args = dict(reversed(tuple(wire["args"].items())))

    _, encoded = encode_command(
        wire["command_name"],
        wire["sdk_session_id"],
        reversed_args,
        command_id=wire["command_id"],
    )

    assert encoded == json.dumps(
        wire, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode()


def test_command_validation_rejects_exact_shape_errors_without_secret_leakage() -> None:
    secret = "credential-do-not-disclose"
    cases = (
        ("core.init", {}, ""),
        ("core.init", {"unknown": secret}, "command"),
        (
            "auth.login",
            {
                "mesh_endpoint": "mesh.example.test:5222",
                "username": "agent@example.test",
                "password": secret * 1000,
                "mesh_id": "mesh",
                "agent_instance_id": "instance",
            },
            "command",
        ),
    )
    for name, args, command_id in cases:
        with pytest.raises(NativeError) as raised:
            encode_command(name, "session", args, command_id=command_id)
        assert raised.value.code == "invalid_input"
        assert secret not in str(raised.value)
        assert secret not in repr(raised.value)


@pytest.mark.parametrize("name", ["auth.token_login", "auth.token_connect"])
def test_token_auth_vectors_are_strict_and_do_not_leak_secrets(name: str) -> None:
    token = (
        "cpsa_e1.01234567-89ab-4def-8123-456789abcdef."
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    )
    command_id, encoded = encode_command(
        name,
        "session-1",
        {"token": token, "mesh_id": "mesh-one"},
        command_id="command-1",
    )
    assert command_id == "command-1"
    assert json.loads(encoded)["args"] == {"token": token, "mesh_id": "mesh-one"}

    _, forced = encode_command(
        name,
        "session-1",
        {"token": token, "mesh_id": "mesh-one", "force_enroll": True},
        command_id="command-2",
    )
    assert json.loads(forced)["args"] == {
        "token": token, "mesh_id": "mesh-one", "force_enroll": True
    }
    for invalid_force in (None, 1, "true"):
        with pytest.raises(NativeError) as raised:
            encode_command(
                name, "session-1",
                {"token": token, "mesh_id": "mesh-one", "force_enroll": invalid_force},
            )
        assert raised.value.code == "invalid_input"

    for invalid in (token.removeprefix("cpsa_"), token + "x", token.upper()):
        with pytest.raises(NativeError) as raised:
            encode_command(
                name,
                "session-1",
                {"token": invalid, "mesh_id": "mesh-one"},
                command_id="command-1",
            )
        assert raised.value.code == "invalid_input"
        assert invalid not in str(raised.value)
        assert invalid not in repr(raised.value)


@pytest.mark.parametrize(
    "name", ["auth.installation_login", "auth.installation_connect"]
)
def test_installation_auth_vectors_validate_profile_and_mesh(name: str) -> None:
    _, encoded = encode_command(
        name,
        "session-1",
        {"profile_id": "default", "mesh_id": "mesh-one"},
        command_id="command-1",
    )
    assert json.loads(encoded)["args"] == {
        "profile_id": "default",
        "mesh_id": "mesh-one",
    }
    with pytest.raises(NativeError) as raised:
        encode_command(
            name,
            "session-1",
            {"profile_id": "../escape", "mesh_id": "mesh-one"},
            command_id="command-1",
        )
    assert raised.value.code == "invalid_input"


def test_extended_token_auth_result_decodes_strictly() -> None:
    result = {
        "agent_id": "01234567-89ab-4def-8123-456789abcdef",
        "mesh_id": "mesh-one",
        "agent_instance_id": "01234567-89ab-4def-8123-456789abcdef",
        "personality": "native",
        "profile_id": "default",
        "credential_expires_at": "2026-09-12T12:00:00Z",
        "offline_start_deadline": "2026-09-12T11:55:00Z",
        "offline_cold_start_target_seconds": 300,
        "offline_target_satisfied": True,
        "policy_revision": 7,
        "session_expiry_mode": "continue",
        "preparation_status": "ready",
    }
    completion = decode_completion(
        json.dumps(_success("auth-command", "auth", result), separators=(",", ":")).encode()
    )
    assert completion.result is not None
    assert completion.result["profile_id"] == "default"
    assert completion.result["policy_revision"] == 7

    with pytest.raises(NativeError) as raised:
        decode_completion(
            json.dumps(
                _success("auth-command", "auth", {**result, "unknown": True}),
                separators=(",", ":"),
            ).encode()
        )
    assert raised.value.code == "native_completion_decode_failed"


def test_invalid_input_is_a_normalized_public_error_code() -> None:
    assert PUBLIC_ERROR_MESSAGES["invalid_input"] == "The command input is invalid"


def _native_payload() -> dict[str, Any]:
    return {"native": {"content_type": "", "path": "/", "body": ""}}


def _agent_id_command_cases(
    agent_id: object,
) -> tuple[tuple[str, dict[str, Any]], ...]:
    legacy_auth = {
        "mesh_endpoint": "mesh.example.test:5222",
        "username": agent_id,
        "password": "password",
        "mesh_id": "mesh-one",
        "agent_instance_id": "instance-one",
    }
    return (
        ("auth.login", legacy_auth),
        ("auth.connect", legacy_auth),
        (
            "address.map.put",
            {"virtual_origin": "https://agent.example", "recipient": agent_id},
        ),
        ("message.send", {"to": agent_id, "payload": _native_payload()}),
        (
            "message.request",
            {"to": agent_id, "payload": _native_payload(), "ttl_ms": 0},
        ),
        (
            "policy.test",
            {"input": {"to": agent_id, "payload": _native_payload()}},
        ),
        ("diagnostics.peer_status", {"peer": agent_id}),
        (
            "policy.set",
            {"rules": [{"action": "allow", "path": "", "agent_id": agent_id}]},
        ),
    )


@pytest.mark.parametrize(
    ("label", "agent_id"),
    [
        ("empty", ""),
        ("overlong ASCII", "a" * 257),
        ("overlong UTF-8", "é" * 129),
        ("invalid UTF-8 bytes", b"peer\xffidentity"),
        ("surrogate", "peer\ud800identity"),
        ("NUL", "peer\x00identity"),
        ("ASCII control", "peer\nidentity"),
        ("Unicode control", "peer\u0085identity"),
    ],
)
def test_agent_id_command_matrix_rejects_invalid_values(
    label: str, agent_id: object
) -> None:
    command_cases = _agent_id_command_cases(agent_id)
    if agent_id == "":
        command_cases = tuple(
            case for case in command_cases if case[0] != "policy.set"
        )
    for command_name, args in command_cases:
        with pytest.raises(NativeError) as raised:
            encode_command(
                command_name,
                "session-one",
                args,
                command_id="command-one",
            )
        assert raised.value.code == "invalid_input", (label, command_name)


@pytest.mark.parametrize("agent_id", ["a" * 256, "é" * 128])
def test_agent_id_exact_256_byte_boundary_is_accepted(agent_id: str) -> None:
    for command_name, args in _agent_id_command_cases(agent_id):
        _, encoded = encode_command(
            command_name,
            "session-one",
            args,
            command_id="command-one",
        )
        assert json.loads(encoded)["command_name"] == command_name


def _agent_id_result_cases(agent_id: str) -> tuple[tuple[str, dict[str, Any]], ...]:
    event_base = {
        "abi_version": 1,
        "event_id": "event-one",
        "created_at": "2026-09-12T12:00:00Z",
    }
    return (
        (
            "auth",
            {
                "agent_id": agent_id,
                "mesh_id": "mesh-one",
                "agent_instance_id": "instance-one",
                "personality": "native",
            },
        ),
        ("agent_id", {"agent_id": agent_id}),
        (
            "status",
            {
                "lifecycle": "ready",
                "connectivity": "available",
                "personality": "native",
                "agent_id": agent_id,
                "mesh_id": "mesh-one",
                "mesh_endpoint": "mesh.example.test:5222",
                "queued_message_count": 0,
            },
        ),
        (
            "address_mappings",
            {
                "mappings": [
                    {
                        "virtual_origin": "https://agent.example",
                        "recipient": agent_id,
                    }
                ]
            },
        ),
        (
            "address_resolution",
            {"recipient": agent_id, "path": "/", "query": ""},
        ),
        (
            "response",
            {
                "message_id": "message-one",
                "conversation_id": "conversation-one",
                "from_agent_id": agent_id,
                "mesh_id": "mesh-one",
                "payload": _native_payload(),
            },
        ),
        (
            "event",
            {
                **event_base,
                "event_name": "peer.reachable",
                "payload": {"peer": agent_id, "reachable": True},
            },
        ),
        (
            "event",
            {
                **event_base,
                "event_name": "message.received",
                "payload": {
                    "message_id": "message-one",
                    "conversation_id": "conversation-one",
                    "from_agent_id": agent_id,
                    "mesh_id": "mesh-one",
                    "mode": "msg",
                    "request_handle": "",
                    "payload": _native_payload(),
                },
            },
        ),
        (
            "conversation_status",
            {
                "conversation_id": "conversation-one",
                "mesh_id": "mesh-one",
                "peer": agent_id,
                "delivery_state": "ready",
                "queued_message_count": 0,
                "blocked": False,
            },
        ),
        (
            "policy",
            {
                "rules": [
                    {"action": "allow", "path": "", "agent_id": agent_id}
                ],
                "allowed": True,
            },
        ),
        (
            "peer_status",
            {
                "peer": agent_id,
                "connectivity": "available",
                "reachable": True,
                "recovery_in_progress": False,
            },
        ),
    )


@pytest.mark.parametrize(
    "agent_id",
    ["a" * 257, "peer\x00identity", "peer\nidentity", "peer\u0085identity"],
)
def test_agent_id_results_reject_nonconforming_values(agent_id: str) -> None:
    for index, (result_type, result) in enumerate(_agent_id_result_cases(agent_id)):
        with pytest.raises(NativeError) as raised:
            decode_completion(
                json.dumps(
                    _success(f"agent-result-{index}", result_type, result),
                    separators=(",", ":"),
                ).encode()
            )
        assert raised.value.code == "native_completion_decode_failed"


@pytest.mark.parametrize("agent_id", ["a" * 256, "é" * 128])
def test_agent_id_results_accept_exact_256_byte_boundary(agent_id: str) -> None:
    for index, (result_type, result) in enumerate(_agent_id_result_cases(agent_id)):
        completion = decode_completion(
            json.dumps(
                _success(f"agent-result-{index}", result_type, result),
                separators=(",", ":"),
            ).encode()
        )
        assert completion.ok is True


def test_empty_policy_agent_selector_remains_a_wildcard() -> None:
    _, encoded = encode_command(
        "policy.set",
        "session-one",
        {"rules": [{"action": "allow", "path": "", "agent_id": ""}]},
        command_id="command-one",
    )
    assert json.loads(encoded)["args"]["rules"][0]["agent_id"] == ""


def test_non_agent_identifiers_keep_the_generic_512_byte_boundary() -> None:
    generic_identifier = "i" * 512
    _, encoded = encode_command(
        "delivery.accept",
        "session-one",
        {"event_id": generic_identifier},
        command_id="command-one",
    )
    assert json.loads(encoded)["args"]["event_id"] == generic_identifier

    _, encoded = encode_command(
        "auth.connect",
        "session-one",
        {
            "mesh_endpoint": "mesh.example.test:5222",
            "username": "agent-one",
            "password": "password",
            "mesh_id": "mesh-one",
            "agent_instance_id": generic_identifier,
        },
        command_id="command-two",
    )
    assert json.loads(encoded)["args"]["agent_instance_id"] == generic_identifier


@pytest.mark.parametrize(
    "url",
    [
        "https://agent.example/bad%ZZescape",
        "https://agent.example/" + "界" * 228,
    ],
)
def test_address_url_uses_go_escaped_path_validation(url: str) -> None:
    with pytest.raises(NativeError) as raised:
        encode_command(
            "address.resolve",
            "session",
            {"url": url},
            command_id="address",
        )
    assert raised.value.code == "invalid_input"


def test_all_completion_vectors_strictly_decode() -> None:
    vectors = json.loads((VECTORS / "results.json").read_text())
    for vector in vectors:
        completion = decode_completion(
            json.dumps(vector["json"], separators=(",", ":")).encode()
        )
        assert completion.command_id == vector["json"]["command_id"]
        assert completion.ok is True
        assert completion.result_type == vector["json"]["result_type"]

    failed = json.loads((VECTORS / "operations.json").read_text())["failed_completion"]
    completion = decode_completion(json.dumps(failed, separators=(",", ":")).encode())
    assert completion.ok is False
    assert completion.error is not None
    assert completion.error.code == "connectivity_unavailable"


@pytest.mark.parametrize(
    "details_json",
    [
        r'{"value":"\ud800"}',
        r'{"value":"\udc00"}',
        r'{"value":[{"nested":"\ud800A"}]}',
        r'{"\ud800":true}',
        r'{"\ud83d\ude00":1,"😀":2}',
    ],
)
def test_application_error_rejects_unpaired_surrogates_recursively(
    details_json: str,
) -> None:
    completion = _success(
        "request-one",
        "response",
        _response_result(details_json),
    )
    with pytest.raises(NativeError) as raised:
        decode_completion(json.dumps(completion, separators=(",", ":")).encode())
    assert raised.value.code == "native_completion_decode_failed"

    with pytest.raises(NativeError) as raised:
        encode_command(
            "message.reply",
            "session-one",
            {
                "request_handle": VALID_REQUEST_HANDLE,
                "payload": {
                    "http_response": _application_error_response(details_json)
                },
            },
            command_id="reply-one",
        )
    assert raised.value.code == "invalid_input"


@pytest.mark.parametrize(
    "details_json",
    [
        r'{"value":"\ud83d\ude00"}',
        '{"value":"😀"}',
        r'{"\ud83d\ude00":"valid key"}',
    ],
)
def test_application_error_accepts_unicode_scalars_and_surrogate_pairs(
    details_json: str,
) -> None:
    completion = decode_completion(
        json.dumps(
            _success("request-one", "response", _response_result(details_json)),
            separators=(",", ":"),
        ).encode()
    )
    assert completion.ok is True

    _, encoded = encode_command(
        "message.reply",
        "session-one",
        {
            "request_handle": VALID_REQUEST_HANDLE,
            "payload": {
                "http_response": _application_error_response(details_json)
            },
        },
        command_id="reply-one",
    )
    wire = json.loads(encoded)
    assert wire["args"]["payload"]["http_response"]["error"]["details_json"] == details_json


@pytest.mark.parametrize(
    "document",
    [
        {
            "abi_version": 1,
            "command_id": "command",
            "ok": False,
            "error": _error("core_error", "unowned message"),
        },
        {
            "abi_version": 1,
            "command_id": "command",
            "ok": True,
            "result_type": "payload_handle",
            "result": {"handle": "payh_short", "size": 0, "eof": True, "chunk": None},
        },
        {
            "abi_version": 1,
            "command_id": "command",
            "ok": True,
            "result_type": "event",
            "result": {
                "abi_version": 1,
                "event_id": "event",
                "event_name": "peer.reachable",
                "created_at": "1970-01-01T00:00:01Z",
                "payload": {"peer": "peer", "reachable": False},
            },
        },
    ],
)
def test_completion_shapes_handles_events_and_errors_are_strict(
    document: dict[str, Any],
) -> None:
    with pytest.raises(NativeError) as raised:
        decode_completion(json.dumps(document, separators=(",", ":")).encode())
    assert raised.value.code == "native_completion_decode_failed"


def test_embedded_event_timestamp_rejects_noncanonical_fraction() -> None:
    document = {
        "abi_version": 1,
        "command_id": "command",
        "ok": True,
        "result_type": "event",
        "result": {
            "abi_version": 1,
            "event_id": "event",
            "event_name": "peer.reachable",
            "created_at": "1970-01-01T00:00:01.10Z",
            "payload": {"peer": "peer", "reachable": True},
        },
    }
    with pytest.raises(NativeError) as raised:
        decode_completion(json.dumps(document, separators=(",", ":")).encode())
    assert raised.value.code == "native_completion_decode_failed"


@pytest.mark.parametrize(
    "name,session,args,command_id",
    [
        ("not.a.command", "session", {}, "command"),
        ("core.init", "", {}, "command"),
        ("core.init", "session", [], "command"),
        ("core.init", "session", {"value": object()}, "command"),
        ("core.init", "session", {"value": 1.5}, "command"),
        ("core.init", "session", {}, "x" * 513),
    ],
)
def test_command_validation_fails_before_native_call(
    fake_library: FakeLibrary,
    core_config: dict[str, int],
    name: str,
    session: str,
    args: Any,
    command_id: str,
) -> None:
    core = _core(fake_library, core_config)
    with pytest.raises(NativeError) as raised:
        core.submit(name, session, args, command_id=command_id)
    assert raised.value.code == "invalid_input"
    assert fake_library.calls["core_submit"] == 0
    core.close()


def test_submission_distinguishes_abi_failure_rejection_and_acceptance(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    fake_library.queue_submit(
        CYNAPSA_STATUS_V1_ERROR,
        error={"code": "core_error", "message": "ABI failed"},
    )
    with pytest.raises(NativeError) as abi_failure:
        core.submit("core.init", "session", command_id="abi")
    assert abi_failure.value.details["failure_kind"] == "abi_call"

    fake_library.queue_submit(
        CYNAPSA_STATUS_V1_OK, result=_admission("rejected", accepted=False)
    )
    with pytest.raises(NativeError) as rejected:
        core.submit("core.init", "session", command_id="rejected")
    assert rejected.value.code == "queue_full"
    assert rejected.value.details["failure_kind"] == "admission_rejected"

    future = core.submit("core.init", "session", command_id="accepted")
    assert isinstance(future, CommandFuture)
    assert core.pending_command_count == 1
    assert json.loads(fake_library.submit_inputs[-1]) == {
        "abi_version": 1,
        "command_id": "accepted",
        "command_name": "core.init",
        "sdk_session_id": "session",
        "args": {},
    }
    core.close()


def test_duplicate_pending_command_id_is_rejected_locally(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    first = core.submit("core.init", "session", command_id="same")
    with pytest.raises(NativeError) as duplicate:
        core.submit("core.status", "session", command_id="same")
    assert duplicate.value.code == "duplicate_command_id"
    assert fake_library.calls["core_submit"] == 1
    assert not first.done()
    core.close()


def test_terminal_id_history_is_bounded_to_queue_limit_and_allows_safe_reuse() -> None:
    fake = FakeLibrary()
    config = {
        "abi_version": 1,
        "command_timeout_ms": 0,
        "rpc_timeout_ms": 0,
        "queue_limit": 2,
        "payload_limit": 1048576,
    }
    core = _core(fake, config)
    for command_id in ("first", "second", "third"):
        future = core.submit("core.init", "session", command_id=command_id)
        fake.queue_completion(CYNAPSA_STATUS_V1_OK, result=_success(command_id))
        core.poll_completion(1)
        assert future.result(0).command_id == command_id

    reused = core.submit("core.init", "session", command_id="first")
    assert not reused.done()
    with pytest.raises(NativeError) as duplicate:
        core.submit("core.init", "session", command_id="third")
    assert duplicate.value.code == "duplicate_command_id"
    core.close()


def test_success_and_failure_completions_route_to_only_the_matching_future(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    first = core.submit("core.init", "session", command_id="first")
    second = core.submit("core.status", "session", command_id="second")
    fake_library.queue_completion(
        CYNAPSA_STATUS_V1_OK,
        result=_success("second", "status", {
            "lifecycle": "ready", "connectivity": "available", "personality": "unset",
            "agent_id": "", "mesh_id": "", "mesh_endpoint": "", "queued_message_count": 0,
        }),
    )
    assert core.poll_completion(1).command_id == "second"  # type: ignore[union-attr]
    assert second.result(0).result_type == "status"
    assert not first.done()

    fake_library.queue_completion(CYNAPSA_STATUS_V1_OK, result=_failed("first"))
    core.poll_completion(1)
    with pytest.raises(NativeError) as failed:
        first.result(0)
    assert failed.value.code == "connectivity_unavailable"
    assert first.completion(0).error is failed.value
    assert core.pending_command_count == 0
    core.close()


def test_poll_and_future_timeouts_preserve_pending_ownership(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    future = core.submit("core.init", "session", command_id="waiting")
    assert core.poll_completion(1) is None
    with pytest.raises(TimeoutError):
        future.result(0)
    assert core.pending_command_count == 1
    fake_library.queue_completion(CYNAPSA_STATUS_V1_OK, result=_success("waiting"))
    core.poll_completion(1)
    assert future.result(0).ok
    core.close()


def test_malformed_unknown_and_duplicate_completions_do_not_corrupt_futures(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    future = core.submit("core.init", "session", command_id="known")
    fake_library.queue_completion(
        CYNAPSA_STATUS_V1_OK,
        result=b'{"abi_version":1,"command_id":"known","ok":true,"result":{},"result":{}}',
    )
    with pytest.raises(NativeError) as malformed:
        core.poll_completion(1)
    assert malformed.value.code == "native_completion_decode_failed"
    assert not future.done()

    fake_library.queue_completion(CYNAPSA_STATUS_V1_OK, result=_success("unknown"))
    with pytest.raises(NativeError) as unknown:
        core.poll_completion(1)
    assert unknown.value.code == "unknown_completion"
    assert not future.done()

    fake_library.queue_completion(CYNAPSA_STATUS_V1_OK, result=_success("known"))
    core.poll_completion(1)
    assert future.done()
    fake_library.queue_completion(CYNAPSA_STATUS_V1_OK, result=_success("known"))
    with pytest.raises(NativeError) as duplicate:
        core.poll_completion(1)
    assert duplicate.value.code == "duplicate_completion"
    assert future.result(0).command_id == "known"
    assert all(count == 1 for count in fake_library.free_counts.values())
    core.close()


def test_mismatched_admission_is_cancelled_by_opaque_handle_and_retired(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    fake_library.queue_submit(CYNAPSA_STATUS_V1_OK, result=_admission("other"))
    with pytest.raises(NativeError) as mismatch:
        core.submit("core.init", "session", command_id="wanted")
    assert mismatch.value.code == "mismatched_admission"
    assert json.loads(fake_library.cancel_inputs[0]) == {
        "abi_version": 1,
        "handle": VALID_COMMAND_HANDLE,
    }
    assert core.pending_command_count == 0
    core.close()


def test_direct_cancellation_never_submits_command_cancel(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    future = core.submit("message.request", "session", {"to": "peer", "payload": {"native": {"content_type": "", "path": "/", "body": ""}}, "ttl_ms": 0}, command_id="cancel-me")
    submissions = fake_library.calls["core_submit"]
    assert future.cancel() is True
    assert fake_library.calls["core_cancel"] == 1
    assert fake_library.calls["core_submit"] == submissions
    assert json.loads(fake_library.cancel_inputs[-1]) == {
        "abi_version": 1,
        "handle": VALID_COMMAND_HANDLE,
    }
    assert future.cancel() is False
    fake_library.queue_completion(CYNAPSA_STATUS_V1_OK, result={
        "abi_version": 1,
        "command_id": "cancel-me",
        "ok": False,
        "error": _error("request_cancelled", "The local request wait was cancelled"),
    })
    core.poll_completion(1)
    assert future.cancelled()
    core.close()


def test_shutdown_terminally_fails_pending_and_blocks_new_submissions(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    future = core.submit("core.init", "session", command_id="pending")
    core.shutdown()
    with pytest.raises(NativeError) as terminal:
        future.result(0)
    assert terminal.value.code == "shutdown_in_progress"
    assert terminal.value.details["terminal"] is True
    with pytest.raises(NativeError) as closed:
        core.submit("core.status", "session", command_id="late")
    assert closed.value.code == "shutdown_in_progress"
    core.destroy()


def test_shutdown_timeout_keeps_pending_until_terminal_join(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    fake_library.queue("core_shutdown", CYNAPSA_STATUS_V1_WAIT_TIMEOUT)
    core = _core(fake_library, core_config)
    future = core.submit("core.init", "session", command_id="pending")
    with pytest.raises(NativeError):
        core.shutdown(1)
    assert not future.done()
    assert core.pending_command_count == 1
    core.shutdown(1)
    assert future.done()
    core.destroy()


def test_async_wait_uses_standard_waiter_while_polling_remains_explicit(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    future = core.submit("core.init", "session", command_id="async")
    fake_library.queue_completion(CYNAPSA_STATUS_V1_OK, result=_success("async"))

    async def run() -> str:
        waiter = asyncio.create_task(future.wait_async(1))
        await asyncio.to_thread(core.poll_completion, 1)
        return (await waiter).command_id

    assert asyncio.run(run()) == "async"
    core.close()


def test_async_waiter_cancellation_does_not_cancel_native_command(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    future = core.submit("core.init", "session", command_id="async-cancel")

    async def run() -> None:
        waiter = asyncio.create_task(future.wait_async())
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert fake_library.calls["core_cancel"] == 0
        fake_library.queue_completion(
            CYNAPSA_STATUS_V1_OK, result=_success("async-cancel")
        )
        core.poll_completion(1)
        assert future.result(0).ok

    asyncio.run(run())
    core.close()


def test_completion_poll_has_single_consumer_ownership(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    entered = threading.Event()
    release = threading.Event()

    def blocking_poll(
        native_core: int, timeout_ms: int, out_result: Any, out_error: Any
    ) -> int:
        fake_library._record(
            "core_next_completion", (native_core, timeout_ms, out_result, out_error)
        )
        fake_library._reset_descriptor(out_result)
        fake_library._reset_descriptor(out_error)
        entered.set()
        assert release.wait(2)
        return CYNAPSA_STATUS_V1_WAIT_TIMEOUT

    fake_library.cynapsa_v1_core_next_completion.implementation = blocking_poll
    first_errors: list[BaseException] = []

    def first_poll() -> None:
        try:
            core.poll_completion(1000)
        except BaseException as exc:
            first_errors.append(exc)

    thread = threading.Thread(target=first_poll)
    thread.start()
    assert entered.wait(1)
    with pytest.raises(NativeError) as concurrent:
        core.poll_completion(0)
    assert concurrent.value.code == "poll_in_progress"
    release.set()
    thread.join(2)
    assert not thread.is_alive()
    assert first_errors == []
    core.close()


def test_shutdown_drains_completion_and_event_queues_before_returning(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    future = core.submit("core.init", "session", command_id="drained")
    fake_library.queue_completion(
        CYNAPSA_STATUS_V1_OK, result=_success("drained")
    )
    fake_library.queue_event(
        CYNAPSA_STATUS_V1_OK,
        result={"opaque_event_document": "consumed but not exposed"},
    )

    def shutdown_after_drain(
        native_core: int, timeout_ms: int, out_error: Any
    ) -> int:
        fake_library._record("core_shutdown", (native_core, timeout_ms, out_error))
        fake_library._reset_descriptor(out_error)
        deadline = threading.Event()
        for _ in range(1000):
            if not fake_library.completion_responses and not fake_library.event_responses:
                return CYNAPSA_STATUS_V1_OK
            deadline.wait(0.001)
        pytest.fail("shutdown pumps did not drain both native queues")

    fake_library.cynapsa_v1_core_shutdown.implementation = shutdown_after_drain
    core.shutdown(1000)

    assert future.result(0).command_id == "drained"
    assert fake_library.calls["core_next_event"] >= 1
    assert all(count == 1 for count in fake_library.free_counts.values())
    core.destroy()


def test_completion_descriptor_pair_is_retired_exactly_once_on_abi_failure(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    future = core.submit("core.init", "session", command_id="pending")
    fake_library.queue_completion(
        CYNAPSA_STATUS_V1_ERROR,
        result=_success("pending"),
        error={"code": "core_error", "message": "poll failed"},
    )
    before = set(fake_library.free_counts)
    with pytest.raises(NativeError) as raised:
        core.poll_completion(1)
    assert raised.value.details["failure_kind"] == "abi_call"
    new_handles = set(fake_library.free_counts) - before
    assert len(new_handles) == 2
    assert all(fake_library.free_counts[handle] == 1 for handle in new_handles)
    assert not future.done()
    core.close()


def test_concurrent_timeout_poll_and_cancel_leave_one_terminal_result(
    fake_library: FakeLibrary, core_config: dict[str, int]
) -> None:
    core = _core(fake_library, core_config)
    future = core.submit("core.init", "session", command_id="race")
    fake_library.queue_completion(CYNAPSA_STATUS_V1_OK, result=_success("race"))
    errors: list[BaseException] = []

    def cancel() -> None:
        try:
            future.cancel()
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=cancel)
    thread.start()
    core.poll_completion(1)
    thread.join()
    assert errors == []
    assert future.result(0).command_id == "race"
    assert core.pending_command_count == 0
    core.close()
