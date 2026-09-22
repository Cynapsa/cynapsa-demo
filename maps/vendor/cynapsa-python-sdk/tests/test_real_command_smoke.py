from __future__ import annotations

import os
import time

import pytest

from cynapsa.native.core import NativeCore


@pytest.mark.skipif(
    not os.environ.get("CYNAPSA_CORE_SMOKE_LIBRARY"),
    reason="set CYNAPSA_CORE_SMOKE_LIBRARY to run the real native smoke",
)
def test_real_create_start_core_init_poll_shutdown_destroy() -> None:
    path = os.environ["CYNAPSA_CORE_SMOKE_LIBRARY"]
    core = NativeCore.create(
        {
            "abi_version": 1,
            "command_timeout_ms": 5_000,
            "rpc_timeout_ms": 5_000,
            "queue_limit": 16,
            "payload_limit": 1_048_576,
        },
        library_path=path,
    )
    try:
        core.start()
        future = core.submit(
            "core.init", "python-milestone-2-smoke", command_id="python-smoke-core-init"
        )
        deadline = time.monotonic() + 10
        while not future.done() and time.monotonic() < deadline:
            core.poll_completion(250)
        completion = future.result(0)
        assert completion.result_type == "core_init"
        assert dict(completion.result or {}) == {
            "sdk_session_id": "python-milestone-2-smoke"
        }
        core.shutdown(5_000)
        core.destroy()
    finally:
        if core.lifecycle not in {"closed", "destroyed"}:
            core.shutdown(5_000)
        if core.lifecycle == "closed":
            core.destroy()


@pytest.mark.skipif(
    not os.environ.get("CYNAPSA_CORE_SMOKE_LIBRARY"),
    reason="set CYNAPSA_CORE_SMOKE_LIBRARY to run the real native smoke",
)
def test_real_create_start_core_init_callback_shutdown_destroy() -> None:
    path = os.environ["CYNAPSA_CORE_SMOKE_LIBRARY"]
    core = NativeCore.create(
        {
            "abi_version": 1,
            "command_timeout_ms": 5_000,
            "rpc_timeout_ms": 5_000,
            "queue_limit": 16,
            "payload_limit": 1_048_576,
        },
        library_path=path,
    )
    try:
        core.start()
        core.register_callbacks(16)
        future = core.submit(
            "core.init",
            "python-milestone-3-callback-smoke",
            command_id="python-smoke-callback-core-init",
        )
        completion = future.result(10)
        assert completion.result_type == "core_init"
        assert dict(completion.result or {}) == {
            "sdk_session_id": "python-milestone-3-callback-smoke"
        }
        assert core.callback_dispatch_error is None
        core.shutdown(5_000)
        assert core.delivery_mode is None
        core.destroy()
    finally:
        if core.lifecycle not in {"closed", "destroyed"}:
            core.shutdown(5_000)
        if core.lifecycle == "closed":
            core.destroy()
