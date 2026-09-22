from __future__ import annotations

import asyncio
import logging
import threading
import time

import httpx
import pytest
import requests
import urllib3

import cynapsa
import cynapsa._http_bridge as bridge_module
import cynapsa.session as session_module
from conftest import FakeLibrary
from cynapsa.native.core import NativeCore
from test_http_bridge import BridgeDriver
from test_session import AUTH, CompletionDriver


def _cynapsa_threads() -> tuple[str, ...]:
    return tuple(
        thread.name
        for thread in threading.enumerate()
        if thread.name.startswith("cynapsa-")
    )


def _wait_for_thread_cleanup() -> None:
    deadline = time.monotonic() + 3
    while _cynapsa_threads() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert _cynapsa_threads() == ()


@pytest.mark.stress
def test_repeated_core_callback_lifecycle_has_no_worker_leak() -> None:
    config = {
        "abi_version": 1,
        "command_timeout_ms": 100,
        "rpc_timeout_ms": 100,
        "queue_limit": 4,
        "payload_limit": 1_048_576,
    }
    for _ in range(40):
        library = FakeLibrary()
        core = NativeCore.create(config, library=library)
        core.start()
        core.register_callbacks(4)
        core.shutdown(100)
        core.destroy()
        assert library.callback is None
        assert library.calls["callbacks_clear"] == 1
        assert library.calls["core_destroy"] == 1
    _wait_for_thread_cleanup()


@pytest.mark.stress
def test_repeated_sessions_concurrent_close_and_secret_hygiene(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    current: list[FakeLibrary] = []
    monkeypatch.setattr(
        session_module,
        "_default_core_factory",
        lambda config: NativeCore.create(config.core_create(), library=current[0]),
    )
    secret = "release-stress-secret-do-not-retain"
    caplog.set_level(logging.DEBUG)
    for _ in range(16):
        library = FakeLibrary()
        current[:] = [library]
        driver = CompletionDriver(library).start()
        session = cynapsa.connect(**{**AUTH, "password": secret})
        owner_values = tuple(
            getattr(session._owner, slot)
            for slot in session._owner.__slots__
            if hasattr(session._owner, slot)
        )
        assert secret not in repr(session)
        assert secret not in repr(owner_values)
        barrier = threading.Barrier(3)
        failures: list[BaseException] = []

        def close() -> None:
            barrier.wait()
            try:
                session.close()
            except BaseException as exc:
                failures.append(exc)

        closers = [threading.Thread(target=close) for _ in range(2)]
        for thread in closers:
            thread.start()
        barrier.wait()
        for thread in closers:
            thread.join(2)
            assert not thread.is_alive()
        driver.stop()
        assert failures == []
        assert sum(c["command_name"] == "auth.logout" for c in driver.commands) == 1
    assert secret not in caplog.text
    with session_module._RETAINED_OWNERS_LOCK:
        assert not session_module._RETAINED_OWNERS
    _wait_for_thread_cleanup()


@pytest.mark.stress
@pytest.mark.asyncio
async def test_repeated_async_sessions_cleanup() -> None:
    for _ in range(8):
        library = FakeLibrary()
        driver = CompletionDriver(library).start()
        original = session_module._default_core_factory
        session_module._default_core_factory = lambda config: NativeCore.create(
            config.core_create(), library=library
        )
        try:
            session = await cynapsa.connect_async(**AUTH)
            await asyncio.gather(session.close(), session.close())
        finally:
            session_module._default_core_factory = original
            driver.stop()
    _wait_for_thread_cleanup()


@pytest.mark.stress
def test_repeated_bridge_lifecycle_restores_all_client_patches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    originals = (
        requests.sessions.Session.send,
        httpx.Client.send,
        httpx.AsyncClient.send,
        urllib3.connectionpool.HTTPConnectionPool.urlopen,
    )
    current: list[FakeLibrary] = []
    monkeypatch.setattr(
        session_module,
        "_default_core_factory",
        lambda config: NativeCore.create(config.core_create(), library=current[0]),
    )
    for _ in range(12):
        library = FakeLibrary()
        current[:] = [library]
        driver = BridgeDriver(library).start()
        handle = cynapsa.login(
            **AUTH,
            address_map={
                "https://release-stress.example": {
                    "recipient": "worker@example.test",
                    "mode": "rpc",
                }
            },
        )
        assert requests.sessions.Session.send is not originals[0]
        handle.close()
        handle.close()
        driver.stop()
        assert (
            requests.sessions.Session.send,
            httpx.Client.send,
            httpx.AsyncClient.send,
            urllib3.connectionpool.HTTPConnectionPool.urlopen,
        ) == originals
        assert not bridge_module._PATCH_MANAGER._bridges
        assert not bridge_module._PATCH_MANAGER._origins
        assert not bridge_module._PATCH_MANAGER._originals
    _wait_for_thread_cleanup()
