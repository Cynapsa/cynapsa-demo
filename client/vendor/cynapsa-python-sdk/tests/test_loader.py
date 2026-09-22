from __future__ import annotations

from pathlib import Path

import pytest

from cynapsa.exceptions import NativeError
from cynapsa.native import loader

from conftest import FakeLibrary


def test_discovery_precedence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    explicit = tmp_path / "explicit.so"
    configured = tmp_path / "configured.so"
    explicit.touch()
    configured.touch()
    monkeypatch.setenv(loader.ENV_LIBRARY, str(configured))

    assert loader.discover_library(explicit) == str(explicit.resolve())
    assert loader.discover_library() == str(configured.resolve())


def test_package_then_system_discovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    packaged = tmp_path / "libcynapsacore.so"
    packaged.touch()
    monkeypatch.delenv(loader.ENV_LIBRARY, raising=False)
    monkeypatch.setattr(loader, "_package_candidates", lambda: [packaged])
    monkeypatch.setattr(loader.ctypes.util, "find_library", lambda name: "system.so")
    assert loader.discover_library() == str(packaged)

    packaged.unlink()
    assert loader.discover_library() == "system.so"


def test_discovery_failure_is_actionable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(loader.ENV_LIBRARY, raising=False)
    monkeypatch.setattr(
        loader, "_package_candidates", lambda: [Path("/not/here/libcynapsacore.so")]
    )
    monkeypatch.setattr(loader.ctypes.util, "find_library", lambda name: None)

    with pytest.raises(NativeError) as raised:
        loader.discover_library()

    assert raised.value.code == "native_library_not_found"
    assert loader.ENV_LIBRARY in raised.value.message
    assert raised.value.details["searched"] == ("/not/here/libcynapsacore.so",)


def test_load_configures_and_validates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    library_path = tmp_path / "libcynapsacore.so"
    library_path.touch()
    fake = FakeLibrary()
    monkeypatch.setattr(loader.ctypes, "CDLL", lambda path: fake)

    assert loader.load_library(library_path) is fake
    assert fake.calls["abi_version"] == 1
    assert fake.cynapsa_v1_core_create.argtypes is not None


def test_load_rejects_abi_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    library_path = tmp_path / "libcynapsacore.so"
    library_path.touch()
    fake = FakeLibrary(abi_version=7)
    monkeypatch.setattr(loader.ctypes, "CDLL", lambda path: fake)

    with pytest.raises(NativeError) as raised:
        loader.load_library(library_path)

    assert raised.value.code == "abi_version_mismatch"
