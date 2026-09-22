from __future__ import annotations

import os
import subprocess
import sys
import tarfile
import venv
import zipfile
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
VENDOR_FILES = {
    "cynapsa/_core_source.json",
    "cynapsa/py.typed",
    "cynapsa/_vendor/cmd/cynapsacore-shared/cynapsacore_v1.h",
    "cynapsa/_vendor/conformance/v1/README.md",
    "cynapsa/_vendor/conformance/v1/abi.json",
    "cynapsa/_vendor/conformance/v1/commands.json",
    "cynapsa/_vendor/conformance/v1/events.json",
    "cynapsa/_vendor/conformance/v1/operations.json",
    "cynapsa/_vendor/conformance/v1/payloads.json",
    "cynapsa/_vendor/conformance/v1/results.json",
}


@pytest.mark.installed_artifact
def test_wheel_and_sdist_contents_and_clean_install(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    subprocess.run(
        [sys.executable, "-m", "build", "--outdir", str(artifacts)],
        cwd=ROOT,
        check=True,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    wheel = next(artifacts.glob("*.whl"))
    sdist = next(artifacts.glob("*.tar.gz"))

    with zipfile.ZipFile(wheel) as archive:
        wheel_names = set(archive.namelist())
        entry_points = archive.read(
            next(
                name
                for name in wheel_names
                if name.endswith(".dist-info/entry_points.txt")
            )
        ).decode("utf-8")
    assert VENDOR_FILES <= wheel_names
    assert "cynapsa/__init__.py" in wheel_names
    assert "cynapsa = cynapsa._cli:main" in entry_points
    assert not any("__pycache__" in name or name.endswith((".pyc", ".pyo")) for name in wheel_names)
    assert not any(name.startswith(("tests/", "examples/", "docs/")) for name in wheel_names)

    with tarfile.open(sdist, "r:gz") as archive:
        sdist_names = {name.split("/", 1)[1] for name in archive.getnames() if "/" in name}
    for required in (
        "README.md",
        "CHANGELOG.md",
        "RELEASE_NOTES.md",
        "pyproject.toml",
        "docs/DEVELOPMENT.md",
        "docs/RELEASE_READINESS.md",
        "examples/sync_native.py",
        "scripts/verify_core_provenance.py",
        "tests/test_packaging.py",
    ):
        assert required in sdist_names
    assert not any("__pycache__" in name or name.endswith((".pyc", ".pyo")) for name in sdist_names)

    environment = tmp_path / "venv"
    venv.EnvBuilder(with_pip=True, clear=True).create(environment)
    python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    subprocess.run(
        [str(python), "-m", "pip", "install", "--no-deps", str(wheel)],
        cwd=tmp_path,
        check=True,
    )
    smoke = r'''
import importlib.util
import json
from importlib import resources
from pathlib import Path

assert importlib.util.find_spec("requests") is None
assert importlib.util.find_spec("httpx") is None
assert importlib.util.find_spec("urllib3") is None
assert importlib.util.find_spec("starlette") is None
import cynapsa
from cynapsa.exceptions import NativeError
from cynapsa.native.loader import discover_library

assert cynapsa.__version__ == "0.1.0"
assert "NativeError" in cynapsa.__all__
package = resources.files("cynapsa")
source = json.loads(package.joinpath("_core_source.json").read_text(encoding="utf-8"))
assert source["schema_version"] == 1
assert package.joinpath("py.typed").is_file()
assert package.joinpath("_vendor", "conformance", "v1", "abi.json").is_file()
try:
    discover_library(Path("definitely-missing-core-library"))
except NativeError as exc:
    assert exc.code == "native_library_not_found"
else:
    raise AssertionError("missing native library did not fail closed")
'''
    clean_env = {
        key: value
        for key, value in os.environ.items()
        if key not in {"PYTHONPATH", "CYNAPSA_CORE_LIBRARY"}
    }
    subprocess.run([str(python), "-I", "-c", smoke], cwd=tmp_path, env=clean_env, check=True)
    subprocess.run(
        [str(python), "-I", "-m", "cynapsa", "--help"],
        cwd=tmp_path,
        env=clean_env,
        check=True,
        stdout=subprocess.DEVNULL,
    )
