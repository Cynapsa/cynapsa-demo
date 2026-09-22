from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from importlib import resources
from pathlib import Path


def test_vendored_contract_matches_declared_provenance() -> None:
    package = resources.files("cynapsa")
    source = json.loads(package.joinpath("_core_source.json").read_text(encoding="utf-8"))
    assert source["schema_version"] == 1
    assert source["go_core_commit"] == "a21e4a23cf0c5b334e2818e2faa8b025d3d1e2b2"
    assert len(source["sha256"]) == 8
    for relative, expected in source["sha256"].items():
        actual = hashlib.sha256(
            package.joinpath("_vendor", *relative.split("/")).read_bytes()
        ).hexdigest()
        assert actual == expected


def test_package_import_and_native_models_do_not_import_optional_http_clients() -> None:
    root = Path(__file__).parents[1]
    code = r'''
import builtins
blocked = {"requests", "httpx", "urllib3", "starlette", "fastapi", "aiohttp"}
seen = []
original = builtins.__import__
def guarded(name, globals=None, locals=None, fromlist=(), level=0):
    top = name.split(".", 1)[0]
    if top in blocked:
        seen.append(name)
        raise ModuleNotFoundError(name)
    return original(name, globals, locals, fromlist, level)
builtins.__import__ = guarded
import cynapsa
assert cynapsa.NativePayload.from_json({"ok": True}).body == b'{"ok":true}'
request = cynapsa.HTTPRequestPayload("GET", "/")
response = cynapsa.HTTPResponsePayload(200, "OK", (), b"ok")
assert request.body == b""
assert response.body == b"ok"
assert seen == []
'''
    env = {**os.environ, "PYTHONPATH": str(root / "src")}
    completed = subprocess.run(
        [sys.executable, "-c", code],
        cwd=root,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr + completed.stdout
