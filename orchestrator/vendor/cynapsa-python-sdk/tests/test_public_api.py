from __future__ import annotations

import ast
import inspect
from pathlib import Path

import cynapsa
from cynapsa import messaging


ROOT = Path(__file__).resolve().parents[1]


def test_public_all_is_unique_complete_and_handle_free() -> None:
    assert len(cynapsa.__all__) == len(set(cynapsa.__all__))
    assert all(hasattr(cynapsa, name) for name in cynapsa.__all__)
    assert {"NativeError", "AztmAuthInfo", "AztmStatus", "AztmCapabilities"} <= set(
        cynapsa.__all__
    )
    assert not any("PayloadHandle" in name for name in cynapsa.__all__)
    assert not hasattr(cynapsa.AztmSession, "payload_open")
    assert not hasattr(cynapsa.AsyncAztmSession, "payload_open")


def test_high_level_signatures_remain_keyword_safe() -> None:
    connect = inspect.signature(cynapsa.connect)
    assert all(
        parameter.kind is inspect.Parameter.KEYWORD_ONLY
        for parameter in connect.parameters.values()
    )
    assert inspect.signature(cynapsa.AztmSession.send).parameters["path"].kind is inspect.Parameter.KEYWORD_ONLY
    assert inspect.signature(cynapsa.AztmSession.request).parameters["ttl_ms"].default == 0
    assert "mode" not in inspect.signature(cynapsa.AztmSession.on).parameters
    assert "mode" not in inspect.signature(cynapsa.AztmSession.off).parameters
    assert not hasattr(cynapsa.CynapsaRequest, "reply")
    assert messaging.AztmRequest is cynapsa.CynapsaRequest
    assert messaging.AsyncAztmRequest is cynapsa.CynapsaRequest


def test_all_python_sources_parse_as_python_310() -> None:
    for directory in (ROOT / "src", ROOT / "tests", ROOT / "scripts", ROOT / "examples"):
        for path in directory.rglob("*.py"):
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path), feature_version=(3, 10))
