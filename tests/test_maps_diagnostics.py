"""Maps tool validation logs categories without leaking request data."""

from __future__ import annotations

import importlib
import importlib.util
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


MAPS = Path(__file__).resolve().parents[1] / "maps"


def _maps_app(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.syspath_prepend(str(MAPS / "vendor" / "cynapsa-python-sdk" / "src"))
    monkeypatch.syspath_prepend(str(MAPS))
    runtime = importlib.import_module("runtime")
    monkeypatch.setattr(runtime, "prepare_runtime", lambda: None)
    spec = importlib.util.spec_from_file_location("demo_maps_diagnostic_app", MAPS / "app.py")
    assert spec is not None and spec.loader is not None
    app = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(app)
    return app


def _call(name: str, arguments: str, call_id: str) -> dict:
    return {
        "id": call_id,
        "function": {"name": name, "arguments": arguments},
    }


def test_unknown_place_id_reports_reason_without_values(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    app = _maps_app(monkeypatch)
    private_query = "private-address-sentinel"
    private_id = "private-place-id-sentinel"
    client = SimpleNamespace(complete=Mock(side_effect=[
        {"tool_calls": [_call("search_places", json.dumps({"query": private_query}), "c1")]},
        {"tool_calls": [_call("get_place_details", json.dumps({"place_id": private_id}), "c2")]},
    ]))
    places = SimpleNamespace(
        search=Mock(return_value={"places": [{"id": "different-id"}]}),
        details=Mock(),
    )

    with caplog.at_level(logging.WARNING), pytest.raises(app.cynapsa.RPCException) as failure:
        app.answer_question("private-question-sentinel", client, places)

    assert failure.value.status_code == 502
    assert failure.value.code == "maps_tool_error"
    assert "round=2 call=1 reason=place_id_not_from_search" in caplog.text
    for secret in (private_query, private_id, "private-question-sentinel", "different-id"):
        assert secret not in caplog.text
    places.details.assert_not_called()


def test_invalid_tool_arguments_report_safe_reason(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    app = _maps_app(monkeypatch)
    client = SimpleNamespace(complete=Mock(return_value={
        "tool_calls": [_call("search_places", "private-malformed-json-sentinel", "c1")],
    }))
    places = SimpleNamespace(search=Mock(), details=Mock())

    with caplog.at_level(logging.WARNING), pytest.raises(app.cynapsa.RPCException) as failure:
        app.answer_question("private-question-sentinel", client, places)

    assert failure.value.code == "maps_tool_error"
    assert "round=1 call=1 reason=invalid_arguments_json" in caplog.text
    assert "private-malformed-json-sentinel" not in caplog.text
    assert "private-question-sentinel" not in caplog.text
    places.search.assert_not_called()


def test_ten_tool_calls_are_allowed_but_eleven_are_rejected(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    app = _maps_app(monkeypatch)
    calls = [
        _call("search_places", json.dumps({"query": "test"}), f"c{index}")
        for index in range(11)
    ]
    places = SimpleNamespace(
        search=Mock(return_value={"places": []}),
        details=Mock(),
    )
    ten_call_client = SimpleNamespace(complete=Mock(side_effect=[
        {"tool_calls": calls[:10]},
        {"tool_calls": [], "content": "Done"},
    ]))

    assert app.answer_question("test", ten_call_client, places)["answer"] == "Done"
    assert places.search.call_count == 10

    eleven_call_client = SimpleNamespace(complete=Mock(return_value={"tool_calls": calls}))
    with caplog.at_level(logging.WARNING), pytest.raises(app.cynapsa.RPCException) as failure:
        app.answer_question("test", eleven_call_client, places)

    assert failure.value.code == "maps_tool_error"
    assert "reason=too_many_calls" in caplog.text
    assert places.search.call_count == 10
