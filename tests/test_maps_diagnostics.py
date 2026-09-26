"""Maps tool validation logs categories without leaking request data."""

from __future__ import annotations

import importlib
import importlib.util
import json
import logging
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest


MAPS = Path(__file__).resolve().parents[1] / "maps"


def _maps_app(monkeypatch: pytest.MonkeyPatch):
    # These tests exercise maps-tool diagnostics, not the SDK transport.
    # Avoid requiring a vendored SDK in this standalone demo checkout.
    if importlib.util.find_spec("cynapsa") is None:
        sdk_stub = ModuleType("cynapsa")

        class RPCException(Exception):
            def __init__(self, status_code, *, code, detail):
                super().__init__(detail)
                self.status_code = status_code
                self.code = code
                self.detail = detail

        sdk_stub.RPCException = RPCException
        monkeypatch.setitem(sys.modules, "cynapsa", sdk_stub)
    monkeypatch.syspath_prepend(str(MAPS))
    runtime = importlib.import_module("runtime")
    monkeypatch.setattr(runtime, "prepare_runtime", lambda: None)
    spec = importlib.util.spec_from_file_location("demo_maps_diagnostic_app", MAPS / "app.py")
    assert spec is not None and spec.loader is not None
    app = importlib.util.module_from_spec(spec)
    # LangGraph resolves TypedDict annotations through the defining module,
    # just as a normal Python import does.
    monkeypatch.setitem(sys.modules, spec.name, app)
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


def test_graph_searches_then_gets_details_and_formats_answer(monkeypatch):
    app = _maps_app(monkeypatch)
    client = SimpleNamespace(complete=Mock(side_effect=[
        {"tool_calls": [_call("search_places", '{"query":"gym"}', "c1")]},
        {"tool_calls": [_call("get_place_details", '{"place_id":"known-id"}', "c2")]},
        {"content": "Gym A is nearby."},
    ]))
    places = SimpleNamespace(
        search=Mock(return_value={"places": [{"id": "known-id", "google_maps_url": "https://maps.example/gym"}]}),
        details=Mock(return_value={"id": "known-id", "name": "Gym A", "google_maps_url": "https://maps.example/gym"}),
    )
    result = app.answer_question("Find a gym", client, places)
    assert result["answer"] == "Gym A is nearby."
    assert result["sources"] == [places.details.return_value]
    places.details.assert_called_once_with("known-id")
    assert client.complete.call_count == 3


def test_graph_stops_after_three_model_rounds(monkeypatch):
    app = _maps_app(monkeypatch)
    client = SimpleNamespace(complete=Mock(return_value={
        "tool_calls": [_call("search_places", '{"query":"gym"}', "c1")],
    }))
    places = SimpleNamespace(search=Mock(return_value={"places": []}))
    with pytest.raises(app.cynapsa.RPCException) as failure:
        app.answer_question("Find a gym", client, places)
    assert failure.value.code == "maps_no_answer"
    assert client.complete.call_count == 3
    assert places.search.call_count == 3
