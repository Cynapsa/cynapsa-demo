from __future__ import annotations

import json
import base64
from pathlib import Path
from typing import get_args
import unittest

from cynapsa import Admission, Completion, CoreConfig, CoreEvent, CoreStatus, CynapsaError, MAXIMUM_PAYLOAD_BYTES
from cynapsa.errors import error_from_document
from cynapsa.models import CommandName


_VECTORS = Path(__file__).resolve().parents[3] / "conformance" / "v1"


class ConformanceTests(unittest.TestCase):
    def test_abi_and_direct_operation_vectors(self) -> None:
        abi = json.loads((_VECTORS / "abi.json").read_text(encoding="utf-8"))
        operations = json.loads((_VECTORS / "operations.json").read_text(encoding="utf-8"))
        self.assertEqual(abi["abi_version"], 1)
        self.assertEqual(abi["status_values"], {"ok": 0, "error": 1, "wait_timeout": 2})
        self.assertEqual(len(abi["exports"]), 21)
        self.assertEqual(CoreConfig(queue_limit=4, payload_limit=1_048_576).to_document(), operations["core_create"]["json"])
        self.assertEqual(MAXIMUM_PAYLOAD_BYTES, operations["core_create"]["maximum_payload_bytes"])
        self.assertEqual(CoreConfig(payload_limit=MAXIMUM_PAYLOAD_BYTES).to_document()["payload_limit"], MAXIMUM_PAYLOAD_BYTES)
        with self.assertRaises(CynapsaError):
            CoreConfig(payload_limit=operations["core_create"]["maximum_payload_bytes"] + 1).to_document()
        self.assertTrue(Admission.from_document(operations["admission"]).accepted)
        self.assertFalse(Completion.from_document(operations["failed_completion"]).ok)
        self.assertEqual(CoreStatus.from_document(operations["status"]).lifecycle, "created")
        self.assertEqual(error_from_document(operations["normalized_error"]).details.code, "invalid_handle")

    def test_command_catalog_matches_go_owned_vectors(self) -> None:
        commands = json.loads((_VECTORS / "commands.json").read_text(encoding="utf-8"))
        self.assertEqual([item["name"] for item in commands], list(get_args(CommandName)))

    def test_all_result_and_event_vectors_parse(self) -> None:
        results = json.loads((_VECTORS / "results.json").read_text(encoding="utf-8"))
        events = json.loads((_VECTORS / "events.json").read_text(encoding="utf-8"))
        for item in results:
            with self.subTest(result=item["name"]):
                self.assertEqual(Completion.from_document(item["json"]).result_type, item["name"])
        for item in events:
            with self.subTest(event=item["name"]):
                self.assertEqual(CoreEvent.from_document(item["json"]).event_name, item["name"])

    def test_payload_vectors_are_exact_one_public_variant(self) -> None:
        payloads = json.loads((_VECTORS / "payloads.json").read_text(encoding="utf-8"))
        variants = {"native", "http_request", "http_response", "payload_handle"}
        self.assertEqual({item["name"] for item in payloads}, {"native", "http request", "http response", "snapshot handle"})
        for item in payloads:
            value = item["json"]
            self.assertEqual(len(set(value) & variants), 1)
            self.assertEqual(set(value) - variants, set())
            variant = next(iter(value))
            if variant == "payload_handle":
                self.assertIsInstance(value[variant], str)
                continue
            body = value[variant]["body"]
            self.assertEqual(base64.b64encode(base64.b64decode(body, validate=True)).decode("ascii"), body)


if __name__ == "__main__":
    unittest.main()
