"""Offline safety and fixture checks; these never launch Docker."""

import importlib.util
from pathlib import Path
import subprocess
import sys
import unittest


SPEC = importlib.util.spec_from_file_location("observability_smoke", Path(__file__).with_name("smoke.py"))
assert SPEC and SPEC.loader
smoke = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(smoke)


class SmokeSafetyTests(unittest.TestCase):
    def test_isolated_compose_resources(self):
        ports = {name: 41000 + index for index, name in enumerate(smoke.IMAGES)}
        document = smoke.compose_document(ports)
        self.assertEqual(set(document["services"]), set(smoke.IMAGES))
        self.assertNotIn("name", document)
        self.assertNotIn("volumes", document)
        self.assertNotIn("name", document["networks"]["smoke"])
        for name, service in document["services"].items():
            self.assertEqual(service["restart"], "no")
            for port in service["ports"]:
                self.assertEqual(port["host_ip"], "127.0.0.1")
                self.assertEqual(port["published"], str(ports[name]))

    def test_metric_fixture_has_no_agent_identity_or_content(self):
        fixture = smoke.otlp_metric("cynapsa.smoke.synthetic", "cynapsa-smoke-synthetic", "smoketest", 7)
        point = fixture["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]
        self.assertEqual(point["gauge"]["dataPoints"][0]["asInt"], "7")
        self.assertNotIn("token", str(fixture).lower())

    def test_live_requires_explicit_flag(self):
        result = subprocess.run([sys.executable, str(Path(__file__).with_name("smoke.py"))],
                                capture_output=True, text=True, timeout=5)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("pass --live explicitly", result.stderr)


if __name__ == "__main__":
    unittest.main()
