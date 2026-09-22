from __future__ import annotations

import os
import unittest

from cynapsa import CynapsaClient


@unittest.skipUnless(os.environ.get("CYNAPSA_CORE_LIBRARY"), "native library path not configured")
class NativeSmokeTests(unittest.TestCase):
    def test_token_methods_reach_native_authentication_without_network(self) -> None:
        for method_name in ("token_login", "token_connect"):
            with self.subTest(method=method_name), CynapsaClient() as client:
                client.start()
                with self.assertRaises(Exception) as raised:
                    getattr(client, method_name)("aztm_invalid.invalid", "mesh-one", command_id=method_name)
                self.assertNotIn("aztm_invalid.invalid", str(raised.exception))
                self.assertEqual(client.status().lifecycle, "created")

    def test_created_core_can_close_cleanly(self) -> None:
        client = CynapsaClient()
        self.assertEqual(client.status().lifecycle, "created")
        client.close(timeout_ms=5_000)

    def test_create_status_start_submit_complete_and_close(self) -> None:
        with CynapsaClient() as client:
            self.assertEqual(client.abi_version, 1)
            self.assertEqual(client.status().lifecycle, "created")
            client.start()
            admission = client.initialize(command_id="python-smoke-init")
            self.assertTrue(admission.accepted)
            completion = client.next_completion(timeout_ms=5_000)
            self.assertIsNotNone(completion)
            assert completion is not None
            self.assertTrue(completion.ok)
            self.assertEqual(completion.command_id, "python-smoke-init")


if __name__ == "__main__":
    unittest.main()
