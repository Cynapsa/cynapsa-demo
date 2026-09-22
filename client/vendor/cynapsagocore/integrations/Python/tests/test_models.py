from __future__ import annotations

import unittest

from cynapsa import (
    Admission,
    Completion,
    CoreConfig,
    CoreEvent,
    CoreStatus,
    CynapsaClient,
    CynapsaError,
    ErrorDetails,
    MAXIMUM_PAYLOAD_BYTES,
)


class ModelTests(unittest.TestCase):
    def test_config_encodes_exact_v1_shape(self) -> None:
        self.assertEqual(
            CoreConfig(queue_limit=4, payload_limit=1024).to_document(),
            {
                "abi_version": 1,
                "command_timeout_ms": 0,
                "rpc_timeout_ms": 0,
                "queue_limit": 4,
                "payload_limit": 1024,
            },
        )

    def test_admission_rejects_unknown_fields(self) -> None:
        with self.assertRaises(CynapsaError):
            Admission.from_document(
                {
                    "abi_version": 1,
                    "command_id": "command-1",
                    "command_handle": "handle-1",
                    "accepted": True,
                    "extra": "not-allowed",
                }
            )

    def test_completion_and_status_parse_public_shapes(self) -> None:
        completion = Completion.from_document(
            {
                "abi_version": 1,
                "command_id": "command-1",
                "ok": True,
                "result_type": "empty",
                "result": {},
            }
        )
        self.assertTrue(completion.ok)

        status = CoreStatus.from_document(
            {
                "abi_version": 1,
                "status": {
                    "lifecycle": "created",
                    "connectivity": "unknown",
                    "personality": "unset",
                    "agent_id": "",
                    "mesh_id": "",
                    "mesh_endpoint": "",
                    "queued_message_count": 0,
                },
            }
        )
        self.assertEqual(status.lifecycle, "created")

    def test_unmodeled_nested_fields_are_not_exposed(self) -> None:
        completion = Completion.from_document(
            {
                "abi_version": 1,
                "command_id": "command-1",
                "ok": True,
                "result_type": "empty",
                "result": {"private_field": "dropped"},
            }
        )
        event = CoreEvent.from_document(
            {
                "abi_version": 1,
                "event_id": "event-1",
                "event_name": "command.queue_full",
                "created_at": "1970-01-01T00:00:01Z",
                "payload": {"private_field": "dropped"},
            }
        )
        self.assertFalse(hasattr(completion, "result"))
        self.assertFalse(hasattr(event, "payload"))

    def test_noncanonical_error_message_is_rejected(self) -> None:
        with self.assertRaises(CynapsaError):
            ErrorDetails.from_value(
                {
                    "code": "core_error",
                    "message": "Unnormalized failure",
                    "retryable": False,
                    "stage": "command",
                    "local_or_remote": "local",
                }
            )

    def test_retired_delivery_contract_values_are_rejected(self) -> None:
        for event_name in ("delivery.ordering_blocked", "delivery.unknown"):
            with self.subTest(event_name=event_name), self.assertRaises(CynapsaError):
                CoreEvent.from_document(
                    {
                        "abi_version": 1,
                        "event_id": "event-1",
                        "event_name": event_name,
                        "created_at": "1970-01-01T00:00:01Z",
                        "payload": {},
                    }
                )
        with self.assertRaises(CynapsaError):
            ErrorDetails.from_value(
                {
                    "code": "core_error",
                    "message": "The AZTM core could not complete the operation",
                    "retryable": False,
                    "stage": "ordering",
                    "local_or_remote": "local",
                }
            )

    def test_config_rejects_timeout_above_native_range(self) -> None:
        with self.assertRaises(CynapsaError):
            CoreConfig(command_timeout_ms=9_223_372_036_855).to_document()

    def test_config_payload_limit_uses_exact_v1_boundary(self) -> None:
        at_limit = CoreConfig(payload_limit=MAXIMUM_PAYLOAD_BYTES).to_document()
        self.assertEqual(at_limit["payload_limit"], MAXIMUM_PAYLOAD_BYTES)
        with self.assertRaises(CynapsaError):
            CoreConfig(payload_limit=MAXIMUM_PAYLOAD_BYTES + 1).to_document()

    def test_event_timestamps_reject_noncalendar_dates(self) -> None:
        with self.assertRaises(CynapsaError):
            CoreEvent.from_document(
                {
                    "abi_version": 1,
                    "event_id": "event-1",
                    "event_name": "command.queue_full",
                    "created_at": "2026-02-31T00:00:00Z",
                    "payload": {},
                }
            )

    def test_close_preserves_shutdown_failure_after_destroy(self) -> None:
        failure = CynapsaError(
            ErrorDetails(
                code="shutdown_timeout",
                message="Shutdown did not finish before the deadline",
                retryable=False,
                stage="shutdown",
                location="local",
            )
        )

        class NativeFailure:
            destroyed = False

            def shutdown(self, _timeout_ms: int) -> None:
                raise failure

            def destroy(self) -> None:
                self.destroyed = True

        client = object.__new__(CynapsaClient)
        client._native = NativeFailure()
        client._state = "started"
        with self.assertRaises(CynapsaError) as raised:
            client.close()
        self.assertIs(raised.exception, failure)
        self.assertEqual(client._state, "closed")
        self.assertTrue(client._native.destroyed)


if __name__ == "__main__":
    unittest.main()
