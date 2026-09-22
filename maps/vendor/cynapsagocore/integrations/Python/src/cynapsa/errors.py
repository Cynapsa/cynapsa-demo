"""Normalized public SDK errors."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


_ERROR_MESSAGES = {
    "sdk_error": "The SDK operation could not be completed",
    "command_error": "The command could not be completed",
    "malformed_input": "The command input is invalid",
    "unsupported_version": "The requested contract version is not supported",
    "invalid_handle": "The local handle is invalid",
    "queue_full": "The local queue is full",
    "request_cancelled": "The local request wait was cancelled",
    "authentication_failed": "Authentication failed",
    "credential_missing": "Required authentication information is missing",
    "authorization_rejected": "The requested operation is not authorized",
    "connectivity_unavailable": "Connectivity is unavailable",
    "peer_unreachable": "The peer is unreachable",
    "payload_transfer_failed": "The payload could not be transferred",
    "payload_too_large": "The payload exceeds the configured limit",
    "payload_integrity_failed": "Payload integrity validation failed",
    "delivery_timeout": "Delivery timed out",
    "duplicate_conflict": "Conflicting duplicate message data was rejected",
    "handler_error": "The application handler failed",
    "rpc_timeout": "The application response timed out",
    "shutdown_in_progress": "Shutdown is in progress",
    "shutdown_timeout": "Shutdown did not finish before the deadline",
    "core_error": "The AZTM core could not complete the operation",
}
_ERROR_STAGES = frozenset(
    {"sdk", "command", "auth", "policy", "connectivity", "delivery", "payload", "handler", "rpc", "shutdown"}
)
_ERROR_LOCATIONS = frozenset({"local", "remote"})


@dataclass(frozen=True, slots=True)
class ErrorDetails:
    """The fixed V1 public error shape."""

    code: str
    message: str
    retryable: bool
    stage: str
    location: str
    diagnostic_id: str | None = None

    @classmethod
    def from_value(cls, value: object) -> "ErrorDetails":
        if not isinstance(value, Mapping):
            raise _invalid_error_document()
        allowed = {
            "code",
            "message",
            "retryable",
            "stage",
            "local_or_remote",
            "diagnostic_id",
        }
        if set(value) - allowed or not {
            "code",
            "message",
            "retryable",
            "stage",
            "local_or_remote",
        }.issubset(value):
            raise _invalid_error_document()
        code = value["code"]
        message = value["message"]
        retryable = value["retryable"]
        stage = value["stage"]
        location = value["local_or_remote"]
        diagnostic_id = value.get("diagnostic_id")
        if (
            not isinstance(code, str)
            or not isinstance(message, str)
            or type(retryable) is not bool
            or not isinstance(stage, str)
            or not isinstance(location, str)
            or diagnostic_id is not None
            and not isinstance(diagnostic_id, str)
            or _ERROR_MESSAGES.get(code) != message
            or stage not in _ERROR_STAGES
            or location not in _ERROR_LOCATIONS
            or diagnostic_id is not None
            and (len(diagnostic_id) != 43 or any(character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_" for character in diagnostic_id))
        ):
            raise _invalid_error_document()
        return cls(code, message, retryable, stage, location, diagnostic_id)


class CynapsaError(RuntimeError):
    """A stable application-level SDK failure."""

    def __init__(self, details: ErrorDetails) -> None:
        super().__init__(details.message)
        self.details = details


def error_from_document(document: object) -> CynapsaError:
    if not isinstance(document, Mapping) or set(document) != {"abi_version", "error"}:
        return _invalid_error_document()
    if document["abi_version"] != 1:
        return CynapsaError(
            ErrorDetails(
                code="unsupported_version",
                message="The native core returned an unsupported ABI version",
                retryable=False,
                stage="sdk",
                location="local",
            )
        )
    try:
        return CynapsaError(ErrorDetails.from_value(document["error"]))
    except CynapsaError as error:
        return error


def sdk_error(message: str, *, code: str = "sdk_error") -> CynapsaError:
    return CynapsaError(
        ErrorDetails(
            code=code,
            message=message,
            retryable=False,
            stage="sdk",
            location="local",
        )
    )


def _invalid_error_document() -> CynapsaError:
    return sdk_error("The native core returned an invalid error document")
