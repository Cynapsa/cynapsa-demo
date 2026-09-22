package sdkboundary

import (
	"encoding/base64"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

// MapError normalizes every internal failure without formatting its private cause.
func (a *Adapter) MapError(value *model.Error) *v1.Error {
	if value == nil {
		return nil
	}
	code, message := publicErrorCode(value.Code)
	return &v1.Error{Code: code, Message: message, Retryable: value.Retryable, Stage: publicErrorStage(value.Stage), Location: publicErrorLocation(value.Location), DiagnosticID: publicDiagnosticID(value.DiagnosticID)}
}

func publicDiagnosticID(value string) v1.DiagnosticID {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(decoded) != diagnosticIDBytes || base64.RawURLEncoding.EncodeToString(decoded) != value {
		return ""
	}
	return v1.DiagnosticID(value)
}

func publicErrorCode(code string) (v1.ErrorCode, string) {
	switch code {
	case "sdk_error":
		return v1.ErrorCodeSDK, "The SDK operation could not be completed"
	case "command_error":
		return v1.ErrorCodeCommand, "The command could not be completed"
	case "malformed_input":
		return v1.ErrorCodeMalformedInput, "The command input is invalid"
	case "unsupported_version":
		return v1.ErrorCodeUnsupportedVersion, "The requested contract version is not supported"
	case "invalid_handle":
		return v1.ErrorCodeInvalidHandle, "The local handle is invalid"
	case "queue_full":
		return v1.ErrorCodeQueueFull, "The local queue is full"
	case "request_cancelled":
		return v1.ErrorCodeRequestCancelled, "The local request wait was cancelled"
	case "authentication_failed":
		return v1.ErrorCodeAuthenticationFailed, "Authentication failed"
	case "credential_missing":
		return v1.ErrorCodeCredentialMissing, "Required authentication information is missing"
	case "authorization_rejected":
		return v1.ErrorCodeAuthorizationRejected, "The requested operation is not authorized"
	case "connectivity_unavailable":
		return v1.ErrorCodeConnectivityUnavailable, "Connectivity is unavailable"
	case "peer_unreachable":
		return v1.ErrorCodePeerUnreachable, "The peer is unreachable"
	case "payload_transfer_failed":
		return v1.ErrorCodePayloadTransferFailed, "The payload could not be transferred"
	case "payload_too_large":
		return v1.ErrorCodePayloadTooLarge, "The payload exceeds the configured limit"
	case "payload_integrity_failed":
		return v1.ErrorCodePayloadIntegrityFailed, "Payload integrity validation failed"
	case "delivery_timeout":
		return v1.ErrorCodeDeliveryTimeout, "Delivery timed out"
	case "duplicate_conflict":
		return v1.ErrorCodeDuplicateConflict, "Conflicting duplicate message data was rejected"
	case "handler_error":
		return v1.ErrorCodeHandler, "The application handler failed"
	case "rpc_timeout":
		return v1.ErrorCodeRPCTimeout, "The application response timed out"
	case "shutdown_in_progress":
		return v1.ErrorCodeShutdownInProgress, "Shutdown is in progress"
	case "shutdown_timeout":
		return v1.ErrorCodeShutdownTimeout, "Shutdown did not finish before the deadline"
	case "core_error":
		return v1.ErrorCodeCore, "The AZTM core could not complete the operation"
	default:
		return v1.ErrorCodeCore, "The AZTM core could not complete the operation"
	}
}

func publicErrorStage(stage string) v1.ErrorStage {
	switch stage {
	case "sdk":
		return v1.ErrorStageSDK
	case "auth":
		return v1.ErrorStageAuth
	case "policy":
		return v1.ErrorStagePolicy
	case "connectivity":
		return v1.ErrorStageConnectivity
	case "delivery":
		return v1.ErrorStageDelivery
	case "payload":
		return v1.ErrorStagePayload
	case "handler":
		return v1.ErrorStageHandler
	case "rpc":
		return v1.ErrorStageRPC
	case "shutdown":
		return v1.ErrorStageShutdown
	default:
		return v1.ErrorStageCommand
	}
}

func publicErrorLocation(location string) v1.ErrorLocation {
	if location == "remote" {
		return v1.ErrorLocationRemote
	}
	return v1.ErrorLocationLocal
}
