package v1

// ErrorCode is an allowlisted, implementation-neutral public error code.
type ErrorCode string

// ErrorStage identifies an application-level failure stage.
type ErrorStage string

// ErrorLocation distinguishes local and remote application failures.
type ErrorLocation string

const (
	ErrorCodeSDK                     ErrorCode = "sdk_error"
	ErrorCodeCommand                 ErrorCode = "command_error"
	ErrorCodeMalformedInput          ErrorCode = "malformed_input"
	ErrorCodeUnsupportedVersion      ErrorCode = "unsupported_version"
	ErrorCodeInvalidHandle           ErrorCode = "invalid_handle"
	ErrorCodeQueueFull               ErrorCode = "queue_full"
	ErrorCodeRequestCancelled        ErrorCode = "request_cancelled"
	ErrorCodeAuthenticationFailed    ErrorCode = "authentication_failed"
	ErrorCodeCredentialMissing       ErrorCode = "credential_missing"
	ErrorCodeAuthorizationRejected   ErrorCode = "authorization_rejected"
	ErrorCodeConnectivityUnavailable ErrorCode = "connectivity_unavailable"
	ErrorCodePeerUnreachable         ErrorCode = "peer_unreachable"
	ErrorCodePayloadTransferFailed   ErrorCode = "payload_transfer_failed"
	ErrorCodePayloadTooLarge         ErrorCode = "payload_too_large"
	ErrorCodePayloadIntegrityFailed  ErrorCode = "payload_integrity_failed"
	ErrorCodeDeliveryTimeout         ErrorCode = "delivery_timeout"
	ErrorCodeDuplicateConflict       ErrorCode = "duplicate_conflict"
	ErrorCodeHandler                 ErrorCode = "handler_error"
	ErrorCodeRPCTimeout              ErrorCode = "rpc_timeout"
	ErrorCodeShutdownInProgress      ErrorCode = "shutdown_in_progress"
	ErrorCodeShutdownTimeout         ErrorCode = "shutdown_timeout"
	ErrorCodeCore                    ErrorCode = "core_error"
)

const (
	ErrorStageSDK          ErrorStage = "sdk"
	ErrorStageCommand      ErrorStage = "command"
	ErrorStageAuth         ErrorStage = "auth"
	ErrorStagePolicy       ErrorStage = "policy"
	ErrorStageConnectivity ErrorStage = "connectivity"
	ErrorStageDelivery     ErrorStage = "delivery"
	ErrorStagePayload      ErrorStage = "payload"
	ErrorStageHandler      ErrorStage = "handler"
	ErrorStageRPC          ErrorStage = "rpc"
	ErrorStageShutdown     ErrorStage = "shutdown"
)

const (
	ErrorLocationLocal  ErrorLocation = "local"
	ErrorLocationRemote ErrorLocation = "remote"
)

// Error is the fixed SDK-visible error schema; it deliberately has no raw-cause field.
type Error struct {
	Code         ErrorCode
	Message      string
	Retryable    bool
	Stage        ErrorStage
	Location     ErrorLocation
	DiagnosticID DiagnosticID
}

// Error will return the boundary-owned normalized message.
func (e Error) Error() string {
	return e.Message
}
