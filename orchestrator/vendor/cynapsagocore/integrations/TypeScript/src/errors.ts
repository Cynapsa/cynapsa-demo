export interface ErrorDetails {
  readonly code: string;
  readonly message: string;
  readonly retryable: boolean;
  readonly stage: string;
  readonly location: string;
  readonly diagnosticId?: string;
}

const ERROR_MESSAGES = new Map([
  ["sdk_error", "The SDK operation could not be completed"],
  ["command_error", "The command could not be completed"],
  ["malformed_input", "The command input is invalid"],
  ["unsupported_version", "The requested contract version is not supported"],
  ["invalid_handle", "The local handle is invalid"],
  ["queue_full", "The local queue is full"],
  ["request_cancelled", "The local request wait was cancelled"],
  ["authentication_failed", "Authentication failed"],
  ["credential_missing", "Required authentication information is missing"],
  ["authorization_rejected", "The requested operation is not authorized"],
  ["connectivity_unavailable", "Connectivity is unavailable"],
  ["peer_unreachable", "The peer is unreachable"],
  ["payload_transfer_failed", "The payload could not be transferred"],
  ["payload_too_large", "The payload exceeds the configured limit"],
  ["payload_integrity_failed", "Payload integrity validation failed"],
  ["delivery_timeout", "Delivery timed out"],
  ["duplicate_conflict", "Conflicting duplicate message data was rejected"],
  ["handler_error", "The application handler failed"],
  ["rpc_timeout", "The application response timed out"],
  ["shutdown_in_progress", "Shutdown is in progress"],
  ["shutdown_timeout", "Shutdown did not finish before the deadline"],
  ["core_error", "The AZTM core could not complete the operation"],
]);

const ERROR_STAGES = new Set([
  "sdk",
  "command",
  "auth",
  "policy",
  "connectivity",
  "delivery",
  "payload",
  "handler",
  "rpc",
  "shutdown",
]);

const ERROR_LOCATIONS = new Set(["local", "remote"]);

export class CynapsaError extends Error {
  readonly details: ErrorDetails;

  constructor(details: ErrorDetails) {
    super(details.message);
    this.name = "CynapsaError";
    this.details = details;
  }
}

export function errorFromDocument(value: unknown): CynapsaError {
  if (!isRecord(value) || !hasExactKeys(value, ["abi_version", "error"])) {
    return invalidErrorDocument();
  }
  if (value.abi_version !== 1) {
    return sdkError("The native core returned an unsupported ABI version", "unsupported_version");
  }
  try {
    return new CynapsaError(parseErrorDetails(value.error));
  } catch (error) {
    return error instanceof CynapsaError ? error : invalidErrorDocument();
  }
}

export function parseErrorDetails(value: unknown): ErrorDetails {
  if (
    !isRecord(value) ||
    !hasAllowedKeys(
      value,
      ["code", "message", "retryable", "stage", "local_or_remote"],
      ["diagnostic_id"],
    ) ||
    typeof value.code !== "string" ||
    typeof value.message !== "string" ||
    typeof value.retryable !== "boolean" ||
    typeof value.stage !== "string" ||
    typeof value.local_or_remote !== "string" ||
    (value.diagnostic_id !== undefined && typeof value.diagnostic_id !== "string") ||
    ERROR_MESSAGES.get(value.code) !== value.message ||
    !ERROR_STAGES.has(value.stage) ||
    !ERROR_LOCATIONS.has(value.local_or_remote) ||
    (value.diagnostic_id !== undefined && !/^[A-Za-z0-9_-]{43}$/.test(value.diagnostic_id))
  ) {
    throw invalidErrorDocument();
  }
  const details: ErrorDetails = {
    code: value.code,
    message: value.message,
    retryable: value.retryable,
    stage: value.stage,
    location: value.local_or_remote,
  };
  return value.diagnostic_id === undefined
    ? details
    : { ...details, diagnosticId: value.diagnostic_id };
}

export function sdkError(message: string, code = "sdk_error"): CynapsaError {
  return new CynapsaError({
    code,
    message,
    retryable: false,
    stage: "sdk",
    location: "local",
  });
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function hasExactKeys(value: Record<string, unknown>, keys: readonly string[]): boolean {
  const actual = Object.keys(value);
  return actual.length === keys.length && keys.every((key) => Object.hasOwn(value, key));
}

export function hasAllowedKeys(
  value: Record<string, unknown>,
  required: readonly string[],
  optional: readonly string[],
): boolean {
  const allowed = new Set([...required, ...optional]);
  return required.every((key) => Object.hasOwn(value, key)) && Object.keys(value).every((key) => allowed.has(key));
}

function invalidErrorDocument(): CynapsaError {
  return sdkError("The native core returned an invalid error document");
}
