import { hasAllowedKeys, hasExactKeys, isRecord, parseErrorDetails, sdkError } from "./errors.js";
import type { ErrorDetails } from "./errors.js";

export type JsonScalar = null | boolean | number | string;
export type JsonValue = JsonScalar | JsonValue[] | { [key: string]: JsonValue };
export type JsonObject = { [key: string]: JsonValue };

export const COMMAND_NAMES = [
  "core.init",
  "core.capabilities",
  "core.status",
  "core.shutdown",
  "session.config.get",
  "session.config.update",
  "command.channel.register",
  "command.channel.clear",
  "command.cancel",
  "event.sink.register",
  "event.sink.clear",
  "event.sink.bind",
  "auth.login",
  "auth.connect",
  "auth.token_login",
  "auth.token_connect",
  "auth.installation_login",
  "auth.installation_connect",
  "auth.logout",
  "auth.agent_id",
  "mesh.list",
  "mesh.membership.refresh",
  "address.map.put",
  "address.map.remove",
  "address.map.list",
  "address.resolve",
  "message.send",
  "message.request",
  "message.reply",
  "delivery.next",
  "delivery.accept",
  "handler.register",
  "handler.unregister",
  "payload.open",
  "payload.write_chunk",
  "payload.finish",
  "payload.cancel",
  "payload.read",
  "payload.close",
  "payload.retain",
  "payload.release",
  "delivery.queue.status",
  "delivery.retry",
  "delivery.pause",
  "delivery.resume",
  "delivery.drop",
  "conversation.list",
  "conversation.status",
  "conversation.close",
  "policy.set",
  "policy.get",
  "policy.test",
  "diagnostics.peer_status",
  "diagnostics.connectivity_status",
  "diagnostics.snapshot",
  "diagnostics.logs.subscribe",
] as const;

export type CommandName = (typeof COMMAND_NAMES)[number];

const RESULT_TYPES = new Set([
  "empty",
  "capabilities",
  "status",
  "config",
  "auth",
  "agent_id",
  "mesh_list",
  "address_mappings",
  "address_resolution",
  "send",
  "response",
  "event",
  "delivery_queue_status",
  "payload_handle",
  "conversation_status",
  "conversation_list",
  "policy",
  "peer_status",
  "connectivity_status",
  "diagnostic_snapshot",
  "core_init",
  "completion_channel",
  "event_sink",
]);

const EVENT_NAMES = new Set([
  "session.state_changed",
  "connectivity.state_changed",
  "peer.reachable",
  "peer.unreachable",
  "message.received",
  "message.queued",
  "message.retried",
  "message.deduplicated",
  "delivery.resumed",
  "delivery.replayed",
  "payload.transfer_started",
  "payload.transfer_progress",
  "payload.transfer_completed",
  "payload.transfer_failed",
  "policy.rejected",
  "rpc.timeout",
  "command.queue_full",
  "event.queue_full",
  "core.error",
  "diagnostics.log",
]);

const LIFECYCLES = new Set(["created", "connecting", "ready", "degraded", "closing", "closed", "failed"]);
const CONNECTIVITY_STATES = new Set(["unknown", "available", "degraded", "unavailable"]);
const PERSONALITIES = new Set(["unset", "http_bridge", "native"]);
const MAX_TIMEOUT_MS = 9_223_372_036_854;
export const MAXIMUM_PAYLOAD_BYTES = 134_217_696;
const COMMAND_NAME_SET = new Set<string>(COMMAND_NAMES);

export interface CoreConfig {
  readonly commandTimeoutMs?: number;
  readonly rpcTimeoutMs?: number;
  readonly queueLimit?: number;
  readonly payloadLimit?: number;
}

export interface Admission {
  readonly commandId: string;
  readonly commandHandle: string;
  readonly accepted: boolean;
  readonly error: ErrorDetails | null;
}

export interface Completion {
  readonly commandId: string;
  readonly ok: boolean;
  readonly resultType: string | null;
  readonly error: ErrorDetails | null;
}

export interface CoreEvent {
  readonly eventId: string;
  readonly eventName: string;
  readonly createdAt: string;
}

export interface CoreStatus {
  readonly lifecycle: string;
  readonly connectivity: string;
  readonly personality: string;
  readonly agentId: string;
  readonly meshId: string;
  readonly meshEndpoint: string;
  readonly queuedMessageCount: number;
}

export function configDocument(config: CoreConfig = {}): JsonObject {
  const commandTimeoutMs = config.commandTimeoutMs ?? 0;
  const rpcTimeoutMs = config.rpcTimeoutMs ?? 0;
  const queueLimit = config.queueLimit ?? 256;
  const payloadLimit = config.payloadLimit ?? 64 * 1024 * 1024;
  for (const value of [commandTimeoutMs, rpcTimeoutMs, queueLimit, payloadLimit]) {
    if (!Number.isSafeInteger(value)) {
      throw sdkError("Core configuration values must be safe integers");
    }
  }
  if (commandTimeoutMs < 0 || commandTimeoutMs > MAX_TIMEOUT_MS || rpcTimeoutMs < 0 || rpcTimeoutMs > MAX_TIMEOUT_MS) {
    throw sdkError("Core timeouts are outside the V1 range");
  }
  if (queueLimit < 1 || queueLimit > 65_536) {
    throw sdkError("Core queueLimit is outside the V1 range");
  }
  if (payloadLimit < 1 || payloadLimit > MAXIMUM_PAYLOAD_BYTES) {
    throw sdkError("Core payloadLimit is outside the V1 range");
  }
  return {
    abi_version: 1,
    command_timeout_ms: commandTimeoutMs,
    rpc_timeout_ms: rpcTimeoutMs,
    queue_limit: queueLimit,
    payload_limit: payloadLimit,
  };
}

export function parseAdmission(value: unknown): Admission {
  const document = v1Document(value, ["abi_version", "command_id", "accepted"], ["command_handle", "error"]);
  const commandId = stringField(document, "command_id");
  const accepted = booleanField(document, "accepted");
  const commandHandle = document.command_handle ?? "";
  if (typeof commandHandle !== "string") {
    throw sdkError("The native core returned an invalid admission");
  }
  const error = document.error === undefined ? null : parseErrorDetails(document.error);
  if (accepted !== (error === null) || accepted !== Boolean(commandHandle)) {
    throw sdkError("The native core returned an inconsistent admission");
  }
  return { commandId, commandHandle, accepted, error };
}

export function parseCompletion(value: unknown): Completion {
  const document = v1Document(value, ["abi_version", "command_id", "ok"], ["result_type", "result", "error"]);
  const commandId = stringField(document, "command_id");
  const ok = booleanField(document, "ok");
  const error = document.error === undefined ? null : parseErrorDetails(document.error);
  if (ok) {
    if (
      typeof document.result_type !== "string" ||
      !RESULT_TYPES.has(document.result_type) ||
      !isJsonObject(document.result) ||
      error !== null
    ) {
      throw sdkError("The native core returned an inconsistent completion");
    }
    return {
      commandId,
      ok: true,
      resultType: document.result_type,
      error: null,
    };
  }
  if (document.result_type !== undefined || document.result !== undefined || error === null) {
    throw sdkError("The native core returned an inconsistent completion");
  }
  return { commandId, ok: false, resultType: null, error };
}

export function parseEvent(value: unknown): CoreEvent {
  if (!isRecord(value) || !hasExactKeys(value, ["abi_version", "event_id", "event_name", "created_at", "payload"])) {
    throw sdkError("The native core returned an invalid event");
  }
  if (value.abi_version !== 1 || !isJsonObject(value.payload)) {
    throw sdkError("The native core returned an invalid event");
  }
  const eventName = stringField(value, "event_name");
  if (!EVENT_NAMES.has(eventName)) {
    throw sdkError("The native core returned an unknown event type");
  }
  const createdAt = stringField(value, "created_at");
  if (!isCanonicalTimestamp(createdAt)) {
    throw sdkError("The native core returned an invalid event timestamp");
  }
  return {
    eventId: stringField(value, "event_id"),
    eventName,
    createdAt,
  };
}

export function isCommandName(value: unknown): value is CommandName {
  return typeof value === "string" && COMMAND_NAME_SET.has(value);
}

export function parseStatus(value: unknown): CoreStatus {
  if (!isRecord(value) || !hasExactKeys(value, ["abi_version", "status"]) || value.abi_version !== 1 || !isRecord(value.status)) {
    throw sdkError("The native core returned an invalid status");
  }
  const status = value.status;
  if (
    !hasExactKeys(status, [
      "lifecycle",
      "connectivity",
      "personality",
      "agent_id",
      "mesh_id",
      "mesh_endpoint",
      "queued_message_count",
    ]) ||
    !Number.isSafeInteger(status.queued_message_count) ||
    (status.queued_message_count as number) < 0
  ) {
    throw sdkError("The native core returned an invalid status");
  }
  const lifecycle = stringField(status, "lifecycle");
  const connectivity = stringField(status, "connectivity");
  const personality = stringField(status, "personality");
  if (!LIFECYCLES.has(lifecycle) || !CONNECTIVITY_STATES.has(connectivity) || !PERSONALITIES.has(personality)) {
    throw sdkError("The native core returned an unknown status value");
  }
  return {
    lifecycle,
    connectivity,
    personality,
    agentId: stringField(status, "agent_id"),
    meshId: stringField(status, "mesh_id"),
    meshEndpoint: stringField(status, "mesh_endpoint"),
    queuedMessageCount: status.queued_message_count as number,
  };
}

function v1Document(value: unknown, required: readonly string[], optional: readonly string[]): Record<string, unknown> {
  if (!isRecord(value) || !hasAllowedKeys(value, required, optional)) {
    throw sdkError("The native core returned an invalid V1 document");
  }
  if (value.abi_version !== 1) {
    throw sdkError("The native core returned an unsupported ABI version", "unsupported_version");
  }
  return value;
}

function stringField(value: Record<string, unknown>, key: string): string {
  const field = value[key];
  if (typeof field !== "string") {
    throw sdkError("The native core returned an invalid V1 document");
  }
  return field;
}

function booleanField(value: Record<string, unknown>, key: string): boolean {
  const field = value[key];
  if (typeof field !== "boolean") {
    throw sdkError("The native core returned an invalid V1 document");
  }
  return field;
}

function isJsonObject(value: unknown): value is JsonObject {
  return isRecord(value);
}

function isCanonicalTimestamp(value: string): boolean {
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{1,9})?Z$/.exec(value);
  if (match === null) {
    return false;
  }
  const [, yearValue, monthValue, dayValue, hourValue, minuteValue, secondValue] = match;
  const year = Number(yearValue);
  const month = Number(monthValue);
  const day = Number(dayValue);
  const hour = Number(hourValue);
  const minute = Number(minuteValue);
  const second = Number(secondValue);
  if (
    !Number.isSafeInteger(year) ||
    year < 1 ||
    month < 1 ||
    month > 12 ||
    hour > 23 ||
    minute > 59 ||
    second > 59
  ) {
    return false;
  }
  const daysInMonth = [31, isLeapYear(year) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1] ?? 0;
  return day >= 1 && day <= daysInMonth;
}

function isLeapYear(year: number): boolean {
  return year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
}
