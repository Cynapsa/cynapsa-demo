import { randomUUID } from "node:crypto";

import { sdkError } from "./errors.js";
import {
  configDocument,
  isCommandName,
  parseAdmission,
  parseCompletion,
  parseEvent,
  parseStatus,
} from "./models.js";
import type {
  Admission,
  CommandName,
  Completion,
  CoreConfig,
  CoreEvent,
  CoreStatus,
  JsonObject,
} from "./models.js";
import { NativeCore } from "./native.js";

type ClientState = "created" | "started" | "closing" | "shutdown" | "closed";
const MAX_TIMEOUT_MS = 9_223_372_036_854;

export class CynapsaClient {
  readonly sdkSessionId: string;
  private readonly native: NativeCore;
  private state: ClientState = "created";

  constructor(options: { libraryPath?: string; config?: CoreConfig; sdkSessionId?: string } = {}) {
    const libraryPath = options.libraryPath ?? process.env.CYNAPSA_CORE_LIBRARY;
    if (!libraryPath) {
      throw sdkError("An absolute Cynapsa Core library path is required");
    }
    this.sdkSessionId = options.sdkSessionId ?? `session-${randomUUID()}`;
    validateIdentifier(this.sdkSessionId, "sdkSessionId");
    this.native = new NativeCore(libraryPath, encode(configDocument(options.config)));
  }

  get abiVersion(): number {
    return this.native.abiVersion;
  }

  start(): void {
    this.requireState("created");
    this.native.start();
    this.state = "started";
  }

  initialize(commandId = `command-${randomUUID()}`): Admission {
    return this.submitCommand("core.init", {}, commandId);
  }

  requestCapabilities(commandId = `command-${randomUUID()}`): Admission {
    return this.submitCommand("core.capabilities", {}, commandId);
  }

  requestStatus(commandId = `command-${randomUUID()}`): Admission {
    return this.submitCommand("core.status", {}, commandId);
  }

  /** Enroll and authenticate for HTTP Bridge mode; await its completion. */
  tokenLogin(token: string, meshId: string, options: { commandId?: string; profileId?: string } = {}): Admission {
    const args: JsonObject = { token, mesh_id: meshId };
    if (options.profileId !== undefined) args.profile_id = options.profileId;
    return this.submitCommand("auth.token_login", args, options.commandId ?? `command-${randomUUID()}`);
  }

  /** Enroll and authenticate for native mode; await its completion. */
  tokenConnect(token: string, meshId: string, options: { commandId?: string; profileId?: string } = {}): Admission {
    const args: JsonObject = { token, mesh_id: meshId };
    if (options.profileId !== undefined) args.profile_id = options.profileId;
    return this.submitCommand("auth.token_connect", args, options.commandId ?? `command-${randomUUID()}`);
  }

  /** Authenticate an enrolled local installation for HTTP Bridge mode. */
  installationLogin(profileId: string, meshId: string, options: { commandId?: string } = {}): Admission {
    return this.submitCommand("auth.installation_login", { profile_id: profileId, mesh_id: meshId }, options.commandId ?? `command-${randomUUID()}`);
  }

  /** Authenticate an enrolled local installation for native mode. */
  installationConnect(profileId: string, meshId: string, options: { commandId?: string } = {}): Admission {
    return this.submitCommand("auth.installation_connect", { profile_id: profileId, mesh_id: meshId }, options.commandId ?? `command-${randomUUID()}`);
  }

  private submitCommand(commandName: CommandName, args: JsonObject, commandId: string): Admission {
    this.requireState("started");
    if (!isCommandName(commandName)) {
      throw sdkError("The SDK command name is not part of the V1 contract");
    }
    validateIdentifier(commandId, "commandId");
    return parseAdmission(
      this.native.submit(
        encode({
          abi_version: 1,
          command_id: commandId,
          command_name: commandName,
          sdk_session_id: this.sdkSessionId,
          args,
        }),
      ),
    );
  }

  cancel(commandHandle: string): void {
    this.requireState("started");
    this.native.cancel(encode({ abi_version: 1, handle: commandHandle }));
  }

  async nextCompletion(timeoutMs = 0): Promise<Completion | null> {
    this.requireState("started");
    validateTimeout(timeoutMs);
    const value = await this.native.nextCompletion(timeoutMs);
    return value === null ? null : parseCompletion(value);
  }

  async nextEvent(timeoutMs = 0): Promise<CoreEvent | null> {
    this.requireState("started");
    validateTimeout(timeoutMs);
    const value = await this.native.nextEvent(timeoutMs);
    return value === null ? null : parseEvent(value);
  }

  status(): CoreStatus {
    this.requireOpen();
    return parseStatus(this.native.status());
  }

  async shutdown(timeoutMs = 0): Promise<void> {
    if (this.state === "shutdown" || this.state === "closed") {
      return;
    }
    this.requireState("started");
    validateTimeout(timeoutMs);
    try {
      await this.native.shutdown(timeoutMs);
    } catch (error) {
      this.state = "closing";
      throw error;
    }
    this.state = "shutdown";
  }

  async close(timeoutMs = 0): Promise<void> {
    if (this.state === "closed") {
      return;
    }
    let shutdownError: unknown;
    if (this.state === "created" || this.state === "started" || this.state === "closing") {
      try {
        validateTimeout(timeoutMs);
        await this.native.shutdown(timeoutMs);
        this.state = "shutdown";
      } catch (error) {
        this.state = "closing";
        shutdownError = error;
      }
    }
    try {
      this.native.destroy();
      this.state = "closed";
    } catch (error) {
      if (shutdownError !== undefined) {
        throw new AggregateError([shutdownError, error], "Cynapsa client teardown failed");
      }
      throw error;
    }
    if (shutdownError !== undefined) {
      throw shutdownError;
    }
  }

  private requireState(expected: ClientState): void {
    if (this.state !== expected) {
      throw sdkError(`This operation requires a client in the ${expected} state`);
    }
  }

  private requireOpen(): void {
    if (this.state === "closed") {
      throw sdkError("This operation requires an open client");
    }
  }
}

function encode(document: unknown): Buffer {
  assertJsonValue(document);
  try {
    return Buffer.from(JSON.stringify(document), "utf8");
  } catch {
    throw sdkError("The SDK command contains a value that is not valid V1 JSON");
  }
}

function assertJsonValue(value: unknown): void {
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "string" ||
    (typeof value === "number" && Number.isFinite(value))
  ) {
    return;
  }
  if (Array.isArray(value)) {
    for (const item of value) {
      assertJsonValue(item);
    }
    return;
  }
  if (typeof value === "object" && value !== null && Object.getPrototypeOf(value) === Object.prototype) {
    for (const item of Object.values(value)) {
      if (item === undefined) {
        throw sdkError("The SDK command contains a value that is not valid V1 JSON");
      }
      assertJsonValue(item);
    }
    return;
  }
  throw sdkError("The SDK command contains a value that is not valid V1 JSON");
}

function validateTimeout(value: number): void {
  if (!Number.isSafeInteger(value) || value < 0 || value > MAX_TIMEOUT_MS) {
    throw sdkError("timeoutMs is outside the V1 range");
  }
}

function validateIdentifier(value: string, name: string): void {
  if (!value || Buffer.byteLength(value, "utf8") > 512) {
    throw sdkError(`${name} is outside the V1 range`);
  }
}
