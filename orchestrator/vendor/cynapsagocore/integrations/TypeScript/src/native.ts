import { realpathSync, statSync } from "node:fs";
import { isAbsolute } from "node:path";

import koffi from "koffi";

import { CynapsaError, errorFromDocument, sdkError } from "./errors.js";

const OK = 0;
const ERROR = 1;
const WAIT_TIMEOUT = 2;
const MAX_BUFFER_BYTES = 64 << 20;

type NativeInteger = number | bigint;

interface BufferDescriptor {
  buffer_handle?: NativeInteger;
  byte_length?: NativeInteger;
}

type NativeFunction = ((...arguments_: unknown[]) => number) & {
  async: (...arguments_: unknown[]) => void;
};

interface NativeFunctions {
  abiVersion: NativeFunction;
  coreCreate: NativeFunction;
  coreStart: NativeFunction;
  coreSubmit: NativeFunction;
  coreCancel: NativeFunction;
  coreNextCompletion: NativeFunction;
  coreNextEvent: NativeFunction;
  coreStatus: NativeFunction;
  coreShutdown: NativeFunction;
  coreDestroy: NativeFunction;
  bufferRead: NativeFunction;
  bufferFree: NativeFunction;
}

koffi.struct("cynapsa_buffer_desc_v1", {
  buffer_handle: "uint64_t",
  byte_length: "uint64_t",
});

export class NativeCore {
  readonly abiVersion: number;
  private readonly functions: NativeFunctions;
  private handle = 0n;
  private destroyed = false;

  constructor(libraryPath: string, config: Buffer) {
    const path = absoluteLibraryPath(libraryPath);
    let library: ReturnType<typeof koffi.load>;
    try {
      library = koffi.load(path);
    } catch {
      throw sdkError("The Cynapsa Core library could not be loaded");
    }
    try {
      this.functions = bindFunctions(library);
    } catch {
      throw sdkError("The Cynapsa Core library is missing a required V1 ABI function");
    }

    const version: number[] = [0];
    const versionError: BufferDescriptor = {};
    this.check(this.functions.abiVersion(version, versionError), versionError);
    this.abiVersion = version[0] ?? 0;
    if (this.abiVersion !== 1) {
      throw sdkError("The Cynapsa Core library uses an unsupported ABI version", "unsupported_version");
    }

    const output: NativeInteger[] = [0n];
    const error: BufferDescriptor = {};
    this.check(this.functions.coreCreate(config, BigInt(config.length), output, error), error);
    this.handle = toBigInt(output[0]);
    if (this.handle === 0n) {
      throw sdkError("The Cynapsa Core library returned an invalid core handle");
    }
  }

  start(): void {
    this.mutation(this.functions.coreStart, this.handle);
  }

  submit(document: Buffer): unknown {
    return this.jsonInputResult(this.functions.coreSubmit, document);
  }

  cancel(document: Buffer): void {
    this.jsonInputMutation(this.functions.coreCancel, document);
  }

  async nextCompletion(timeoutMs: number): Promise<unknown | null> {
    return this.waitResult(this.functions.coreNextCompletion, timeoutMs);
  }

  async nextEvent(timeoutMs: number): Promise<unknown | null> {
    return this.waitResult(this.functions.coreNextEvent, timeoutMs);
  }

  status(): unknown {
    const result: BufferDescriptor = {};
    const error: BufferDescriptor = {};
    this.check(this.functions.coreStatus(this.handle, result, error), error);
    return this.readJson(result);
  }

  async shutdown(timeoutMs: number): Promise<void> {
    const error: BufferDescriptor = {};
    let finished = false;
    const shutdown = callAsync(this.functions.coreShutdown, [this.handle, BigInt(timeoutMs), error]).finally(() => {
      finished = true;
    });
    while (!finished) {
      try {
        await this.waitResult(this.functions.coreNextEvent, 50);
      } catch {
        if (!finished) {
          await new Promise((resolve) => setTimeout(resolve, 10));
        }
      }
    }
    const status = await shutdown;
    this.check(status, error);
  }

  destroy(): void {
    if (this.destroyed) {
      return;
    }
    this.mutation(this.functions.coreDestroy, this.handle);
    this.destroyed = true;
    this.handle = 0n;
  }

  private jsonInputResult(nativeFunction: NativeFunction, document: Buffer): unknown {
    const result: BufferDescriptor = {};
    const error: BufferDescriptor = {};
    const status = nativeFunction(this.handle, document, BigInt(document.length), result, error);
    this.check(status, error);
    return this.readJson(result);
  }

  private jsonInputMutation(nativeFunction: NativeFunction, document: Buffer): void {
    const error: BufferDescriptor = {};
    this.check(nativeFunction(this.handle, document, BigInt(document.length), error), error);
  }

  private async waitResult(nativeFunction: NativeFunction, timeoutMs: number): Promise<unknown | null> {
    const result: BufferDescriptor = {};
    const error: BufferDescriptor = {};
    const status = await callAsync(nativeFunction, [this.handle, BigInt(timeoutMs), result, error]);
    if (status === WAIT_TIMEOUT) {
      if (descriptorHandle(result) !== 0n || descriptorLength(result) !== 0n || descriptorHandle(error) !== 0n || descriptorLength(error) !== 0n) {
        throw sdkError("The Cynapsa Core library returned an invalid wait result");
      }
      return null;
    }
    this.check(status, error);
    return this.readJson(result);
  }

  private mutation(nativeFunction: NativeFunction, ...arguments_: unknown[]): void {
    const error: BufferDescriptor = {};
    this.check(nativeFunction(...arguments_, error), error);
  }

  private check(status: number, error: BufferDescriptor): void {
    if (status === OK) {
      if (descriptorHandle(error) !== 0n || descriptorLength(error) !== 0n) {
        this.discard(error);
        throw sdkError("The Cynapsa Core library returned an invalid success result");
      }
      return;
    }
    if (status === ERROR && descriptorHandle(error) !== 0n) {
      let document: unknown;
      try {
        document = JSON.parse(this.copyAndFree(error).toString("utf8"));
      } catch {
        throw sdkError("The native core returned an invalid error document");
      }
      throw errorFromDocument(document);
    }
    this.discard(error);
    throw sdkError("The Cynapsa Core library returned an invalid ABI status");
  }

  private readJson(descriptor: BufferDescriptor): unknown {
    try {
      return JSON.parse(this.copyAndFree(descriptor).toString("utf8"));
    } catch (error) {
      if (error instanceof CynapsaError) {
        throw error;
      }
      throw sdkError("The native core returned an invalid V1 document");
    }
  }

  private copyAndFree(descriptor: BufferDescriptor): Buffer {
    const handle = descriptorHandle(descriptor);
    const length = descriptorLength(descriptor);
    if (handle === 0n || length <= 0n || length > BigInt(MAX_BUFFER_BYTES)) {
      this.discard(descriptor);
      throw sdkError("The Cynapsa Core library returned an invalid buffer descriptor");
    }
    const size = Number(length);
    const destination = Buffer.alloc(size);
    const copied: NativeInteger[] = [0n];
    const readError: BufferDescriptor = {};
    const readStatus = this.functions.bufferRead(handle, 0n, destination, BigInt(size), copied, readError);
    if (readStatus !== OK || toBigInt(copied[0]) !== length) {
      this.discard(readError);
      this.discard(descriptor);
      throw sdkError("The Cynapsa Core library could not copy an ABI buffer");
    }
    const freeError: BufferDescriptor = {};
    const freeStatus = this.functions.bufferFree(handle, freeError);
    if (freeStatus !== OK) {
      this.discard(freeError);
      throw sdkError("The Cynapsa Core library could not retire an ABI buffer");
    }
    return destination;
  }

  private discard(descriptor: BufferDescriptor): void {
    const handle = descriptorHandle(descriptor);
    if (handle === 0n) {
      return;
    }
    const nested: BufferDescriptor = {};
    this.functions.bufferFree(handle, nested);
    const nestedHandle = descriptorHandle(nested);
    if (nestedHandle !== 0n && nestedHandle !== handle) {
      this.functions.bufferFree(nestedHandle, {});
    }
  }
}

function bindFunctions(library: ReturnType<typeof koffi.load>): NativeFunctions {
  const bind = (prototype: string): NativeFunction => library.func(prototype) as unknown as NativeFunction;
  return {
    abiVersion: bind("int32_t cynapsa_v1_abi_version(_Out_ uint32_t *out_version, _Out_ cynapsa_buffer_desc_v1 *out_error)"),
    coreCreate: bind("int32_t cynapsa_v1_core_create(const uint8_t *input, uint64_t input_length, _Out_ uint64_t *out_core, _Out_ cynapsa_buffer_desc_v1 *out_error)"),
    coreStart: bind("int32_t cynapsa_v1_core_start(uint64_t core, _Out_ cynapsa_buffer_desc_v1 *out_error)"),
    coreSubmit: bind("int32_t cynapsa_v1_core_submit(uint64_t core, const uint8_t *input, uint64_t input_length, _Out_ cynapsa_buffer_desc_v1 *out_result, _Out_ cynapsa_buffer_desc_v1 *out_error)"),
    coreCancel: bind("int32_t cynapsa_v1_core_cancel(uint64_t core, const uint8_t *input, uint64_t input_length, _Out_ cynapsa_buffer_desc_v1 *out_error)"),
    coreNextCompletion: bind("int32_t cynapsa_v1_core_next_completion(uint64_t core, int64_t timeout_ms, _Out_ cynapsa_buffer_desc_v1 *out_result, _Out_ cynapsa_buffer_desc_v1 *out_error)"),
    coreNextEvent: bind("int32_t cynapsa_v1_core_next_event(uint64_t core, int64_t timeout_ms, _Out_ cynapsa_buffer_desc_v1 *out_result, _Out_ cynapsa_buffer_desc_v1 *out_error)"),
    coreStatus: bind("int32_t cynapsa_v1_core_status(uint64_t core, _Out_ cynapsa_buffer_desc_v1 *out_result, _Out_ cynapsa_buffer_desc_v1 *out_error)"),
    coreShutdown: bind("int32_t cynapsa_v1_core_shutdown(uint64_t core, int64_t timeout_ms, _Out_ cynapsa_buffer_desc_v1 *out_error)"),
    coreDestroy: bind("int32_t cynapsa_v1_core_destroy(uint64_t core, _Out_ cynapsa_buffer_desc_v1 *out_error)"),
    bufferRead: bind("int32_t cynapsa_v1_buffer_read(uint64_t buffer, uint64_t offset, uint8_t *destination, uint64_t destination_length, _Out_ uint64_t *out_copied, _Out_ cynapsa_buffer_desc_v1 *out_error)"),
    bufferFree: bind("int32_t cynapsa_v1_buffer_free(uint64_t buffer, _Out_ cynapsa_buffer_desc_v1 *out_error)"),
  };
}

function absoluteLibraryPath(value: string): string {
  if (!isAbsolute(value)) {
    throw sdkError("The Cynapsa Core library path must be absolute");
  }
  let path: string;
  try {
    path = realpathSync(value);
  } catch {
    throw sdkError("The Cynapsa Core library path does not exist");
  }
  try {
    if (!statSync(path).isFile()) {
      throw sdkError("The Cynapsa Core library path is not a file");
    }
  } catch (error) {
    if (error instanceof CynapsaError) {
      throw error;
    }
    throw sdkError("The Cynapsa Core library path is not a file");
  }
  return path;
}

function descriptorHandle(descriptor: BufferDescriptor): bigint {
  return toBigInt(descriptor.buffer_handle);
}

function descriptorLength(descriptor: BufferDescriptor): bigint {
  return toBigInt(descriptor.byte_length);
}

function toBigInt(value: NativeInteger | undefined): bigint {
  if (value === undefined) {
    return 0n;
  }
  return typeof value === "bigint" ? value : BigInt(value);
}

function callAsync(nativeFunction: NativeFunction, arguments_: unknown[]): Promise<number> {
  return new Promise((resolve, reject) => {
    const callback = (error: unknown, status: unknown): void => {
      if (error !== null && error !== undefined) {
        reject(sdkError("The Cynapsa Core library call could not be completed"));
        return;
      }
      if (typeof status !== "number") {
        reject(sdkError("The Cynapsa Core library returned an invalid ABI status"));
        return;
      }
      resolve(status);
    };
    try {
      nativeFunction.async(...arguments_, callback);
    } catch {
      reject(sdkError("The Cynapsa Core library call could not be completed"));
    }
  });
}
