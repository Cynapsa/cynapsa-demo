import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import test from "node:test";

import { COMMAND_NAMES, MAXIMUM_PAYLOAD_BYTES, configDocument, parseAdmission, parseCompletion, parseEvent, parseStatus } from "../src/models.js";
import { errorFromDocument } from "../src/errors.js";
import { CynapsaError } from "../src/errors.js";

interface Vector {
  readonly name: string;
  readonly json: unknown;
}

const vectorDirectory = resolve(process.cwd(), "../../conformance/v1");

function vectors(filename: string): Vector[] {
  return JSON.parse(readFileSync(resolve(vectorDirectory, filename), "utf8")) as Vector[];
}

function document(filename: string): Record<string, unknown> {
  return JSON.parse(readFileSync(resolve(vectorDirectory, filename), "utf8")) as Record<string, unknown>;
}

test("ABI and direct operation vectors match", () => {
  const abi = document("abi.json");
  const operations = document("operations.json");
  assert.equal(abi.abi_version, 1);
  assert.deepEqual(abi.status_values, { ok: 0, error: 1, wait_timeout: 2 });
  assert.equal((abi.exports as unknown[]).length, 21);
  assert.equal((operations.core_create as { maximum_payload_bytes: unknown }).maximum_payload_bytes, MAXIMUM_PAYLOAD_BYTES);
  const coreCreate = (operations.core_create as { json: unknown }).json;
  assert.deepEqual(configDocument({ queueLimit: 4, payloadLimit: 1_048_576 }), coreCreate);
  assert.equal(configDocument({ payloadLimit: MAXIMUM_PAYLOAD_BYTES }).payload_limit, MAXIMUM_PAYLOAD_BYTES);
  assert.throws(() => configDocument({ payloadLimit: MAXIMUM_PAYLOAD_BYTES + 1 }), CynapsaError);
  assert.equal(parseAdmission(operations.admission).accepted, true);
  assert.equal(parseCompletion(operations.failed_completion).ok, false);
  assert.equal(parseStatus(operations.status).lifecycle, "created");
  assert.equal(errorFromDocument(operations.normalized_error).details.code, "invalid_handle");
});

test("command catalog matches Go-owned conformance vectors", () => {
  const commands = vectors("commands.json");
  assert.deepEqual(COMMAND_NAMES, commands.map((command) => command.name));
});

test("all Go-owned result vectors parse", () => {
  for (const vector of vectors("results.json")) {
    assert.equal(parseCompletion(vector.json).resultType, vector.name);
  }
});

test("all Go-owned event vectors parse", () => {
  for (const vector of vectors("events.json")) {
    assert.equal(parseEvent(vector.json).eventName, vector.name);
  }
});

test("payload vectors use exactly one public variant", () => {
  const payloads = vectors("payloads.json");
  const variants = new Set(["native", "http_request", "http_response", "payload_handle"]);
  assert.deepEqual(new Set(payloads.map((vector) => vector.name)), new Set(["native", "http request", "http response", "snapshot handle"]));
  for (const vector of payloads) {
    const value = vector.json as Record<string, unknown>;
    const keys = Object.keys(value);
    assert.equal(keys.length, 1);
    assert.equal(variants.has(keys[0] ?? ""), true);
    const variant = keys[0];
    if (variant === "payload_handle") {
      assert.equal(typeof value[variant], "string");
      continue;
    }
    const body = (value[variant ?? ""] as { body: string }).body;
    assert.equal(Buffer.from(body, "base64").toString("base64"), body);
  }
});
