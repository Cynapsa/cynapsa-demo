import assert from "node:assert/strict";
import test from "node:test";

import { CynapsaError, parseErrorDetails } from "../src/errors.js";
import { MAXIMUM_PAYLOAD_BYTES, configDocument, parseAdmission, parseCompletion, parseEvent, parseStatus } from "../src/models.js";
import { CynapsaClient } from "../src/client.js";

test("config encodes the exact V1 shape", () => {
  assert.deepEqual(configDocument({ queueLimit: 4, payloadLimit: 1024 }), {
    abi_version: 1,
    command_timeout_ms: 0,
    rpc_timeout_ms: 0,
    queue_limit: 4,
    payload_limit: 1024,
  });
});

test("admission rejects unknown fields", () => {
  assert.throws(
    () =>
      parseAdmission({
        abi_version: 1,
        command_id: "command-1",
        command_handle: "handle-1",
        accepted: true,
        extra: "not-allowed",
      }),
    CynapsaError,
  );
});

test("completion and status parse public shapes", () => {
  const completion = parseCompletion({
    abi_version: 1,
    command_id: "command-1",
    ok: true,
    result_type: "empty",
    result: {},
  });
  assert.equal(completion.ok, true);

  const status = parseStatus({
    abi_version: 1,
    status: {
      lifecycle: "created",
      connectivity: "unknown",
      personality: "unset",
      agent_id: "",
      mesh_id: "",
      mesh_endpoint: "",
      queued_message_count: 0,
    },
  });
  assert.equal(status.lifecycle, "created");
});

test("unmodeled nested fields are not exposed", () => {
  const completion = parseCompletion({
    abi_version: 1,
    command_id: "command-1",
    ok: true,
    result_type: "empty",
    result: { private_field: "dropped" },
  });
  const event = parseEvent({
    abi_version: 1,
    event_id: "event-1",
    event_name: "command.queue_full",
    created_at: "1970-01-01T00:00:01Z",
    payload: { private_field: "dropped" },
  });
  assert.equal(Object.hasOwn(completion, "result"), false);
  assert.equal(Object.hasOwn(event, "payload"), false);
});

test("noncanonical error messages are rejected", () => {
  assert.throws(
    () =>
      parseErrorDetails({
        code: "core_error",
        message: "Unnormalized failure",
        retryable: false,
        stage: "command",
        local_or_remote: "local",
      }),
    CynapsaError,
  );
});

test("retired delivery contract values are rejected", () => {
  for (const eventName of ["delivery.ordering_blocked", "delivery.unknown"]) {
    assert.throws(
      () =>
        parseEvent({
          abi_version: 1,
          event_id: "event-1",
          event_name: eventName,
          created_at: "1970-01-01T00:00:01Z",
          payload: {},
        }),
      CynapsaError,
    );
  }
  assert.throws(
    () =>
      parseErrorDetails({
        code: "core_error",
        message: "The AZTM core could not complete the operation",
        retryable: false,
        stage: "ordering",
        local_or_remote: "local",
      }),
    CynapsaError,
  );
});

test("config rejects timeout above the native range", () => {
  assert.throws(() => configDocument({ commandTimeoutMs: 9_223_372_036_855 }), CynapsaError);
});

test("config payload limit uses exact V1 boundary", () => {
  assert.equal(configDocument({ payloadLimit: MAXIMUM_PAYLOAD_BYTES }).payload_limit, MAXIMUM_PAYLOAD_BYTES);
  assert.throws(() => configDocument({ payloadLimit: MAXIMUM_PAYLOAD_BYTES + 1 }), CynapsaError);
});

test("event timestamps reject non-calendar dates", () => {
  assert.throws(
    () =>
      parseEvent({
        abi_version: 1,
        event_id: "event-1",
        event_name: "command.queue_full",
        created_at: "2026-02-31T00:00:00Z",
        payload: {},
      }),
    CynapsaError,
  );
});

test("close preserves shutdown failure after destroy", async () => {
  const failure = new CynapsaError({
    code: "shutdown_timeout",
    message: "Shutdown did not finish before the deadline",
    retryable: false,
    stage: "shutdown",
    location: "local",
  });
  const fake = {
    state: "started",
    native: {
      shutdown: async () => Promise.reject(failure),
      destroy: () => undefined,
    },
  };
  await assert.rejects(CynapsaClient.prototype.close.call(fake), (error: unknown) => error === failure);
  assert.equal(fake.state, "closed");
});
