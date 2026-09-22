import assert from "node:assert/strict";
import test from "node:test";

import { CynapsaClient } from "../src/index.js";

const libraryPath = process.env.CYNAPSA_CORE_LIBRARY;

test("token methods reach native authentication without network", { skip: !libraryPath }, async () => {
  if (!libraryPath) return;
  for (const method of ["tokenLogin", "tokenConnect"] as const) {
    const client = new CynapsaClient({ libraryPath });
    try {
      client.start();
      assert.throws(
        () => client[method]("aztm_invalid.invalid", "mesh-one", { commandId: method }),
        (error: unknown) => !String(error).includes("aztm_invalid.invalid"),
      );
      assert.equal(client.status().lifecycle, "created");
    } finally {
      await client.close();
    }
  }
});

test("created core can close cleanly", { skip: !libraryPath }, async () => {
  if (!libraryPath) {
    return;
  }
  const client = new CynapsaClient({ libraryPath });
  assert.equal(client.status().lifecycle, "created");
  await client.close(5_000);
});

test("create, status, start, submit, complete, and close", { skip: !libraryPath }, async () => {
  if (!libraryPath) {
    return;
  }
  const client = new CynapsaClient({ libraryPath });
  try {
    assert.equal(client.abiVersion, 1);
    assert.equal(client.status().lifecycle, "created");
    client.start();
    const admission = client.initialize("typescript-smoke-init");
    assert.equal(admission.accepted, true);
    const completion = await client.nextCompletion(5_000);
    assert.equal(completion?.ok, true);
    assert.equal(completion?.commandId, "typescript-smoke-init");
  } finally {
    await client.close();
  }
});
