# Cynapsa TypeScript SDK scaffold

This project demonstrates a thin Node.js TypeScript SDK over the Cynapsa V1
shared-library ABI. The public package exposes typed application-level models
and a client. Koffi remains a private implementation dependency used only to
load the C ABI.

The implemented scaffold covers library/version checks, core lifecycle,
status, command submission and cancellation, and asynchronous polling for
normalized completions and events. A production SDK can add command-specific
convenience methods and the remaining V1 native operations without changing
this boundary.

## Layout

```text
src/
  index.ts          public exports
  client.ts         public lifecycle and command API
  errors.ts         normalized public exception
  models.ts         versioned public result models
  native.ts         private shared-library calls and buffer ownership
examples/basic.ts   minimal lifecycle and command flow
tests/              model and native smoke tests
```

## Install and build

```bash
cd integrations/TypeScript
npm ci
npm run build
```

## Run the example

Build the shared library as described in `../README.md`, then run:

```bash
npm run example -- /tmp/cynapsa-sdk-demo/libcynapsacore.so
```

The flow is:

1. load the native library from an explicit absolute path;
2. verify ABI version 1;
3. create and start one core;
4. submit a versioned public command;
5. copy and free the admission and completion buffers;
6. shut down and destroy the core.

## Validate the scaffold

```bash
npm test
```

Set `CYNAPSA_CORE_LIBRARY` to an absolute shared-library path to enable the
native smoke test.

## Token authentication

After `client.start()`, submit `client.tokenLogin(token, meshId, { profileId: "replica-a" })` for HTTP
Bridge personality or `client.tokenConnect(token, meshId)` for native
personality. Options accept `profileId` and `commandId`. Reopen the durable
installation without the token with `client.installationLogin(profileId,
meshId)` or `client.installationConnect(profileId, meshId)`. The token must use the exact
`cpsa_e1.<uuid>.<43-character-secret>` shape. Core resolves the endpoint,
authenticated username, and short-lived credential through its private
enrollment service. Display metadata and returned credentials never cross the
public completion or status boundary.
