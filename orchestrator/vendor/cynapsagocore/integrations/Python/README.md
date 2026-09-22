# Cynapsa Python SDK scaffold

This project demonstrates a thin Python SDK over the Cynapsa V1 shared-library
ABI. The public package exposes typed application-level models and a client.
The `ctypes` loader remains private to the package.

The implemented scaffold covers library/version checks, core lifecycle,
status, command submission and cancellation, and polling for normalized
completions and events. A production SDK can add command-specific convenience
methods and the remaining V1 native operations without changing this boundary.

## Layout

```text
src/cynapsa/
  __init__.py       public exports
  client.py         public lifecycle and command API
  errors.py         normalized public exception
  models.py         versioned public result models
  _native.py        private shared-library calls and buffer ownership
examples/basic.py   minimal lifecycle and command flow
tests/              model and native smoke tests
```

## Run the example

Build the shared library as described in `../README.md`, then run:

```bash
cd integrations/Python
PYTHONPATH=src python3 examples/basic.py \
  /tmp/cynapsa-sdk-demo/libcynapsacore.so
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
PYTHONPATH=src python3 -m unittest discover -s tests -v
python3 -m compileall -q src examples tests
```

Set `CYNAPSA_CORE_LIBRARY` to an absolute shared-library path to enable the
native smoke test.

## Token authentication

After `client.start()`, submit `client.token_login(token, mesh_id, profile_id="replica-a")` for HTTP
Bridge personality or `client.token_connect(token, mesh_id)` for native
personality. Both accept optional `profile_id` and `command_id`. Reopen the
durable installation without the token with
`client.installation_login(profile_id, mesh_id)` or
`client.installation_connect(profile_id, mesh_id)`. The token must use the exact
`cpsa_e1.<uuid>.<43-character-secret>` shape. Core resolves the endpoint,
authenticated username, and short-lived credential through its private
enrollment service. Display metadata and returned credentials never cross the
public completion or status boundary.
