# Manual local mesh

This directory contains the same small application behavior used by the deep
E2E scenario, expressed as ordinary customer applications. There are no test
counters, assertions, readiness files, fault controls, fixed identities, or
automatic mesh provisioning.

Both servers expose `/reverse`:

- `native_server.py` returns `[reversed_text, "native"]` through `session.on`.
- `fastapi_server.py` is an ordinary FastAPI application with no Cynapsa import
  and returns `[reversed_text, "fastapi"]`.

The four clients demonstrate synchronous and asynchronous native SDK calls,
Requests, and HTTPX. Each prompts for messages, sends every message to both
servers, and prints both responses. Enter `quit` to stop a client. Both servers
print every received message before replying.

## Start the platform

The local platform runs the current Management backend and portal, Enrollment,
the dedicated ejabberd repository, PostgreSQL, and the local Auth0 emulator. It
does not create a tenant, mesh, agent, membership, or SDK process.

```sh
./examples/manual_mesh/platform/start.sh
```

Open <http://localhost:5173/local-login.html>. The local-login page requests a
fresh short-lived development portal session and takes a first-time user to the
organization setup screen. After the organization slug is claimed, the local
portal refreshes that session and opens the dashboard without using the normal
browser Auth0 flow. The helper is bound to localhost and is for this disposable
local environment only. It also refreshes the local session automatically if a
Management token expires while the portal is open.

In the portal:

1. Create the organization/environment requested by the setup screen.
2. Create one mesh.
3. Create the agents you want to run.
4. Add those agents to the mesh.
5. Issue or copy one reusable enrollment token for each agent. A token is shown
   only when it is issued, so save it immediately.

The Auth0 emulator stores users only in memory. Treat this as one disposable
manual session; rebuilding or restarting Auth0 invalidates its provisioned
users even though PostgreSQL and ejabberd volumes are persistent.

## Prepare agent inputs

Copy the environment template and replace its values with the authoritative
mesh ID (`ejabberd_group_id`, not the database UUID) and the two server JIDs
returned during provisioning:

```sh
cp examples/manual_mesh/platform/agents.env.example \
  examples/manual_mesh/platform/.runtime/agents.env
```

Store each enrollment token in a private file whose directory name is the
label passed to `run-agent.sh`:

```sh
mkdir -p examples/manual_mesh/platform/.runtime/agents/fastapi-server
printf '%s\n' 'cpsa_e1.REPLACE_WITH_PORTAL_TOKEN' \
  > examples/manual_mesh/platform/.runtime/agents/fastapi-server/token
chmod 600 examples/manual_mesh/platform/.runtime/agents/fastapi-server/token
```

Use a different label, token file, and automatically-created Core state volume
for each independently provisioned agent. After first enrollment the token may
be removed; later starts use the installation credential in that agent's
volume.

## Run agents yourself

Start the native server in the first terminal and the ordinary FastAPI server
in the second:

```sh
./examples/manual_mesh/platform/run-agent.sh native-server native-server
./examples/manual_mesh/platform/run-agent.sh fastapi-server fastapi-server
```

Start an ordinary interactive Requests client in the third terminal:

```sh
./examples/manual_mesh/platform/run-agent.sh requests-client clienta
```

Type a message at the client prompt. It sends the same content to both servers;
each server logs the content and the client prints both reversed responses.

The helper builds the current SDK and Go Core into a local agent image, joins
the running platform network, mounts the local CA, and preserves one Core state
volume per label. The application source remains the ordinary Python visible in
this directory. HTTP applications receive authentication, address mappings,
and local allow rules from `cynapsa run` rather than importing Cynapsa.

Native variants are also available:

```sh
./examples/manual_mesh/platform/run-agent.sh native-sync-client native-sync-client
./examples/manual_mesh/platform/run-agent.sh native-async-client native-async-client
./examples/manual_mesh/platform/run-agent.sh httpx-client httpx-client
```

The current native SDK does not yet expose the CLI's high-level `--allow`
configuration surface. These native files deliberately avoid private SDK
objects so they remain representative customer code; with Core's empty
fail-closed local application policy, native traffic requires the forthcoming
public policy configuration API. The FastAPI/Requests/HTTPX path is runnable
now because the CLI installs its explicitly supplied allow rule.

## Operations

```sh
# Follow platform logs
docker compose \
  --env-file examples/manual_mesh/platform/.runtime/platform.env \
  -f examples/manual_mesh/platform/compose.yaml logs -f

# Stop while preserving PostgreSQL and ejabberd volumes
./examples/manual_mesh/platform/stop.sh
```

To fully reset the disposable environment, stop it, remove the
`cynapsa-local-platform` Compose volumes, and remove
`examples/manual_mesh/platform/.runtime`.
