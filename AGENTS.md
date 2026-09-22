# Demo entity runbook for agents

This file is the operational contract for running the three entities in this
repository. Do not put real enrollment tokens or API keys in tracked files,
Dockerfiles, images, commits, logs, or documentation.

## Topology

Start server identities before the interactive client:

```text
client --Cynapsa RPC /ask--> orchestrator --Cynapsa RPC /maps--> maps
                                                                   |
                                                                   +--> Google Places API
```

Every directory is independently runnable. Its `run.sh` downloads and verifies
the published ARM64 or AMD64 container image when that image is not already
available locally. The image contains the application, Cynapsa Python SDK, Go
Core, and Python dependencies. Credentials and installation state are supplied
at runtime and are not part of the image.

## Prerequisites

- Git
- Docker Desktop or a compatible Docker Engine, already running
- All three Cynapsa identities must belong to the same mesh
- A fresh enrollment token for every new local installation
- A Gemini API key for the maps and orchestrator containers
- A Google Maps API key with Places API access for the maps container

Enrollment tokens are one-time credentials. After successful enrollment, each
identity uses the encrypted profile in its Docker volume and no longer needs the
token.

Do not configure an entity's own agent ID. Enrollment resolves that identity
and Go Core stores it in the profile. `DEMO_ORCHESTRATOR_AGENT_ID` and
`DEMO_MAPS_AGENT_ID` are destination IDs used by the application for routing;
they are not local identity overrides.

## Choose a run mode

### Client against the deployed demo

Only run the client. Configure its mesh ID and the deployed orchestrator's bare
agent ID together with the client enrollment token.

### All three entities locally

Use separate local Cynapsa installations. Do not start a local server with the
same installation profile while its deployed Cloud Run worker is active.

For isolated local testing, create or select server identities in one mesh and
generate fresh enrollment tokens. Start maps first. Copy its printed bare agent
ID into the orchestrator's `DEMO_MAPS_AGENT_ID`. Start the orchestrator and copy
its printed bare agent ID into the client's `DEMO_ORCHESTRATOR_AGENT_ID`.

## 1. Run the maps agent

From the repository root:

```sh
cd maps
test -f .env || cp .env.example .env
```

Set these values in `maps/.env`:

```dotenv
CYNAPSA_TOKEN=replace-with-maps-enrollment-token
DEMO_MESH_ID=replace-with-mesh-id
GEMINI_API_KEY=replace-with-gemini-api-key
GOOGLE_MAPS_API_KEY=replace-with-google-maps-api-key
```

`DEMO_GEMINI_MODEL` is an optional override. Start the agent:

```sh
./run.sh
```

Successful startup prints:

```text
demo-maps ready as <agent-id>@connect.cynapsa.com
```

After the first successful enrollment, remove `CYNAPSA_TOKEN` from `.env`. Keep
both API keys available for every run.

Default state volume: `cynapsa-demo-maps-state`.

## 2. Run the orchestrator

In another terminal, from the repository root:

```sh
cd orchestrator
test -f .env || cp .env.example .env
```

Set these values in `orchestrator/.env`:

```dotenv
CYNAPSA_TOKEN=replace-with-orchestrator-enrollment-token
DEMO_MESH_ID=replace-with-mesh-id
DEMO_MAPS_AGENT_ID=agent-id@connect.cynapsa.com
GEMINI_API_KEY=replace-with-gemini-api-key
```

`DEMO_GEMINI_MODEL` is an optional override. Start the agent:

```sh
./run.sh
```

Successful startup prints:

```text
demo-orchestrator ready as <agent-id>@connect.cynapsa.com
```

After the first successful enrollment, remove `CYNAPSA_TOKEN` from `.env`. Keep
`GEMINI_API_KEY` available for every run.

Default state volume: `cynapsa-demo-orchestrator-state`.

## 3. Run the client

In another terminal, from the repository root:

```sh
cd client
test -f .env || cp .env.example .env
```

Set this value in `client/.env`:

```dotenv
CYNAPSA_TOKEN=replace-with-client-enrollment-token
DEMO_MESH_ID=replace-with-mesh-id
DEMO_ORCHESTRATOR_AGENT_ID=agent-id@connect.cynapsa.com
```

Start the client:

```sh
./run.sh
```

Successful startup prints the client identity and an interactive prompt:

```text
demo-client ready as <agent-id>@connect.cynapsa.com
Ask a question (or "quit"):
```

Ask a location question to exercise the complete path. Enter `quit` to stop the
client. After successful enrollment, remove `CYNAPSA_TOKEN` from `.env`.

Default state volume: `cynapsa-demo-client-state`.

## Later runs and image updates

Run an already enrolled identity from its directory:

```sh
./run.sh
```

Force a fresh download of its published image:

```sh
./run.sh --pull
```

Maintainers with local SDK and Go Core checkouts can build instead:

```sh
./run.sh --build
```

## Test another enrollment token

A saved profile takes precedence over a new token. To test another identity or
installation without destroying existing state, use a different volume name:

```sh
CYNAPSA_TOKEN='another-token' \
  CYNAPSA_DEMO_CLIENT_VOLUME=cynapsa-demo-client-test-2 \
  ./client/run.sh
```

Equivalent volume variables are:

- `CYNAPSA_DEMO_CLIENT_VOLUME`
- `CYNAPSA_DEMO_ORCHESTRATOR_VOLUME`
- `CYNAPSA_DEMO_MAPS_VOLUME`

Reuse the same volume name on later runs for that installation.

## Stop and reset

Press `Ctrl-C` to stop an entity. The container is removed automatically, but
its encrypted Cynapsa installation remains in the named Docker volume.

Deleting a state volume permanently removes that local installation state. For
example:

```sh
docker volume rm cynapsa-demo-client-state
```

Do this only when the installation should be discarded. A subsequent run needs
a fresh enrollment token.

## Expected failure signals

- `Docker is required`: install Docker.
- `Docker is not running`: start the Docker engine.
- `an enrollment token is required`: the selected volume has no profile and
  `CYNAPSA_TOKEN` was not supplied.
- Enrollment rejected: obtain a fresh token for the intended identity and mesh.
- Unauthorized RPC: confirm every identity belongs to the same mesh and that
  target IDs are canonical bare agent IDs.
- Model or Places errors: confirm the runtime API-key variables are present and
  the required Google APIs are enabled.
