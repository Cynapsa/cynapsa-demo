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

Every directory is independently runnable. Its `run.sh` builds a local
container image from that directory when the image is not already available.
The directory includes its own application, Cynapsa Python SDK source, Go Core
source, Docker build, and runtime scripts. Credentials and installation state
are supplied at runtime and are not part of the image.

## Prerequisites

- Git
- Docker Desktop or a compatible Docker Engine, already running
- All three Cynapsa identities must belong to the same mesh
- An active enrollment grant with capacity for each new local installation
- A LiteLLM gateway API key for the maps and orchestrator containers
- A Google Maps API key with Places API access for the maps container

Enrollment grants are reusable by default, but may have a mesh scope, expiry,
revocation, or installation limit. After successful enrollment, each identity
uses the encrypted installation profile in its Docker volume and no longer
needs the original token. Mesh removal revokes installations in that mesh,
not enrollment grants. After rejoining, the old profile cannot authenticate;
use `./run.sh --force-enroll` with a still-valid token to create a new
installation in the same state volume. Go Core handles profile replacement.

Do not configure an entity's own agent ID. Enrollment resolves that identity
and Go Core stores it in the profile. `DEMO_ORCHESTRATOR_AGENT_ID` and
`DEMO_MAPS_AGENT_ID` are destination IDs used by the application for routing;
they are not local identity overrides.

## Choose a run mode

### Client against deployed agents

Only run the client if both demo worker pools have been explicitly scaled back
up. They are currently disabled at zero instances. Configure the client's mesh
ID and the orchestrator's bare agent ID with the client enrollment token.

### All three entities locally

Use separate local Cynapsa installations. Do not start a local server with the
same installation profile while a deployed Cloud Run worker is active. Both
demo worker pools are currently disabled, but do not reuse their saved Cloud
Run profiles for local installs. Reuse an active grant only when its scope and
installation limit permit another installation.

For isolated local testing, create or select server identities in one mesh and
provide valid enrollment grants. Start maps first. Copy its printed bare agent
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
# LITELLM_API_KEY=replace-with-gateway-api-key
GOOGLE_MAPS_API_KEY=replace-with-google-maps-api-key
```

Put the gateway key in `maps/.private/litellm_api_key` (mode `0600`), or
uncomment `LITELLM_API_KEY` in `.env`. `LITELLM_BASE_URL` and `LITELLM_MODEL`
are optional overrides; defaults are `https://litellm.eladrave.com` and
`gpt-5.6-terra-high`. Start the agent:

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
# LITELLM_API_KEY=replace-with-gateway-api-key
```

Put the gateway key in `orchestrator/.private/litellm_api_key` (mode `0600`),
or uncomment `LITELLM_API_KEY` in `.env`. The same optional gateway overrides
apply here. Start the agent:

```sh
./run.sh
```

Successful startup prints:

```text
demo-orchestrator ready as <agent-id>@connect.cynapsa.com
```

After the first successful enrollment, remove `CYNAPSA_TOKEN` from `.env`. Keep
the gateway key available for every run.

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

## Later runs and image rebuilds

Run an already enrolled identity from its directory:

```sh
./run.sh
```

Force a rebuild from the SDK and Go Core bundled in the entity directory:

```sh
./run.sh --build
```

After an identity has been removed from its mesh and re-added, place its
still-valid enrollment token in that directory's `.env` and run
`./run.sh --force-enroll`. The entrypoint invokes the bundled
`cynapsa run --force-enroll` with the token from a private file and a no-op
target. After that CLI session closes, it starts the native agent from the
new saved profile in the same Docker volume; it does not switch volumes. Stop
the old container first. If a grant expired or was revoked independently,
obtain a new grant.
The bundled SDK includes force-enroll and native remote-error handling.
After changing bundled SDK/Core source, use `./run.sh --build` and restart
any running container; an existing container keeps its original image.

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
an active enrollment grant with capacity for a new installation.

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
