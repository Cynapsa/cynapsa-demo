# Demo entity runbook for agents

This file is the operational contract for running the three entities in this
repository. Do not put real enrollment tokens or API keys in tracked files,
Dockerfiles, images, commits, logs, or documentation.

## Topology

This runbook describes the local working-tree implementation, not verified
GitHub heads or deployed cloud status. Builds clone remote SDK/Core branch
heads; unpushed local changes do not enter the image. At the 2026-09-26 audit,
the latest local-policy removal was not yet pushed in the SDK/Core.
Do not claim that a GitHub-built SDK contains it until its fetched
revision is verified. Enrollment is owned separately and remains read-only here.

Start server identities before the interactive client:

```text
client --Cynapsa RPC /ask--> orchestrator --Cynapsa RPC /maps--> maps
                                                                   |
                                                                   +--> Google Places API
```

Every directory is independently runnable. Its `run.sh` builds a local
container image from that directory when the image is not already available.
The directory includes its own application, Docker build, and runtime scripts.
During each image build, it fetches the `remove-snapshot` branches of the
Python SDK and Go Core from GitHub using `CYNAPSA_GITHUB_TOKEN` as a BuildKit
secret. Put that token in the entity's ignored `.env`; it needs read access to
both repositories and is never passed to the running container. Enrollment
tokens and installation state are supplied at runtime and are not part of the image.

## Prerequisites

- Git
- Docker Desktop or a compatible Docker Engine, already running
- A GitHub token with read access to both Cynapsa source repositories for builds
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

Only run the client if both demo worker pools have been independently verified
available. They were previously observed at zero instances; this audit did not
check current cloud status. Configure the client's mesh
ID and the orchestrator's bare agent ID with the client enrollment token.

### All three entities locally

Use separate local Cynapsa installations. Do not start a local server with the
same installation profile while a deployed Cloud Run worker is active. Both
demo worker pools were previously observed disabled, but do not reuse their saved Cloud
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
CYNAPSA_GITHUB_TOKEN=replace-with-github-token
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
CYNAPSA_GITHUB_TOKEN=replace-with-github-token
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
CYNAPSA_GITHUB_TOKEN=replace-with-github-token
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

Each `SOURCE_DEPENDENCIES.md` explains remote provenance, BuildKit secret
handling, image-tag differences, and why a local SDK fix may not be in a build.
Volume selection is explicit `CYNAPSA_DEMO_<ROLE>_VOLUME`, then an existing
ignored `.private/active-volume` pointer, then the role's default volume. The
runner reads but does not update that pointer; it does not switch volumes for
force-enrollment. Do not inspect or publish private state or secret contents.

Run an already enrolled identity from its directory:

```sh
./run.sh
```

Fetch the current SDK and Go Core branch heads and rebuild:

```sh
./run.sh --build
```

After an identity has been removed from its mesh and re-added, place its
still-valid enrollment token in that directory's `.env` and run
`./run.sh --force-enroll`. The entrypoint invokes the built-in
`cynapsa run --force-enroll` with the token from a private file and a no-op
target. After that CLI session closes, it starts the native agent from the
new saved profile in the same Docker volume; it does not switch volumes. Stop
the old container first. If a grant expired or was revoked independently,
obtain a new grant.
The fetched SDK includes force-enroll and native remote-error handling.
After the SDK/Core branch changes, use `./run.sh --build` and restart
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

## Application and delivery bounds

Maps uses at most three model rounds and ten tool calls per round. The first
round must call a tool; details IDs must come from a previous search. Invalid
tools return 502 `maps_tool_error`, unfinished answers `maps_no_answer`, Places
failure `places_unavailable`, and model failure 503 `model_unavailable`.
Orchestrator permits zero or one maps call plus a final completion. Its bad
input, timeout and model failures are 400/504/503 respectively; downstream
native/remote failures become generic 502 `orchestrator_failed`. Its terminal
logs include canonical downstream details and traces; keep them private.

Source: entity `app.py`, `llm.py`, `runtime.py`, and `maps/places.py`. Model and
Places HTTP timeouts (25/10 seconds) are not a total request deadline. Maps RPC
TTL is 100,000 ms, client `/ask` TTL 140,000 ms, and session RPC timeout option
110,000 ms; these distinct limits do not prove every allowed tool chain finishes.

Use canonical bare destination JIDs, not friendly aliases/portal UUIDs. The
authority resolves a full exact-session JID. No active target is unavailable;
mesh/organization mismatch is forbidden. Peer revoke application needs its
separate 30-second action ACK. Stream resume can replay an exact pending queue,
but final expiry has no mailbox/reroute and accepted sends are not custody
receipts. Local mock checks do not prove remote builds or live delivery.
