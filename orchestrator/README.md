# Demo orchestrator

This identity accepts `/ask` RPCs from the demo client. Gemini decides whether
to answer directly or call the maps identity over Cynapsa.

The directory is independently runnable and includes its own pinned Python SDK
and Go Core source. It does not use repository-root files, sibling directories,
or external SDK/Core checkouts.

## Run

```sh
cp .env.example .env
# Set every required value in .env.
./run.sh
```

The credentials are runtime environment variables and are never stored in the
image. Encrypted Cynapsa state persists in the
`cynapsa-demo-orchestrator-state` Docker volume. Remove `CYNAPSA_TOKEN` from
`.env` after enrollment; `GEMINI_API_KEY` is required on every run.
`DEMO_MESH_ID` and `DEMO_MAPS_AGENT_ID` are also required on every run.

Use `./run.sh --build` to force a rebuild from this directory. The image takes
`CYNAPSA_TOKEN` at container startup from the ignored `.env` or shell
environment only when its state volume has no saved profile. No token is baked
into the Dockerfile or image.

## Test another identity

```sh
CYNAPSA_TOKEN='another-enrollment-token' \
  CYNAPSA_DEMO_ORCHESTRATOR_VOLUME=cynapsa-demo-orchestrator-test-2 \
  ./run.sh
```

The Cloud Run worker pool `cynapsa-demo-orchestrator` is disabled at zero
instances. For this branch, run this directory locally with a fresh enrollment
token and an empty state volume. Do not reuse a token consumed by the former
cloud installation.
