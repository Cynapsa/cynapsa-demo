# Demo maps agent

This identity exposes a wildcard native Cynapsa handler and uses Gemini function
calling with Google Places (New) to answer place questions.

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
image. Encrypted Cynapsa state persists in the `cynapsa-demo-maps-state` Docker
volume. Remove `CYNAPSA_TOKEN` from `.env` after enrollment; both API keys are
required on every run. `DEMO_MESH_ID` is also required on every run.

Use `./run.sh --build` to force a rebuild from this directory.

## Test another identity

```sh
CYNAPSA_TOKEN='another-enrollment-token' \
  CYNAPSA_DEMO_MAPS_VOLUME=cynapsa-demo-maps-test-2 \
  ./run.sh
```

The deployed Cloud Run worker pool is named `cynapsa-demo-maps` in project
`aztm-amesh`, region `us-central1`. Its secrets and state are supplied through
Secret Manager and are not committed here.
