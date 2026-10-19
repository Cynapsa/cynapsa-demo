# Maps agent

This is a conventional FastAPI application. POST `/maps` with JSON
`{"question": "..."}`; its wildcard POST route accepts the same body at
other paths. A local LangGraph uses a LiteLLM-compatible model and Google
Places search/details tools to return an answer, sources, and attribution.
Its graph state is fresh for each request; conversation memory lives in the
Orchestrator. The app does not import Cynapsa.

## Run

Copy `.env.example` to the ignored `.env`. Set `DEMO_MESH_ID`, an enrollment
`CYNAPSA_TOKEN` for new installations, `GOOGLE_MAPS_API_KEY`, a LiteLLM key,
and `CYNAPSA_GITHUB_TOKEN` for image builds. The LiteLLM key may instead be
stored at ignored `.private/litellm_api_key` (mode 0600). `./run.sh` builds
the image if missing and launches the app via `cynapsa run --config
/tmp/cynapsa-private/cynapsa.json -- app:app`. Its generated JSON config
contains the mesh ID, timeout, and optional enrollment-token file path, not
the token itself. The CLI delivers mesh requests to FastAPI without opening
an HTTP port. `./run.sh --build` fetches SDK `main` and
Core `main`; existing images do not
update automatically. The encrypted installation profile remains in the
`cynapsa-demo-maps-state` Docker volume.

Use `./run.sh --force-enroll` with a still-valid token if the saved
installation was revoked after mesh removal. Stop the old process first.
For a separate installation, select another
`CYNAPSA_DEMO_MAPS_VOLUME` and ensure the grant has capacity.

Set `DEMO_RUN_COMMAND=uvicorn app:app --host 0.0.0.0 --port 8000
--no-access-log` in the container environment to run the same app as ordinary
HTTP. It listens on all container interfaces; the existing cloud worker pool
has no public endpoint or Direct VPC ingress, so external clients cannot reach
it. The default is the
full Cynapsa command above. Only these two exact values are accepted.

The same image can run as a Cloud Run **service** on port 8000. In HTTP mode,
Uvicorn serves `/maps`. In Cynapsa mode, the app stays socketless, while a
minimal listener serves only `GET /health` so Cloud Run can start the service;
all other HTTP paths return 404. Keep one instance and disable CPU throttling
so the mesh session can run between HTTP requests. Do not run the old worker
pool in Cynapsa mode at the same time with the same installation credentials.
The Cloud Run service URL is the real `DEMO_MAPS_ORIGIN` for the orchestrator
in *both* modes; the CLI maps that same URL to the Maps agent ID.

## Bounds and logs

The model has at most three rounds and ten tool calls per round. The first
round must call a tool. Place detail IDs must come from a prior search.
Invalid input returns HTTP 400 `bad_request`; invalid tools, exhausted
rounds, and Places errors return HTTP 502 with distinct codes; model failure
returns HTTP 503. The CLI projects these canonical responses to remote
callers. Model/Places HTTP access INFO logs are suppressed, while full demo
question/answer bodies are logged. Do not send sensitive content to cloud
logs. Native SDK event alerts are not part of this HTTP application.

See [source dependencies](SOURCE_DEPENDENCIES.md) for build provenance.
