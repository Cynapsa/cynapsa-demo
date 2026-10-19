# Cynapsa Demo

Four separate HTTP applications communicate through Cynapsa when launched
with its CLI. Application code uses normal Requests, HTTPX, FastAPI and
LangGraph; the CLI supplies enrollment, destination mapping, and socketless
ASGI delivery.

```text
interactive client -> orchestrator -> maps -> Google Places
                                  \-> files -> read-only local documents
```

The client sends POST `/ask` to Orchestrator. Orchestrator can call Maps
`/maps` and optionally Files `/files`, then answer with both sources.
Each server owns its own graph/process and can run on a different machine.
The Orchestrator persists conversation memory in SQLite in its local state
volume. Files is optional; no model or document data is shared between
processes except through explicit requests and responses.

## Run locally over Cynapsa

1. In each required directory, copy `.env.example` to the ignored `.env`.
   Fill the mesh ID, enrollment token and API keys. Set destination IDs from
   the portal: Orchestrator needs Maps's bare ID and optionally Files's;
   Client needs Orchestrator's bare ID. Each image build also requires a
   `CYNAPSA_GITHUB_TOKEN` with read access to the SDK/Core repos.
2. From separate terminals, run `./run.sh` in `maps/`, optionally `files/`,
   then `orchestrator/`, then `client/`. The first run builds that entity's
   own image. `./run.sh --build` refreshes SDK `main`
   and Go Core `main` from GitHub.
3. Ask a question in Client; enter `new` for a fresh chat or `quit` to stop.
   `client/run.sh --quiet` keeps only prompts, answers and failures visible.

Each entity stores its encrypted Cynapsa installation in a separate named
Docker volume. A revoked installation can be replaced with `./run.sh
--force-enroll` and a valid token after rejoining the mesh. Do not run two
instances with one saved installation concurrently.

## The HTTP/CLI switch

Maps and Orchestrator are conventional FastAPI apps. Their default image
command is `cynapsa run --config /tmp/cynapsa-private/cynapsa.json -- app:app`.
The entrypoint creates each agent's config from its environment, then the SDK
starts the ASGI lifespan directly without Uvicorn or an HTTP listener. In an
AWS task definition or GCP worker-pool revision, set `DEMO_RUN_COMMAND` to
`uvicorn app:app --host 0.0.0.0 --port 8000 --no-access-log` to run the
same code as ordinary HTTP. Uvicorn listens on the container's network
interfaces; cloud network ingress controls whether anyone can reach it.
The demo's AWS task security group denies inbound traffic, and its GCP worker
pool has no public endpoint or Direct VPC ingress. Switch
the variable back to the Cynapsa command to restore mesh access. No image or
application-code change is required for the switch, although the cloud
platform starts a replacement task/revision.

Successful model/Places HTTP access logs are suppressed. Cynapsa-launched
application requests use concise `[cynapsa] request started/received` and
`response received/sent` lines with a short demo request ID and elapsed time;
CLI lifecycle outcomes (`session.authenticate`, `session.ready`,
`session.closed`), changes in the actual `core.status` result, and confirmed
server-authority events (`peer.authorized`, `peer.rejected`, `peer.revoked`)
are also printed in Cynapsa ASGI mode. The eight-character demo request ID is
carried in an ordinary HTTP header through the bridge; it is not a Cynapsa
message ID or distributed trace. No line claims a network path or remote
delivery from local request submission. `client/run.sh --quiet` suppresses
the extra network-status stream. Direct HTTP mode uses `[http]`. Framework
startup and warnings remain separate.
The demo deliberately logs full request/response content, so use only
non-sensitive test data.
There are no native SDK handler or event-observer processes in these images.

For exact configuration and security details, read [AGENTS.md](AGENTS.md) and
each directory's README. Source SHAs are recorded in each built image at
`/opt/cynapsa/source-revisions`; current cloud deployments are independent
of this working tree.

## Optional telemetry

Rebuild an entity with `./run.sh --build` to include the paired Core and SDK
`main` branches. The image's `/opt/cynapsa/source-revisions` file records
the fetched commits. Telemetry is off by default. For live counters in a CLI-run
process, add `--telemetry-interval 30` before `--` in its Cynapsa run command;
this prints fixed SDK/Core snapshots to stderr every 30 seconds. The separate
`--diagnostics` option prints diagnostic events, not counters. Native sessions
and HTTP Bridge handles can also read `telemetry_snapshot()` and
`core_telemetry_snapshot()` explicitly.

For a local, retained metrics view, see [observability/](observability/README.md).
It provides an opt-in Collector, Prometheus, and Grafana stack. Each demo
`.env.example` documents the two explicit OTLP exporter settings and the
host-shell Docker network attachment. Starting the stack alone does not turn
on export. The demo's content-bearing application logs are **not** ingested by
this metrics stack. Neither cloud deployment nor distributed traces are
configured by these local steps.
