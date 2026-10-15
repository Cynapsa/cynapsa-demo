# Cynapsa demo operational runbook

This working tree has four independently buildable entity directories. Do not
commit enrollment tokens, API keys, installation profiles, document databases,
or conversation SQLite files. Each image clones SDK `feature/cli-operations`
and Go Core `main` from GitHub at build time; unpushed source edits are not in an image.
Record and verify `/opt/cynapsa/source-revisions` when debugging a build.

## Topology and launch mode

```text
client (Requests) --/ask--> orchestrator (FastAPI/LangGraph)
                              |--/maps--> maps (FastAPI/LangGraph + Google Places)
                              +--/files--> files (FastAPI/LangGraph + read-only docs)
```

All four applications use ordinary HTTP libraries and have no `cynapsa`
imports. Maps and Orchestrator generate a non-secret Cynapsa JSON config from
their environment and default to `cynapsa run --config ... -- app:app`. The
CLI enrolls or loads an installation, maps outbound HTTP origins to bare
agent IDs, and drives the FastAPI lifespan directly without an HTTP listener.
Files still uses socketless Uvicorn through the CLI. Inter-agent traffic is
Cynapsa RPC even though application code uses HTTP request/response objects.
Maps and Orchestrator accept one of two exact `DEMO_RUN_COMMAND` values:
the Cynapsa command above or `uvicorn app:app --host 127.0.0.1 --port 8000
--no-access-log`. The latter is ordinary HTTP bound to container loopback,
not publicly reachable. An explicit trusted image command remains possible.

Start Maps and optional Files first, then Orchestrator, then Client. Do not
run two containers with the same saved installation profile concurrently.
Each identity must be in the same mesh. Only destination IDs are configured:
enrollment supplies each process's own identity.

## Prerequisites and secrets

- Docker Engine with Buildx and network access to GitHub, base-image registries,
  Go modules, and Python packages.
- `CYNAPSA_GITHUB_TOKEN` with read access to the SDK/Core repositories in each
  entity's ignored `.env` for image builds. BuildKit uses it only as a secret;
  the runner excludes it from the runtime environment.
- A valid enrollment grant for each new installation, scoped to the mesh;
  a saved encrypted profile lives in a named Docker volume.
- LiteLLM API key for the three servers, Google Places API key for Maps, and
  an absolute read-only document directory for Files.

Copy each `<entity>/.env.example` to `<entity>/.env` and fill only that entity's
settings. Use its ignored `.private/litellm_api_key` (mode 0600) or
`LITELLM_API_KEY` in `.env`. Never put keys into code or image layers. After
successful enrollment, the original `CYNAPSA_TOKEN` may be removed from
`.env`; keep model/Places keys available on every run. If mesh removal revoked
an installation, rejoin the mesh and run `./run.sh --force-enroll` with a
still-valid token in the same volume. Stop the old process first.

## Local run

From separate terminals, use `./run.sh` inside the required directories in
the order above. Maps needs `DEMO_MESH_ID`, model/Places keys. Orchestrator
also needs `DEMO_MAPS_AGENT_ID`; set optional `DEMO_FILES_AGENT_ID` to enable
the Files tool. Files needs `DEMO_FILES_DIRECTORY`; the runner mounts only
that directory read-only. Client needs `DEMO_ORCHESTRATOR_AGENT_ID`; it accepts
`./run.sh --quiet`. These are canonical **bare** Cynapsa IDs obtained from
the portal's identity page, not friendly display names. A distinct named
volume may be selected with `CYNAPSA_DEMO_<ROLE>_VOLUME`.

`./run.sh --build` refetches remote SDK/Core heads. A push or local source
change does not update an existing image/container. Current cloud images are
not changed by edits in this repository. Rebuild and redeploy separately.

## HTTP app and memory contract

Maps accepts POST `/maps` (and the wildcard POST handler) with a JSON
`question`. Files accepts POST `/files`. Orchestrator accepts POST `/ask`
with `prompt` and optional `conversation_id`. Its HTTPX calls use virtual
origins `http://demo-maps.local` and `http://demo-files.local`; the CLI maps
each to a destination ID. Without Cynapsa, those origins can be overridden
via `DEMO_MAPS_ORIGIN` and `DEMO_FILES_ORIGIN` to ordinary reachable HTTP
servers. Client calls `http://demo-orchestrator.local/ask` through its map.

Orchestrator checkpoints LangGraph conversations in a private 0700 directory
with a 0600 SQLite file. By default this is `/var/lib/cynapsa` in the local
state volume; `DEMO_MEMORY_DIRECTORY` may override it (e.g. task-local disk
separate from EFS). Do not share a live SQLite WAL database across replicas.
Conversation keys include mesh, sender, and conversation ID. The CLI's ASGI
bridge supplies authenticated sender in `request.state.cynapsa_sender`; the
app falls back to HTTP client address in direct HTTP mode. Direct HTTP mode
does not authenticate callers: do not expose it publicly without an auth
layer. The client can enter `new` to start a fresh conversation.

These demo apps log full question/answer bodies without redaction. Do not use
private content if cloud log retention is inappropriate. External model and
Places HTTP request logs are reduced to WARNING. Native SDK event observers
were removed with the native handler paths; the CLI handles transport lifecycle.

## Delivery and failures

All remote calls are RPC; direct HTTP calls remain ordinary HTTP. Server-side
authorization controls Cynapsa communication. Canonical HTTP errors from
FastAPI are projected by the CLI to mapped HTTP callers. Orchestrator logs
downstream failures and returns a generic 502 to the client; invalid input is
400, downstream timeout 504, and model failure 503. Maps returns 400 for bad
questions, 502 for tool/Places failures, and 503 for model unavailability.
The demo does not guarantee a multi-tool chain finishes within the client's
140-second timeout. An accepted send is not a delivery receipt.

Run credential-free checks from the repository root with
`.venv/bin/python -m pytest -q`. These mock HTTP, model, and Places calls;
they do not prove live mesh delivery. Use the entity READMEs for endpoint
details and the `SOURCE_DEPENDENCIES.md` files for build provenance.
