# Orchestrator agent

This is a conventional FastAPI/LangGraph application. POST `/ask` with JSON
`{"prompt": "...", "conversation_id": "chat-1"}`. The conversation ID is
optional; if omitted, a new one is returned. The graph can answer directly,
ask Maps, ask the optional Files agent, or chain Files → Maps to answer a
location question using a fact found in private documents. The app imports
HTTPX, not the Cynapsa SDK.

## Run and destination mapping

Copy `.env.example` to ignored `.env`. Set `DEMO_MESH_ID`,
`DEMO_MAPS_AGENT_ID` (canonical bare JID), an enrollment `CYNAPSA_TOKEN` for
new installations, the LiteLLM API key, and `CYNAPSA_GITHUB_TOKEN` for image
builds. Set optional `DEMO_FILES_AGENT_ID` to enable the Files tool. The
LiteLLM key may instead live in ignored `.private/litellm_api_key` with mode
0600. Then run `./run.sh`. The CLI maps `DEMO_MAPS_ORIGIN` to Maps's ID
and `http://demo-files.local` to Files's ID. Its generated JSON config holds
the mesh ID, destination mappings, timeout, and optional token-file path (not
the token). The CLI runs `app:app` directly without an HTTP listener: HTTPX
calls cross the Cynapsa mesh; inbound `/ask` goes to FastAPI.

`./run.sh --build` refreshes SDK `main` and Core `main`. Existing images do not update
automatically. The installation profile persists in the
`cynapsa-demo-orchestrator-state` Docker volume. If revoked, rejoin the mesh
and use `./run.sh --force-enroll` with a valid token after stopping the old
process. Do not run two processes with the same saved installation.

For ordinary HTTP mode, set `DEMO_RUN_COMMAND=uvicorn app:app --host
0.0.0.0 --port 8000 --no-access-log`. Uvicorn listens on all container
interfaces. The AWS task security group denies inbound traffic, so this HTTP
listener is not remotely reachable. If you intentionally build
an HTTP deployment, point `DEMO_MAPS_ORIGIN` and optional `DEMO_FILES_ORIGIN`
at reachable HTTP servers and add real authentication before exposing it.

For the deployed Maps agent, use
`DEMO_MAPS_ORIGIN=https://cynapsa-demo-maps-service-144701547770.us-central1.run.app`
in both modes. The CLI maps that exact URL to `DEMO_MAPS_AGENT_ID`; direct HTTP
calls the same URL. The Cloud Run service currently requires IAM authentication,
and `/maps` returns 404 while Cynapsa mode is selected. To make an end-to-end
HTTP comparison, switch Maps to HTTP mode and configure access intentionally.

## Conversation memory

LangGraph uses async SQLite checkpoints in a private directory. In the local
container, the default is `/var/lib/cynapsa/conversations.sqlite` in the
named state volume. `DEMO_MEMORY_DIRECTORY` can move memory to a private
owner-controlled 0700 directory (e.g. local task disk while credentials are
on EFS); its SQLite file is 0600 and not encrypted. Do not place a live SQLite
WAL file on shared NFS/EFS or run multiple writers against it. Replacement of
task-local disk loses conversation history.

The key includes owner, mesh, sender, and conversation ID. In Cynapsa mode,
the CLI's ASGI bridge supplies authenticated sender as
`request.state.cynapsa_sender`; no user JSON field can spoof it. In direct
HTTP mode the demo falls back to HTTP client address. That fallback is not
an authentication system, so untrusted HTTP clients must be authenticated
separately before using this memory design. The model sees the last eight
successful turns; `new` in Client creates a fresh conversation ID.

## Bounds and errors

There are at most four remote tool calls per question, and both tools remain
available after each result so a Files result can inform a subsequent Maps
question. Remote calls use HTTPX with a 100-second per-call timeout; the
Client's `/ask` timeout is 140 seconds, not a guarantee that every chain
finishes. Invalid input returns 400, downstream timeouts 504, model failure
503, and other failures a generic 502 to the caller. Real downstream errors
are logged in the Orchestrator terminal. A normalized downstream
`authorization_rejected` is shown there as one warning without a Python
traceback; the HTTP caller still receives the demo's generic failure response.
Unexpected exceptions retain a traceback for diagnosis. Successful model HTTP
access logs are suppressed; full demo questions/answers are logged without
redaction.

See [source dependencies](SOURCE_DEPENDENCIES.md) for build provenance.
