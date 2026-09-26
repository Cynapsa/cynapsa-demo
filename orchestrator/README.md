# Demo orchestrator

Console alerts observe the SDK event stream independently of request handling
and input. Example: `ALERT mesh connectivity: available -> degraded`, followed
by recovery to `available` or failure to `unavailable`. SDK lifecycle changes
and canonical `core.error` details are also printed when emitted. A disconnect
alone does not identify revocation; the app is not automatically terminated.
The observer owns `next_event()` and stops before intentional session teardown.
Rebuild with `./run.sh --build` after the updated demo and Core sources have
been pushed; existing images do not gain these changes automatically.

This identity accepts `/ask` RPCs from the demo client. An OpenAI-compatible
LiteLLM gateway decides whether to answer directly or call the maps identity
over Cynapsa. A local LangGraph manages reasoning, the remote tool call, and
final answer composition. Maps runs its own graph on its own machine; it is
not an in-process subgraph.
The async native SDK must dispatch handlers on the persistent loop that owns
the session, graph, and SQLite saver. Keep that loop running through
`await session.close()`; per-request `asyncio.run()` dispatch is incompatible
with reused async locks. This requires the async-dispatch fix on the SDK's
`main` branch; rebuilding against an older SDK does not include it.
Downstream native and canonical remote errors are logged with their actual
status, code, message, and public details in this agent's terminal. The client
receives the generic `The demo could not answer` RPC error for those downstream
native/internal failures. Validation, timeout, and model failures have distinct
public codes below. Treat terminal details/traces as private.

The directory is independently runnable. Its image build fetches the Python
SDK and Go Core `main` branches from GitHub; it does not use
repository-root files or sibling directories. Set `CYNAPSA_GITHUB_TOKEN`
in this directory's ignored `.env` for builds. It is not sent to the
running container.

## Run

```sh
cp .env.example .env
# Set every required value in .env.
./run.sh
```

The credentials are runtime environment variables and are never stored in the
image. Encrypted Cynapsa state persists in the
`cynapsa-demo-orchestrator-state` Docker volume. Remove `CYNAPSA_TOKEN` from
`.env` after enrollment. The model gateway key is required on every run,
either as `LITELLM_API_KEY` in `.env` or in the ignored
`.private/litellm_api_key` file with mode `0600`; `run.sh` mounts the file
read-only. The default gateway is `https://litellm.eladrave.com`, using model
`gpt-5.6-terra-high`. Override with `LITELLM_BASE_URL` and `LITELLM_MODEL`.
Remove obsolete `GEMINI_*` entries from any existing local `.env` file;
the runner ignores them.
`DEMO_MESH_ID` and `DEMO_MAPS_AGENT_ID` are also required on every run.
For each setting, `.env` takes precedence over an exported shell variable;
the shell supplies only settings absent from `.env`.

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

Remove `CYNAPSA_TOKEN` from `.env` before using the shell-token example above.
The Cloud Run worker pool `cynapsa-demo-orchestrator` was previously observed
at zero instances; current status was not checked in this audit.
For a new local installation, use an empty state volume and an
active enrollment grant whose mesh scope and installation limit allow it. The
existing volume uses its saved installation proof, not `CYNAPSA_TOKEN`.
After mesh removal and re-addition, that proof is revoked. Supply a still-valid
token and run `./run.sh --force-enroll` to replace the installation through
the built-in `cynapsa run --force-enroll` CLI in the same selected volume. The
CLI session closes before the native orchestrator starts from the new profile.
The fetched SDK includes force-enroll and native remote-error handling. After
the SDK/Core branches change, rebuild with `./run.sh --build` and restart any
running container; subsequent runs reuse the rebuilt image.

## Conversation memory

Send `{"prompt": "...", "conversation_id": "..."}` to `/ask`. The client
creates and prints the ID, uses it for every turn, and accepts `new` to start
a fresh chat. Requests without an ID still work, but each gets a fresh ID
returned in the response. IDs contain 1–64 letters/numbers/underscores/hyphens.

LangGraph's async SQLite checkpointer stores state in
`/var/lib/cynapsa/conversations.sqlite`, inside the existing orchestrator
volume. Restarting/rebuilding the container preserves memory when the volume
is retained. Threads are scoped to this orchestrator's canonical identity,
authenticated sender, mesh, and conversation ID—not sender fields from JSON.
A different client cannot access your history by guessing your ID. A new
installation of the same logical client can resume a chat using the same ID.

The model sees the last eight successful user/assistant turn pairs; stored
answer context is capped at 4,000 characters per turn. It can use a remembered
address to formulate a self-contained Maps question. Maps itself is stateless,
and old map evidence is not treated as a fresh Places lookup. Failed turns
do not enter the successful history; later turns start fresh rather than
automatically retrying an interrupted RPC.

This is persistent conversation memory, not a cross-chat user-profile store.
Checkpoint records can grow on disk even though model context is bounded.
The file is mode 0600 in a private directory, but is not encrypted like the
Cynapsa credential profile. Protect the volume: prompts/answers/tool evidence
are private. `new` does not delete old checkpoints. One process owns the file
and serializes requests; shared-volume replicas/distributed memory and disk
retention policies are not implemented. See
[LangGraph persistence](https://docs.langchain.com/oss/python/langgraph/persistence).

## Model/tool and error bounds

The app accepts zero or exactly one `ask_maps_agent` tool call; it validates
the question (1–2000 characters) and call ID, invokes `/maps` with a
100,000-ms TTL, then performs one final model completion without another tool
loop. Invalid/extra tool calls fail; they are not silently retried. The model
HTTP client uses a 25-second timeout, not a whole-RPC completion guarantee.

Bad request input returns 400 `bad_request`, maps/native safety timeouts return
504 `maps_timeout`, and model failures return 503 `model_unavailable`.
Downstream canonical remote/native errors and other internal failures return
502 `orchestrator_failed`; their actual details/traces remain in this terminal.
The destination must be a complete canonical **bare** JID, not a friendly alias
or a full installation-session address. No active target is unavailable;
pending resume is not an offline mailbox or delivery receipt.

Saved profiles take precedence over new tokens during ordinary runs but can
still fail credential expiry/attachment checks. Stop the old container before
force-enrolling a replacement in the same volume. See
[source dependencies](SOURCE_DEPENDENCIES.md) for remote-versus-local changes.
