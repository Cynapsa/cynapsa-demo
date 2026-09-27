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

The graph loops `reason -> agent_tools -> reason` until the model returns a final
answer. Each question allows at most four remote tool calls in total, to Maps or
the optional Files agent. Both tool schemas remain available after each result,
so Files can supply a city for a subsequent Maps search in the same question.
Multiple calls in a model response run sequentially after the entire batch is
validated; dependent lookups should be requested in separate reasoning rounds.
Questions must contain 1–2000 characters, and call IDs must be nonempty and unique
within the turn. Unknown/malformed/over-budget batches fail before any calls in
that batch are sent. Remote failures are propagated, not automatically retried.
At four calls, the next model completion uses `tool_choice=none`; an attempted
extra call is rejected. The prompt requires honest partial-result reporting at
the limit, not invented missing information. There are at most five model
completions. Each downstream RPC retains a 100,000-ms TTL and the model HTTP
client retains a 25-second timeout. These are not a whole-question deadline:
the client's existing 140,000-ms `/ask` TTL can expire during a slow chain.
Successful answers retain both `files` and `maps` evidence; repeated calls to
the same agent retain earlier citations. The final answer alone enters successful
conversation history. A failed chain is not resumed/replayed by a later turn.

Bad request input returns 400 `bad_request`, maps/native safety timeouts return
504 `agent_timeout`, and model failures return 503 `model_unavailable`.
Downstream canonical remote/native errors and other internal failures return
502 `orchestrator_failed`; their actual details/traces remain in this terminal.
The destination must be a complete canonical **bare** JID, not a friendly alias
or a full installation-session address. No active target is unavailable;
pending resume is not an offline mailbox or delivery receipt.

Saved profiles take precedence over new tokens during ordinary runs but can
still fail credential expiry/attachment checks. Stop the old container before
force-enrolling a replacement in the same volume. See
[source dependencies](SOURCE_DEPENDENCIES.md) for remote-versus-local changes.
## Optional files agent

Set `DEMO_FILES_AGENT_ID` to its canonical bare JID to enable `ask_files_agent`.
Blank/absent preserves maps-only operation. Private-document questions should
use this tool; place searches use Maps. Combined questions can chain Files then
Maps within the four-call budget, without another user prompt. For example,
“Find Guy's home city and recommend cafes there” should retrieve the city, search
Maps using it, and return both facts and citations in one final answer.
File results are returned under `files`, with source paths/line evidence for the
client. Existing `maps` responses and identity-scoped conversation memory remain.
Downstream timeout is now 504 `agent_timeout`; detailed downstream errors are
logged locally while the client receives the existing generic 502 failure.
Rebuild/restart after application changes. File excerpts and answers may be
retained in the orchestrator/model context; see `../files/README.md` for privacy.
