# Demo maps agent

Console alerts observe the SDK event stream independently of request handling
and input. Example: `ALERT mesh connectivity: available -> degraded`, followed
by recovery to `available` or failure to `unavailable`. SDK lifecycle changes
and canonical `core.error` details are also printed when emitted. A disconnect
alone does not identify revocation; the app is not automatically terminated.
The observer owns `next_event()` and stops before intentional session teardown.
Rebuild with `./run.sh --build` after the updated demo and Core sources have
been pushed; existing images do not gain these changes automatically.

This identity exposes a wildcard native Cynapsa handler and uses an
OpenAI-compatible LiteLLM gateway with Google Places (New) to answer place questions.
Its independent LangGraph alternates model reasoning and Places tool nodes,
then formats the final answer. Each request has fresh graph state; conversation
memory belongs to the orchestrator. Only JSON RPC data crosses Cynapsa, not
graph objects or shared process memory.

The directory is independently runnable. Its image build fetches the Python
SDK and Go Core `remove-snapshot` branches from GitHub; it does not use
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
image. Encrypted Cynapsa state persists in the `cynapsa-demo-maps-state` Docker
volume. Remove `CYNAPSA_TOKEN` from `.env` after enrollment. The model gateway
key is required on every run, either as `LITELLM_API_KEY` in `.env` or in the
ignored `.private/litellm_api_key` file with mode `0600`. `run.sh` mounts that
file read-only. `GOOGLE_MAPS_API_KEY` and `DEMO_MESH_ID` are also required.
The default gateway is `https://litellm.eladrave.com`, using model
`gpt-5.6-terra-high`; override with `LITELLM_BASE_URL` and `LITELLM_MODEL`.
Remove obsolete `GEMINI_*` entries from any existing local `.env` file;
the runner ignores them.
For each setting, `.env` takes precedence over an exported shell variable;
the shell supplies only settings absent from `.env`.

Use `./run.sh --build` to force a rebuild from this directory. The image takes
`CYNAPSA_TOKEN` at container startup from the ignored `.env` or shell
environment only when its state volume has no saved profile. No token is baked
into the Dockerfile or image.

## Test another identity

```sh
CYNAPSA_TOKEN='another-enrollment-token' \
  CYNAPSA_DEMO_MAPS_VOLUME=cynapsa-demo-maps-test-2 \
  ./run.sh
```

Remove `CYNAPSA_TOKEN` from `.env` before using the shell-token example above.
The Cloud Run worker pool `cynapsa-demo-maps` was previously observed at zero
instances; current status was not verified by this documentation audit.
For a new local installation, use an empty state volume and an active
enrollment grant whose mesh scope and installation limit allow it. The
existing volume uses its saved installation proof, not `CYNAPSA_TOKEN`.
After mesh removal and re-addition, that proof is revoked. Supply a still-valid
token and run `./run.sh --force-enroll` to replace the installation through
the built-in `cynapsa run --force-enroll` CLI in the same selected volume. The
CLI session closes before the native maps agent starts from the new profile.
The fetched SDK includes force-enroll and native remote-error handling. After
the SDK/Core branches change, rebuild with `./run.sh --build` and restart any
running container; subsequent runs reuse the rebuilt image.

## Model/tool and delivery bounds

The working-tree app allows three model rounds and at most ten tool calls per
round. The first round must call a tool. Details IDs must come from a prior
search; search queries are 1–300 characters with up to five results, and detail
IDs are 1–256 characters. No Routes/Roads API is provided. Missing/invalid tools
return 502 `maps_tool_error`; exhausted rounds return `maps_no_answer`; Places
failures return `places_unavailable`; model failures return 503
`model_unavailable`. Invalid questions return 400 `bad_request`.

Tool diagnostics record bounded round/call/reason categories, not arguments or
place IDs. Model/Places HTTP timeouts are 25/10 seconds per client operation,
not a total completion guarantee. Saved installation state is not unlimited
authority: expired credentials and server revocation can reject it.
Exact pending-session replay is best effort; there is no offline mailbox.
See [source dependencies](SOURCE_DEPENDENCIES.md): remote SDK/Core builds do
not include unpushed local changes by assumption.
