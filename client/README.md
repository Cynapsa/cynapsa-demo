# Demo client

Console alerts observe the SDK event stream independently of request handling
and input. Example: `ALERT mesh connectivity: available -> degraded`, followed
by recovery to `available` or failure to `unavailable`. SDK lifecycle changes
and canonical `core.error` details are also printed when emitted. A disconnect
alone does not identify revocation; the app is not automatically terminated.
The observer owns `next_event()` and stops before intentional session teardown.
Rebuild with `./run.sh --build` after the updated demo and Core sources have
been pushed; existing images do not gain these changes automatically.

This directory is an independently runnable interactive client for the local
Cynapsa demo. Start the maps and orchestrator containers first; their GCP
worker pools were previously observed disabled, not verified in this audit.
Docker is the runtime prerequisite; building also
needs GitHub access and a `CYNAPSA_GITHUB_TOKEN` with read access to the
SDK and Go Core repositories.

For complete setup and usage instructions, see [RUN.md](RUN.md).
Every prompt carries a conversation ID for the orchestrator's persistent
LangGraph memory. Type `new` for a fresh chat; set `DEMO_CONVERSATION_ID` in
`.env` to resume a printed ID after restarting the client. No LLM or graph
runs in the client container.

## Run

```sh
cp .env.example .env
# Replace the CYNAPSA_TOKEN placeholder in .env.
./run.sh
```

The runner fetches SDK and Go Core `main` from GitHub when the image
is missing. Set `CYNAPSA_GITHUB_TOKEN` in this directory's ignored `.env`
for builds; it is not passed to the running container. The enrollment token
is passed as a runtime environment variable and enrolls the client only when
the `cynapsa-demo-client-state` Docker volume has no saved profile. Remove the
token from `.env` after enrollment; later runs reuse the encrypted profile.

You can export `CYNAPSA_TOKEN` instead of using `.env`. An exported value takes
precedence over the value in `.env`.

## Commands

```sh
./run.sh          # build if needed, then run
./run.sh --build  # force a rebuild from this directory
./run.sh --force-enroll  # enroll a new installation after mesh removal/re-add
```

`--force-enroll` needs a still-valid `CYNAPSA_TOKEN` in `.env` or the shell. It
replaces the installation in the selected Docker volume using the fetched
`cynapsa run --force-enroll` command. That CLI session closes before the
native client opens its connection from the new profile.
The fetched SDK includes force-enroll and native remote-error handling. After
changing SDK/Core branches, rebuild with `./run.sh --build` and restart any
running container; subsequent runs reuse the rebuilt image.

`DEMO_MESH_ID` and `DEMO_ORCHESTRATOR_AGENT_ID` are required. This keeps the
same image reusable for any mesh and orchestrator identity.

Use the complete canonical **bare** destination JID, not the friendly label or
an installation `/r2...` address; the authority resolves the exact peer. The
client prints canonical remote errors/native errors and continues prompting.
No active target is unavailable, not automatic delivery to an offline mailbox.
See [source dependencies](SOURCE_DEPENDENCIES.md) for remote-versus-local SDK
changes; an unpushed local API removal is not in a GitHub image by assumption.
