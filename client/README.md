# Demo client

`app.py` is an ordinary Requests program. It sends JSON to
`DEMO_ORCHESTRATOR_ORIGIN/ask` and imports no Cynapsa package. The container
entrypoint can run it through `cynapsa run`, which maps that HTTP origin to the
orchestrator's bare agent ID in RPC mode, or run it as plain HTTP. The CLI owns
login and the HTTP hook in Cynapsa mode.
The HTTP program does not consume SDK connection events; request failures
remain visible at the prompt. Rebuild with `./run.sh --build` to get this code.

This directory is an independently runnable interactive client for the
Cynapsa demo. Start the matching Maps and Orchestrator services first.
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
# Fill the private values needed for your chosen mode in .env.
./run.sh
```

The runner fetches SDK `main` and Go Core `main` from GitHub when the image
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
./run.sh --force-enroll  # Cynapsa mode: enroll after mesh removal/re-add
```

In Cynapsa mode, `--force-enroll` needs a still-valid `CYNAPSA_TOKEN` in `.env`
or the shell. It replaces the installation in the selected Docker volume through the same
`cynapsa run --force-enroll` process that runs the HTTP app. After changing
SDK/Core branches, rebuild with `./run.sh --build` and restart any running
container; subsequent runs reuse the rebuilt image.

`DEMO_MESH_ID` and `DEMO_ORCHESTRATOR_AGENT_ID` are required in Cynapsa mode.
Set `DEMO_ORCHESTRATOR_ORIGIN` to the orchestrator's actual reachable HTTPS
origin. The current AWS-owned origin is
`https://de-4f1d206529514541bb8cb316c5b0b51e.ecs.us-east-1.on.aws`.
It is both the URL used by the HTTP app and the origin mapped by the launcher.
The old `http://demo-orchestrator.local` default only works with Cynapsa; it is
not a real HTTP address. Direct HTTP mode also requires the shared
`DEMO_HTTP_ACCESS_KEY` in the shell or ignored `.env`; ask the demo operator for
the value. Never commit or paste it into documentation.

The same `app.py` and image support two exact commands. At the top of
`run.sh`, leave exactly one line uncommented:

```sh
DEMO_RUN_COMMAND='python /app/app.py'
# DEMO_RUN_COMMAND='cynapsa run --config /tmp/cynapsa-private/cynapsa.json -- python /app/app.py'
```

Move the `#` to the first line to select Cynapsa. The selected line overrides
any `DEMO_RUN_COMMAND` in `.env`. The entrypoint creates the private
`cynapsa.json` from `.env` when Cynapsa mode starts; you do not need to write
or mount that file. In HTTP mode, the runner uses the key from
the shell or ignored `.env`, or fetches it from GCP Secret Manager if absent.

The first sends a real HTTP request to `DEMO_ORCHESTRATOR_ORIGIN/ask` with the
demo access-key header and does not require mesh login. The second intercepts
that *same URL* and maps it to
`DEMO_ORCHESTRATOR_AGENT_ID`. It requires the mesh ID, agent ID, and an enrolled
client. The launcher accepts only these two exact command strings.

The current AWS origin is the runner's default if neither the shell nor `.env`
specifies one. Automatic key lookup requires a locally authenticated `gcloud`
account with access to the demo secret. No key is stored in the script.

Use the complete canonical **bare** destination JID, not the friendly label or
an installation `/r2...` address; the authority resolves the exact peer. The
client prints a generic failure for non-2xx responses or request errors and
continues prompting. The orchestrator logs the detailed downstream error.
No active target is unavailable, not automatic delivery to an offline mailbox.
See [source dependencies](SOURCE_DEPENDENCIES.md) for remote-versus-local SDK
changes; an unpushed local API removal is not in a GitHub image by assumption.

## Traffic logs

For chat-only output:

```sh
./run.sh --quiet
```

On the first run after updating this code, rebuild with
`./run.sh --build --quiet`. The Python app also accepts `python app.py --quiet`.
Quiet mode hides Python logs and startup banners while
keeping the input prompt, answers, citations, user-triggered `new` conversation
IDs, and request failures. The HTTP app has no SDK event observer.
Successful build output is hidden; build/startup failures remain visible.
It can be combined with `--force-enroll` without changing credentials or volumes.
This is a demo-client option, not a new SDK/Core flag; other agents are unchanged.

Without `--quiet`, the container logs `[cynapsa] request started to ...` and
`[cynapsa] response received from ...` with the orchestrator name, identity,
and full content, or a request failure. Running `python app.py` directly
instead labels its actual network requests `[http]`. There is no
redaction or truncation; do not put sensitive prompts in this demo if logs will
be shared. Headers are not logged. The answer also prints normally. HTTP access
logs are quiet, but warnings/errors remain. Rebuild with
`./run.sh --build` to update the local image.
