# Run the Cynapsa demo client

The client runs on your laptop and reaches the orchestrator over Cynapsa or
ordinary HTTPS, depending on its run command.
The orchestrator and maps agents must already be running, whether locally or
remotely. You need Docker; Cynapsa mode also needs a client enrollment token.
The application and Docker build live in this directory. The Python SDK
`main` and Go Core `main` branches are fetched from GitHub
during the image build.

## Requirements

- Git
- Docker Desktop, running before you start the client
- Docker Buildx/BuildKit for GitHub-backed image builds
- For Cynapsa mode: a client agent in the `cynapsa-demo` mesh and an active
  enrollment token for its first login
- For HTTP mode: access to the shared demo key in GCP Secret Manager, or the
  key in the shell or ignored `.env`

Also put `CYNAPSA_GITHUB_TOKEN` with read access to both repositories in
the ignored `.env` for builds. The GitHub token is build-only and is not
passed to the running container. Treat both tokens like passwords. Never
commit `.env` or paste a token into a
tracked repository file.

## Download only the client

```sh
git clone --branch main --filter=blob:none --sparse https://github.com/Cynapsa/cynapsa-demo.git
cd cynapsa-demo
git sparse-checkout set client
cd client
```

## First run

Create an ignored environment file and fill the values needed for your mode.
The mesh ID, orchestrator bare agent ID, and enrollment token are needed only
for Cynapsa mode; the token is needed only for a new state volume:

```sh
cp .env.example .env
# Edit .env, then:
./run.sh
```

Alternatively, provide the token directly from the shell:

```sh
CYNAPSA_TOKEN='your-enrollment-token' ./run.sh
```

In Cynapsa mode, the script builds the client image from this directory,
enrolls once, and runs an ordinary Requests program through the CLI-installed
HTTP mapping.
The app itself imports no Cynapsa package. The CLI saves the encrypted
installation profile in the
`cynapsa-demo-client-state` Docker volume. Remove `CYNAPSA_TOKEN` from `.env`
after enrollment.

To compare plain HTTP with Cynapsa, edit the two lines at the top of
`run.sh`. Keep exactly one uncommented:

```sh
DEMO_RUN_COMMAND='python /app/app.py'
# DEMO_RUN_COMMAND='cynapsa run --config /tmp/cynapsa-private/cynapsa.json -- python /app/app.py'
```

Move the `#` to the HTTP line to select Cynapsa; then run the same `./run.sh`
command. The container generates that private `cynapsa.json` from `.env`
on each Cynapsa start; you do not need to create it yourself. It includes
the mesh ID, client profile, mapping, timeout, and token-file path only when
enrollment is needed. Set `DEMO_ORCHESTRATOR_ORIGIN` to the orchestrator's real HTTPS
origin in `.env` and keep it unchanged when switching. The CLI
maps that exact HTTPS origin to `DEMO_ORCHESTRATOR_AGENT_ID`; the application
source remains ordinary `requests` code. For the current no-Cloudflare HTTP
demo, set `DEMO_ORCHESTRATOR_ORIGIN` to
`https://de-4f1d206529514541bb8cb316c5b0b51e.ecs.us-east-1.on.aws` and
supply `DEMO_HTTP_ACCESS_KEY` from the demo operator in your ignored `.env`
or shell. If absent, the runner fetches it with `gcloud` from GCP Secret
Manager at runtime. Never commit the key. Direct HTTP does not require mesh
enrollment. The old `.local` origin is only a virtual mapping, not a usable
HTTP endpoint.

Automatic key lookup needs a locally authenticated `gcloud` account with
access to the demo secret. The key itself is not stored in `run.sh`.

After the prompt appears, ask a question such as:

```text
Where is the nearest gym to 12201 Park Drive, Hollywood, Florida?
```

Follow up with questions such as "Which of those is closest?"; the
orchestrator remembers successful turns in this conversation. Enter `new` to
start a fresh chat, or `quit` to close the client.

The client prints a conversation ID at startup. To resume that chat after a
restart, set `DEMO_CONVERSATION_ID` in this client's `.env` to the printed ID,
then run `./run.sh`. You must use the same logical client identity, mesh, and
orchestrator identity, and retain the orchestrator's state volume. Without
this optional setting, each client run starts a new chat. The Python program
also accepts `--conversation-id`, while the container runner uses `.env`.
Changing the ID starts a different chat; it does not erase earlier history.

## Later runs

For prompts and answers without diagnostic logs or startup banners:

```sh
./run.sh --quiet
```

Rebuild once after updating to this implementation with
`./run.sh --build --quiet`. Build failures and request failures remain visible;
successful build output is hidden. Normal runs without `--quiet` retain logs.
In Cynapsa mode, use `./run.sh --force-enroll --quiet` if a replacement
enrollment is required.
For direct Python execution, the equivalent option is `python app.py --quiet`.
The HTTP app has no SDK event observer. Quiet mode preserves chat citations
and `new`.

```sh
cd /path/to/cynapsa-demo/client
./run.sh
```

If this client's installation was revoked when its agent was removed from the
mesh, re-add the agent, supply a still-valid token, and run
`./run.sh --force-enroll`. This runs `cynapsa run --force-enroll` against the
same Docker volume and starts the HTTP program within that CLI session.
Later ordinary runs use
the same volume. Use `./run.sh --build` to fetch newer branch commits;
ordinary retries reuse the existing image.

Stop the old container before replacement. A retained enrollment grant can
still expire, be independently revoked, or lack installation capacity. A saved
profile suppresses ordinary enrollment, not server revocation or credential
expiry checks. Do not copy the profile to another replica.

## Rebuild the image

```sh
./run.sh --build
```

This fetches remote branch heads, not unpushed SDK/Core edits. Check
[SOURCE_DEPENDENCIES.md](SOURCE_DEPENDENCIES.md) before claiming a local fix is
included. An existing running container retains its original image.

## Test another token without losing the current installation

Use a different Docker volume for the other token:

```sh
CYNAPSA_TOKEN='another-enrollment-token' \
  CYNAPSA_DEMO_CLIENT_VOLUME=cynapsa-demo-client-test-2 \
  ./run.sh
```

Use the same `CYNAPSA_DEMO_CLIENT_VOLUME` on later runs of that installation.

## Remove the local installation

```sh
docker volume rm cynapsa-demo-client-state
```

This permanently removes the saved client installation from the laptop. It
does not delete the agent in the Cynapsa portal.

## Troubleshooting

- `Docker is required`: install Docker Desktop.
- `Docker is not running`: start Docker Desktop and wait until it is ready.
- `an enrollment token is required`: set `CYNAPSA_TOKEN` in `.env` or the shell.
- Enrollment fails: check the token's status, mesh scope, expiry, and
  active-installation limit. A grant can be reused when those rules allow it.
- To force a fresh local image build, run `./run.sh --build`.
