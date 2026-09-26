# Run the Cynapsa demo client

The client runs on your laptop and communicates with locally running maps and
orchestrator agents over Cynapsa. Start those two containers first; their GCP
worker pools were previously observed at zero instances, not checked for this
audit. You need Docker and a Cynapsa
enrollment token. The
application and Docker build live in this directory. The Python SDK and Go
Core are fetched from their `main` GitHub branches during the
image build.

## Requirements

- Git
- Docker Desktop, running before you start the client
- Docker Buildx/BuildKit for GitHub-backed image builds
- A client agent in the `cynapsa-demo` mesh
- An active enrollment token for that client agent

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

Create an ignored environment file and replace all placeholders. The mesh ID
and orchestrator bare agent ID are required on every run; the enrollment token
is required only for a new state volume:

```sh
cp .env.example .env
# Edit .env, then:
./run.sh
```

Alternatively, provide the token directly from the shell:

```sh
CYNAPSA_TOKEN='your-enrollment-token' ./run.sh
```

The script builds the client image from this directory, enrolls once, and
saves the encrypted installation profile in the
`cynapsa-demo-client-state` Docker volume. Remove `CYNAPSA_TOKEN` from `.env`
after enrollment.

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

```sh
cd /path/to/cynapsa-demo/client
./run.sh
```

If this client's installation was revoked when its agent was removed from the
mesh, re-add the agent, supply a still-valid token, and run
`./run.sh --force-enroll`. This runs `cynapsa run --force-enroll` against the
same Docker volume, then starts the native client from the replaced profile.
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
