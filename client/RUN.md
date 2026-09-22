# Run the Cynapsa demo client

The client runs on your laptop and communicates with the deployed demo agents
over Cynapsa. You only need Docker and a Cynapsa enrollment token. The
application, Python SDK, and Go Core are already included in the image.

## Requirements

- Git
- Docker Desktop, running before you start the client
- A client agent in the `cynapsa-demo` mesh
- A fresh enrollment token for that client agent

Treat the token like a password. Never commit `.env` or paste the token into a
tracked repository file.

## Download only the client

```sh
git clone --filter=blob:none --sparse https://github.com/Cynapsa/cynapsa-demo.git
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

The script downloads and verifies the correct client image, enrolls once, and
saves the encrypted installation profile in the
`cynapsa-demo-client-state` Docker volume. Remove `CYNAPSA_TOKEN` from `.env`
after enrollment.

After the prompt appears, ask a question such as:

```text
Where is the nearest gym to 12201 Park Drive, Hollywood, Florida?
```

Enter `quit` to close the client.

## Later runs

```sh
cd /path/to/cynapsa-demo/client
./run.sh
```

## Update the image

```sh
./run.sh --pull
```

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
- Enrollment fails: generate a fresh token; enrollment tokens are intended for
  first-time use and belong to one identity.
- To force a clean image download, run `./run.sh --pull`.
