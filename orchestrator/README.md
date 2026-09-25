# Demo orchestrator

This identity accepts `/ask` RPCs from the demo client. An OpenAI-compatible
LiteLLM gateway decides whether to answer directly or call the maps identity
over Cynapsa.
Downstream native and canonical remote errors are logged with their actual
status, code, message, and public details in this agent's terminal. The client
receives only the generic `The demo could not answer` RPC error.

The directory is independently runnable and includes its own pinned Python SDK
and Go Core source. It does not use repository-root files, sibling directories,
or external SDK/Core checkouts.

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
The Cloud Run worker pool `cynapsa-demo-orchestrator` is disabled at zero
instances. For a new local installation, use an empty state volume and an
active enrollment grant whose mesh scope and installation limit allow it. The
existing volume uses its saved installation proof, not `CYNAPSA_TOKEN`.
After mesh removal and re-addition, that proof is revoked. Supply a still-valid
token and run `./run.sh --force-enroll` to replace the installation through
the bundled `cynapsa run --force-enroll` CLI in the same selected volume. The
CLI session closes before the native orchestrator starts from the new profile.
The bundled SDK includes force-enroll and native remote-error handling. After
changing bundled source, rebuild with `./run.sh --build` and restart any
running container; subsequent runs reuse the rebuilt image.
