# Demo maps agent

This identity exposes a wildcard native Cynapsa handler and uses an
OpenAI-compatible LiteLLM gateway with Google Places (New) to answer place questions.

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
The Cloud Run worker pool `cynapsa-demo-maps` is disabled at zero instances.
For a new local installation, use an empty state volume and an active
enrollment grant whose mesh scope and installation limit allow it. The
existing volume uses its saved installation proof, not `CYNAPSA_TOKEN`.
After mesh removal and re-addition, that proof is revoked. Supply a still-valid
token and run `./run.sh --force-enroll` to replace the installation through
the bundled `cynapsa run --force-enroll` CLI in the same selected volume. The
CLI session closes before the native maps agent starts from the new profile.
The bundled SDK includes force-enroll and native remote-error handling. After
changing bundled source, rebuild with `./run.sh --build` and restart any
running container; subsequent runs reuse the rebuilt image.
