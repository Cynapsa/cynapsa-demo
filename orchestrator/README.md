# Demo orchestrator

This identity accepts `/ask` RPCs from the demo client. Gemini decides whether
to answer directly or call the maps identity over Cynapsa.

Run locally with Docker:

```sh
./run.sh
```

The first run prompts for a Gemini API key and a portal enrollment token. The
API key is stored in ignored `.private/gemini_api_key` with mode `0600`; the
one-time enrollment token is deleted after the process exits. Encrypted Cynapsa
state persists in the `cynapsa-demo-orchestrator-state` Docker volume.

The deployed Cloud Run worker pool is named `cynapsa-demo-orchestrator` in
project `aztm-amesh`, region `us-central1`. Its secrets and installation state
are provided through Secret Manager and are not part of this repository.
