# Demo maps agent

This identity exposes a wildcard native Cynapsa handler and uses Gemini function
calling with Google Places (New) to answer place questions.

Run locally with Docker:

```sh
./run.sh
```

The first run prompts for Gemini, Google Maps, and portal enrollment credentials.
API keys are retained only in ignored mode-`0600` files under `.private/`; the
temporary enrollment token is deleted after exit. Encrypted Cynapsa state
persists in the `cynapsa-demo-maps-state` Docker volume.

The deployed Cloud Run worker pool is named `cynapsa-demo-maps` in project
`aztm-amesh`, region `us-central1`. Its secrets and state are supplied through
Secret Manager and are not committed here.
