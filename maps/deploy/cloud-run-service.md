# Maps on Cloud Run service

The `cynapsa-demo-maps-service` in GCP project `aztm-amesh`, region
`us-central1`, uses the same Maps image in both modes. Its stable origin is:

`https://cynapsa-demo-maps-service-144701547770.us-central1.run.app`

In the AWS Orchestrator task definition, `DEMO_MAPS_ORIGIN` is this exact
origin. The Cynapsa CLI maps it to `DEMO_MAPS_AGENT_ID`; ordinary HTTP calls
use the same origin directly. Neither the URL nor the application code needs
to change when switching modes.

The service has one minimum and one maximum instance, CPU available between
HTTP requests, port 8000, the `cynapsa-demo-workers` service account, and
Secret Manager references for enrollment, LiteLLM, and Google Maps keys.
The old `cynapsa-demo-maps` worker pool is retained at zero instances.

Current mode (Cynapsa):

```sh
gcloud run services update cynapsa-demo-maps-service \
  --project aztm-amesh --region us-central1 \
  --update-env-vars='DEMO_RUN_COMMAND=cynapsa run --config /tmp/cynapsa-private/cynapsa.json -- app:app'
```

Plain HTTP mode:

```sh
gcloud run services update cynapsa-demo-maps-service \
  --project aztm-amesh --region us-central1 \
  --update-env-vars='DEMO_RUN_COMMAND=uvicorn app:app --host 0.0.0.0 --port 8000 --no-access-log'
```

Each update creates a new revision. In Cynapsa mode, `/maps` is not exposed
over HTTP; a small built-in listener serves only `GET /health` for Cloud Run.
In HTTP mode, Uvicorn serves `/maps` and the agent is not connected to the
mesh. Do not run another Maps instance with the same installation credentials.

The Cloud Run service currently requires IAM-authenticated invocation. A
plain HTTP client on another cloud cannot call it without an authorized
identity and suitable token. Do not enable public access to the paid Maps/LLM
endpoint without an explicit security and abuse-cost decision. Likewise,
the AWS Orchestrator has no stable public HTTP ingress yet; the complete
plain-HTTP demonstration needs that endpoint separately.

## Live validation, 2026-10-16

The Cloud Run service became Ready in both modes. In Cynapsa mode,
authenticated `GET /health` returned 200 and `POST /maps` over HTTP returned
404. Cynapsa-routed FastAPI responses of 400, 592-byte JSON 200, and
3,012-byte HTML 200 reached a local client. A full client → AWS Orchestrator →
GCP Maps question reached Maps, which logged a generated answer, but the
Orchestrator timed out waiting for that reply. A direct successful Maps request
from the client also timed out. This is **not yet an end-to-end passing demo**;
do not infer a simple size limit from these observations. Investigate the
successful application reply path across the SDK, Go Core, and mesh server.
