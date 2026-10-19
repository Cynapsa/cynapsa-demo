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

To switch to Cynapsa mode:

```sh
gcloud run services update cynapsa-demo-maps-service \
  --project aztm-amesh --region us-central1 \
  --update-env-vars='DEMO_RUN_COMMAND=cynapsa run --config /tmp/cynapsa-private/cynapsa.json -- app:app'
```

Current mode (ordinary HTTP, as of 2026-10-16):

```sh
gcloud run services update cynapsa-demo-maps-service \
  --project aztm-amesh --region us-central1 \
  --update-env-vars='DEMO_RUN_COMMAND=uvicorn app:app --host 0.0.0.0 --port 8000 --no-access-log'
```

Each update creates a new revision. In Cynapsa mode, `/maps` is not exposed
over HTTP; a small built-in listener serves only `GET /health` for Cloud Run.
In HTTP mode, Uvicorn serves `/maps` and the agent is not connected to the
mesh. Do not run another Maps instance with the same installation credentials.

The service now permits unauthenticated Cloud Run invocation because an
organization policy prevented the usual `allUsers` IAM grant. Its application
requires the shared `x-demo-access-key` header on all paths except `/health`;
the value lives in Secret Manager secret `cynapsa-demo-http-access-key`, version
2. The AWS Orchestrator has the same key in Secrets Manager. This is a demo
guard, not rate limiting or production-grade authentication. Monitor paid
Maps/LLM use.

## Live validation, 2026-10-16

The Cloud Run service became Ready in both modes. In Cynapsa mode,
authenticated `GET /health` returned 200 and `POST /maps` over HTTP returned
404. Cynapsa-routed FastAPI responses of 400, 592-byte JSON 200, and
3,012-byte HTML 200 reached a local client. A full client → AWS Orchestrator →
GCP Maps question reached Maps, which logged a generated answer, but the
Orchestrator timed out waiting for that reply. A direct successful Maps request
from the client also timed out. This was in Cynapsa mode; it does not qualify
the new ordinary-HTTP mode. Do not infer a simple size limit from these
observations. Investigate the successful application reply path across the
SDK, Go Core, and mesh server separately.

## HTTP rollout, 2026-10-16

The current revision `cynapsa-demo-maps-service-00005-hkp` serves 100% of
traffic from image digest `sha256:0d429801c769fac4aa93b2919046c540742196a209d4f22a9d00b29fea398e7c`.
The model HTTP timeout was raised from 25 to 45 seconds after an intermittent
`model_unavailable` response; logs now record only the transport exception
class if the gateway fails. A client → AWS Orchestrator → GCP Maps HTTP
question then returned 200 with a cafe answer and Google Maps sources.
Earlier requests saw a transient 502 and 503, so this one success does not
establish long-term reliability.
