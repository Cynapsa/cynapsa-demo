# GCP Maps deployment

Updated 2026-09-29: project `aztm-amesh`, region `us-central1`, worker pool
`cynapsa-demo-maps`, one instance. Revision `cynapsa-demo-maps-00009-5hj`.

Image:
`us-central1-docker.pkg.dev/aztm-amesh/cynapsa-v2/cynapsa-demo-maps@sha256:baa453fa2581b8f98238bbbc9bca38d12c00a6f183b9f262341dd5af76a83e7f`.
Application source `6e442e1` on main.
Core `46b89c89abf129fef1668c86f0130fd715ac0ab6`, SDK
`11146ed796146a281ce4c0ea5cd2dc22e76a83fa` are recorded in the image.

Runtime Secret Manager references (no secret values in source/image):

- `CYNAPSA_TOKEN`: `cynapsa-demo-maps-enrollment:1`.
- `LITELLM_API_KEY`: `cynapsa-demo-litellm-api-key:1`.
- `GOOGLE_MAPS_API_KEY`: `cynapsa-demo-google-maps-api-key:1`.

Service account `cynapsa-demo-workers@aztm-amesh.iam.gserviceaccount.com`.
Mesh `cynapsa_demo_cynapsa_demo`, model `gpt-5.6-terra-high`, gateway
`https://litellm.eladrave.com`. Gemini and old credential-seed bindings were
removed from this revision; their secret resources were not deleted.
`DEMO_ORCHESTRATOR_AGENT_ID` labels the authenticated incoming peer in simple
request/response logs. Full content is logged without redaction or truncation.

The worker enrolls a new installation when its ephemeral filesystem is empty.
Profiles are not on a persistent volume. Replacement can require another
enrollment, so keep the grant valid and with available installation capacity.
Do not claim durable profile renewal or HA. One replica only.

AWS `demo-maps` was scaled to zero on 2026-09-27; on 2026-09-29, ECS reports
that service as `MISSING`. Historical configuration files remain in this repo
(`aws-historical.md`, `aws-task-definition.json`), but a live AWS Maps service is
not available for immediate rollback. The old GCP orchestrator was left at zero;
three unrelated legacy GCP pools were untouched. Runtime compute,
logs, secrets and retained images continue to incur applicable charges.

## Operations

```sh
gcloud run worker-pools describe cynapsa-demo-maps --project aztm-amesh --region us-central1
gcloud run worker-pools update cynapsa-demo-maps --project aztm-amesh --region us-central1 --instances=0
gcloud run worker-pools update cynapsa-demo-maps --project aztm-amesh --region us-central1 --instances=1
```

Startup reported the expected Maps bare identity and available mesh connectivity.
Live request verification is recorded in the orchestrator runbook after cutover.
On 2026-09-29, a laptop client asked the AWS orchestrator for a coffee shop;
the GCP Maps agent handled its `/maps` RPC and the client received an address
and Maps citations. The new logs show request arrival and handler return while
successful model/Places HTTP access logging stays quiet. This is a single
cross-cloud smoke test, not transport, HA or renewal qualification.

After revision `00009-5hj`, a live client request appeared in AWS and Maps
traffic logs with friendly names, full IDs and content. The new revision is
ready and reports available mesh connectivity. However, the client returned an
authorization error; Maps logged repeated arrivals and handler returns, while
the orchestrator later timed out waiting for Maps. The new end-to-end path is
not yet verified. Repeating this test can invoke paid APIs multiple times.
