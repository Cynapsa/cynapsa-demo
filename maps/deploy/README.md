# GCP Maps deployment

Migrated 2026-09-27: project `aztm-amesh`, region `us-central1`, worker pool
`cynapsa-demo-maps`, one instance. Revision `cynapsa-demo-maps-00007-v94`.

Image:
`us-central1-docker.pkg.dev/aztm-amesh/cynapsa-v2/cynapsa-demo-maps@sha256:e54df57a89a500f51ce21ed357a8424e8098c57b51e6240b7bea25356193274d`.
Application source `b149199`, merged into main by `1f1a888`.
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

The worker enrolls a new installation when its ephemeral filesystem is empty.
Profiles are not on a persistent volume. Replacement can require another
enrollment, so keep the grant valid and with available installation capacity.
Do not claim durable profile renewal or HA. One replica only.

Old AWS `demo-maps` desired/running/pending count is zero. Rollback resources
are preserved (`aws-historical.md`, `aws-task-definition.json`). GCP orchestrator
remains at zero; three unrelated legacy GCP pools are untouched. Runtime compute,
logs, secrets and retained images continue to incur applicable charges.

## Operations

```sh
gcloud run worker-pools describe cynapsa-demo-maps --project aztm-amesh --region us-central1
gcloud run worker-pools update cynapsa-demo-maps --project aztm-amesh --region us-central1 --instances=0
gcloud run worker-pools update cynapsa-demo-maps --project aztm-amesh --region us-central1 --instances=1
```

Startup reported the expected Maps bare identity and available mesh connectivity.
Live request verification is recorded in the orchestrator runbook after cutover.
