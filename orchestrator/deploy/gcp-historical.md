# Historical GCP orchestrator deployment (inactive)

Scaled to zero on 2026-09-27. Image/secrets/revisions are preserved. The active
orchestrator now runs on AWS; see `README.md`. The restart command below is
rollback-only; do not start competing processes against one installation.

Deployed 2026-09-26 in project `aztm-amesh`, region `us-central1`.
Worker pool: `cynapsa-demo-orchestrator`; ready revision:
`cynapsa-demo-orchestrator-00007-xll`; instance count: 1.

Image: `us-central1-docker.pkg.dev/aztm-amesh/cynapsa-v2/cynapsa-demo-orchestrator@sha256:bb7b2274360ca223749edc0e76d27bb2b0187745a33caa830b51b914996cafd2`.
Built from demo `29a33b6`, Core `38f72b0f7fd8ac190fd16be8721bc508841e1f8e`,
and SDK `11146ed796146a281ce4c0ea5cd2dc22e76a83fa`. Runtime: 1 vCPU, 1 GiB.

The old revision has no instance allocation. Existing GCP installation-profile
Secret Manager references remain; the obsolete Gemini secret binding was
replaced with `LITELLM_API_KEY=cynapsa-demo-litellm-api-key:1`.
Gateway/model: `https://litellm.eladrave.com`, `gpt-5.6-terra-high`.
No secret values are stored in this document or image.

The agent reported available mesh connectivity and its expected bare identity.
The live laptop -> GCP orchestrator -> AWS Maps RPC returned a Maps-backed answer;
a follow-up correctly recalled it from conversation history. This is not
production, HA, restart/renewal, or revocation qualification.

## Important persistence limitation

Chat checkpoints are currently on the worker's ephemeral local disk, so memory
does not survive worker replacement. Credential seeds come from existing
Secret Manager versions; changes to the local profile are not synchronized
back to those secrets. Restart/renewal durability remains a separate deployment
task. Do not claim cloud-persistent memory or shared-profile HA, and do not
increase replicas with this profile. No persistent filesystem was provisioned
for the orchestrator during this deployment.

## Stop or restart

```sh
gcloud run worker-pools update cynapsa-demo-orchestrator --project aztm-amesh --region us-central1 --instances=0
gcloud run worker-pools update cynapsa-demo-orchestrator --project aztm-amesh --region us-central1 --instances=1
```

At the original 2026-09-26 deployment, `cynapsa-demo-maps` was at zero instances
and Maps ran on AWS. This was superseded by the 2026-09-27 cutover: Maps now runs
on GCP and the orchestrator on AWS. Preserve old image revisions/secrets for rollback, but never run two
copies using one installation profile. The user explicitly retained the three
unrelated legacy GCP pools. Cloud Run compute, logs, image storage, and secrets
continue to incur their applicable charges.
