# AWS orchestrator deployment

Updated 2026-09-29: account `821211778460`, region `us-east-1`, ECS cluster `cynapsa-demo`,
service `demo-orchestrator`, task definition `demo-orchestrator:2`. Exact inputs
are in `aws-task-definition.json`; it contains references, not secret values.
One on-demand Fargate task: 0.5 vCPU, 1 GiB, Linux amd64, platform 1.4.0.
No HTTP listener/load balancer or task-security-group ingress. Public-subnet
outbound access enables Cynapsa and the model gateway; this does not promise P2P.

Application source `7845b1c` on main. Core
`46b89c89abf129fef1668c86f0130fd715ac0ab6`, SDK
`11146ed796146a281ce4c0ea5cd2dc22e76a83fa` are recorded in the image.
ECR image digest:
`sha256:41cb670f79068e1e4b05ad2ffc32235702ad55f97148a4927658c92bba67d40d`.
GitHub credentials were BuildKit-only secrets, never provided at runtime.

Both `DEMO_MAPS_AGENT_ID` (GCP) and `DEMO_FILES_AGENT_ID` (separately run Files)
are configured. Files/database and client are not deployed by this cloud swap;
start them locally. Destination identities do not change when moving clouds.

## Credentials, memory and access

Secrets Manager `cynapsa-demo-orchestrator-runtime` supplies enrollment and LLM
keys through task-secret JSON references. The execution role has scoped
image-pull, log-write and secret-read access; ECR authentication alone requires
`*`. The task role can mount/write only access point `fsap-030cbcbd58b476d6d`
on encrypted EFS `fs-02bb63c19f7412232`, using IAM and TLS. Its `/orchestrator`
directory is owned by UID/GID 65532, mode 0700. Old Maps EFS state is preserved
at a separate access point. NFS ingress permits only the task security group.
ECS role trust is restricted by source account and regional ECS ARN (specific
cluster SourceArn is unsupported).

Installation credentials persist at `/var/lib/cynapsa`. SQLite uses
`DEMO_MEMORY_DIRECTORY=/var/lib/cynapsa-memory` on local task storage; chat history
resets on replacement. Do not move live SQLite WAL onto NFS/EFS. One task/writer,
stop-before-start deployment (`minimumHealthyPercent=0`, `maximumPercent=100`),
with circuit breaker. Do not increase replicas against one installation state.
Initial enrollment is fresh; old GCP chat/profile state was not migrated.

CloudWatch `/ecs/cynapsa-demo-orchestrator` retains logs 14 days. Restrict access
to private diagnostics. ECR scan-on-push is enabled. Additional CloudWatch alarms
and CloudTrail data-event coverage are recommended, not claimed configured.
Task compute, public IPv4, secrets/images/logs and retained EFS data incur costs,
including retained-resource charges after scale-down.

## Operations

```sh
aws ecs describe-services --region us-east-1 --cluster cynapsa-demo --services demo-orchestrator
aws ecs update-service --region us-east-1 --cluster cynapsa-demo --service demo-orchestrator --desired-count 0
aws ecs update-service --region us-east-1 --cluster cynapsa-demo --service demo-orchestrator --desired-count 1
```

Register a new digest-pinned task definition for image changes, then update the
service with the existing stop-before-start settings. Secret rotation requires
replacement. Preserve EFS state. GCP orchestrator remains at zero; its historical
configuration is in `gcp-historical.md`.

## Cutover verification

On 2026-09-27, ECS reported the orchestrator deployment `COMPLETED`, with
desired/running/pending counts 1/1/0. AWS Maps was 0/0/0; GCP Maps had one
instance and a ready revision, and GCP orchestrator had zero instances.
A real laptop client requested a Fort Lauderdale cafe: the AWS orchestrator
called GCP Maps and returned a cafe name, street address and Google Maps sources.
A follow-up in the same conversation recalled the name/address correctly.
The temporary client was stopped afterward. This is a live cross-cloud smoke
test, not restart, revocation, HA or transport-rank qualification. Files was not
running during this check; the live Files-to-Maps chain remains to be exercised
with the separately started local Files agent.

## 2026-09-29 logging rollout

Revision `demo-orchestrator:2` uses the new image and retains the same secrets,
EFS access point, one-writer deployment policy, task sizing and network setup.
It logs Cynapsa RPC arrival, downstream submission/response, and handler return
without request/response bodies or credentials. Successful model HTTP access
logs are suppressed; warnings/errors remain. The task reported available mesh
connectivity. A live laptop client received a Maps-backed coffee shop/address
through AWS orchestrator -> GCP Maps, and both agents emitted Cynapsa traffic
records. That verifies one request, not recovery or high availability. The
temporary laptop client was stopped. Chat memory on task-local SQLite resets
on task replacement; the EFS installation profile is retained.
