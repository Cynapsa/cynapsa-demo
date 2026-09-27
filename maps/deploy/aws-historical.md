# Historical AWS Maps deployment (inactive)

Retired on 2026-09-27: desired/running/pending count zero. Resources, image,
credentials and EFS state are preserved, not deleted. Maps now runs on GCP;
see `README.md`. Restart commands below are rollback-only.

Deployed 2026-09-26 in account `821211778460`, region `us-east-1`.
The cluster is `cynapsa-demo`, service `demo-maps`, task definition `demo-maps:1`.
The included task definition contains resource references, never secret values.

The image was built from demo `29a33b6`, Core `38f72b0f7fd8ac190fd16be8721bc508841e1f8e`,
and SDK `11146ed796146a281ce4c0ea5cd2dc22e76a83fa`.
The ECR digest is pinned in `aws-task-definition.json`; GitHub credentials are
BuildKit-only secrets and are not supplied to the running task.

## Runtime

- One on-demand Fargate task: 0.5 vCPU, 1 GiB, Linux amd64.
- No load balancer, application listener, or task-security-group ingress.
  Public-subnet outbound access is enabled for Cynapsa, Places, and LiteLLM.
  This is not a promise that direct peer connectivity is available.
- Enrollment and API keys are injected from `cynapsa-demo-maps-runtime` in
  Secrets Manager. The execution role accesses only this secret, the image
  repository, and the service log group (ECR authentication needs `*`).
- `/var/lib/cynapsa` uses encrypted EFS `fs-02bb63c19f7412232`, with TLS,
  IAM authorization, and access point `fsap-0feea5bdb47dfe0a2` owned by
  UID/GID 65532 with mode 0700. NFS ingress permits only the task security group.
  Do not run another task or local process against this installation state.
- Logs: `/ecs/cynapsa-demo-maps`, retention 14 days. Application logs may
  contain prompts/diagnostics; restrict access. Additional CloudWatch alarms
  and CloudTrail data-event monitoring are recommended, not configured here.
- Deployment configuration: `maximumPercent=100`, `minimumHealthyPercent=0`.
  Stop-before-start prevents shared credential overlap but causes update downtime.
  Do not increase replicas without independent installation state.

Task, public IPv4, EFS, Secrets Manager, image storage, and logs incur charges.
Scaling the service to zero stops task compute, not all retained-resource costs.

## Operations

Inspect the current service:

```sh
aws ecs describe-services --region us-east-1 --cluster cynapsa-demo --services demo-maps
```

Stop without deleting credentials or resources:

```sh
aws ecs update-service --region us-east-1 --cluster cynapsa-demo --service demo-maps --desired-count 0
```

Restart it:

```sh
aws ecs update-service --region us-east-1 --cluster cynapsa-demo --service demo-maps --desired-count 1
```

For an image change, register a new task definition with an immutable image
digest, then update this service's task definition while retaining the
stop-before-start deployment settings. Secret rotation requires task replacement.
Keep deployment state and rollback image digests; do not delete the EFS volume.

## Live verification

The AWS task reported the expected Maps bare identity and available mesh connectivity.
A laptop client called the GCP orchestrator, which called this Maps task over
Cynapsa and returned a Places-backed gym result. LiteLLM and Places HTTP calls
returned 200. A second prompt recalled the recommended name/address from the
orchestrator's conversation without another search. The temporary client was stopped.
This verifies the exercised path, not HA, direct peer routing, revocation,
credential renewal, or restart recovery.

The old GCP Maps pool remains at zero; its resources were preserved. Three
unrelated legacy GCP pools were explicitly left running.
