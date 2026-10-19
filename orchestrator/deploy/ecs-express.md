# Orchestrator HTTP endpoint without Cloudflare

The `demo-orchestrator-http` ECS Express Gateway service in cluster
`cynapsa-demo` (AWS account `821211778460`, `us-east-1`) provides an AWS-owned,
TLS-protected origin:

`https://de-4f1d206529514541bb8cb316c5b0b51e.ecs.us-east-1.on.aws`

`GET /health` is public for health checks. `POST /ask` requires the private
`x-demo-access-key` header. The key is stored in AWS Secrets Manager JSON key
`http_access_key` in `cynapsa-demo-orchestrator-runtime`; the matching GCP Maps
key is Secret Manager `cynapsa-demo-http-access-key`, version 2. The key is not
in the repository or image. These public URLs do not require Cloudflare or a
custom DNS record, but the shared key is a demo guard, not rate limiting or
production-grade authentication.

The Express service uses task definition family `demo-orchestrator-express`,
0.5 vCPU, 1 GiB RAM, one task, port 8000, `/health`, and the same EFS access
point as the original ECS service for Cynapsa installation state. Chat memory
is task-local. The ALB idle timeout is 180 seconds for LLM requests. Its HTTP
run command is:

```sh
uvicorn app:app --host 0.0.0.0 --port 8000 --no-access-log
```

The original `demo-orchestrator` ECS service is separate and still runs in
Cynapsa mode. It does not receive traffic from this HTTPS endpoint. Before
switching the Express service itself to Cynapsa mode, scale the original
service to zero; two simultaneous Cynapsa processes must not share one saved
installation profile. The Maps Cloud Run service must use the same mode as the
Orchestrator for the cross-cloud request in this demonstration.

For the laptop client, set `DEMO_ORCHESTRATOR_ORIGIN` to the URL above and
`DEMO_RUN_COMMAND=python /app/app.py` for direct HTTP. Supply the shared key as
`DEMO_HTTP_ACCESS_KEY` in the client environment; do not commit it. The same
client source can instead use `DEMO_RUN_COMMAND=cynapsa run -- python /app/app.py`
when Orchestrator and Maps are in Cynapsa mode.

To inspect the Express service:

```sh
aws ecs describe-express-gateway-service --region us-east-1 \
  --service-arn arn:aws:ecs:us-east-1:821211778460:service/cynapsa-demo/demo-orchestrator-http
aws ecs list-service-deployments --region us-east-1 \
  --cluster cynapsa-demo --service demo-orchestrator-http
```

Express updates use a canary traffic shift, so a new task passing its health
check is not enough to declare the public URL ready. Wait for the deployment
to complete and verify both authenticated and unauthenticated requests. Keep
the old service and its persistent data until explicitly retired.

## Live HTTP check, 2026-10-16

The Express deployment completed successfully. TLS verification and
`GET /health` passed; `POST /ask` without the demo key returned 401. An
authenticated direct request returned 200 via AWS Orchestrator → GCP Maps.
The rebuilt laptop client also returned a Maps-backed cafe answer through
ordinary Requests. Earlier retries saw a transient Cloud Run 502 and a Maps
model-gateway 503; the latter coincided with Maps' 25-second model timeout.
The Maps image was rebuilt with a 45-second timeout, and the next client
question passed. This is a smoke test, not a reliability or load test.
