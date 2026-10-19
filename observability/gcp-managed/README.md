# GCP-managed telemetry sidecar (staging)

The Collector receives opt-in Cynapsa OTLP traffic on loopback and forwards
metrics, traces, and sanitized SDK events to the Google Cloud Telemetry API.
Cloud Run worker pools support same-instance sidecars; the main container and
Collector share a network namespace. This path does not open a public telemetry
gateway or run a self-hosted Prometheus/Tempo/Loki stack.

Use Collector `0.140.0` or newer for OTLP metrics ingestion into Cloud
Monitoring. Set `GOOGLE_CLOUD_PROJECT=aztm-amesh` on the sidecar. Grant the
worker-pool service account `roles/telemetry.writer` and
`roles/serviceusage.serviceUsageConsumer` on the target project. The APIs
`telemetry.googleapis.com` and `monitoring.googleapis.com` must be enabled.
The sidecar authenticates with Application Default Credentials through
`googleclientauth`; it does not carry a service-account key.

For the application container, opt in explicitly with loopback OTLP endpoints
for Core metrics/traces and SDK metrics/events. The current exporters label
plaintext HTTP opt-in `*_INSECURE_DEV=1`; because this is a staging-only
same-instance loopback hop, no network peer can reach it. This is **not** the
TLS-backed production edge described in `../production/README.md`.

The sidecar queue is in memory. A worker-pool restart can lose buffered
telemetry; it does not lose Cynapsa messages. Do not claim this is durable or
production-complete. Verify actual GCP metric names, trace samples, and
sanitized logs before enabling alert policies. No alert notification receiver
is configured yet.

Official references:

- https://docs.cloud.google.com/run/docs/deploy-worker-pools#deploy_sidecar
- https://docs.cloud.google.com/stackdriver/docs/otlp-metrics/deploy-collector
- https://docs.cloud.google.com/stackdriver/docs/otlp/overview
