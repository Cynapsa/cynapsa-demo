# Production observability reference (configuration only)

These are **templates, not a deployed stack**. Nothing here changes the sibling
local `observability/` setup. Choose **one** cloud for the central backends;
agents on GCP and AWS can both send to it. The topology is:

```text
Core / SDK / optional mesh adapter --HTTPS loopback--> per-workload Collector
  --OTLP/HTTP mTLS, persistent queue--> central Collector (private backends)
    --scrape--> Prometheus on encrypted persistent disk
    --OTLP/HTTP, persistent queue--> Tempo (S3 or GCS, 7-day query target)
    --OTLP/HTTP, persistent queue--> Loki (S3 or GCS, 7-day query target)
Prometheus --mTLS scrape--> each edge Collector and queue-disk exporter
Prometheus --rules--> operator-selected Alertmanager/notifier
```

The [edge](edge-collector.yaml), [gateway](gateway-collector.yaml),
[Prometheus](prometheus.yml), and [alerts](alerts.yml) files are separate
deployable inputs. Pick one Tempo and one Loki file under `storage/` for the
central cloud. Do not start these templates with the local Compose file.

## Security and identity decisions

- The **agent-to-edge** listener is HTTPS on `127.0.0.1:4318`, reachable only
  in the workload's network namespace. Issue a local server certificate with
  an IP SAN for `127.0.0.1` from an internal CA and add that CA to the agent
  container's OS trust store. Do not turn on the exporters' `*_INSECURE_DEV`
  flags in production. The producer has no custom auth-header/client-cert
  support, so loopback network isolation is the trust boundary at this hop.
  Do not publish this port or share the network namespace with untrusted code.
- The **edge-to-gateway** connection uses mTLS. `gateway-ca.crt` verifies the
  server name in `OTEL_GATEWAY_HOST`; `agent-ca.crt` limits clients to an
  operator-controlled issuing CA. Issue separate short-lived client certs per
  workload, rotate and revoke them, protect keys in a secret manager/volume,
  and restrict ingress to approved GCP/AWS egress ranges or private
  interconnect. Use a TCP/TLS-passthrough load balancer; do not terminate TLS
  before the Collector unless equivalent client identity is re-established.
  mTLS authenticates the edge certificate, **not** the self-reported
  `cynapsa.role` resource attribute; compromised authorized clients can spoof
  that label. Enforce certificate-to-role binding at an authenticated ingress
  proxy if role integrity is a requirement.
- Only expose gateway `4318` to the cross-cloud agent path. Gateway `8888`
  and `9464`, Prometheus, Tempo, Loki, and Grafana must be on private networks
  with least-privilege firewall rules. Collector internal metrics readers bind
  to `0.0.0.0:8888` for container-network scraping; this is **not** permission
  to publish port `8888`. Gateway-to-backend HTTP in this example assumes a
  trusted, isolated internal network; use backend TLS and auth where that
  boundary is not acceptable. Secure Grafana separately (SSO/RBAC/TLS).
- Do not forward arbitrary application logs: these demo apps may log full
  prompts/responses. Only the explicitly sanitized SDK events/logs are
  appropriate for this Loki pipeline; review each added producer for PII,
  tokens, and high-cardinality attributes. Never place secrets in labels.

This reference requires direct, private, per-instance mTLS scraping of each
edge proxy and a durable queue. On GCP, use an addressable GKE pod or Compute
Engine instance with persistent storage and private routing. **Do not use
this reference unchanged for Cloud Run services or worker pools**, including
the current Maps worker pool. A Cloud Run service does not expose the direct
per-instance sidecar target needed for this mTLS scrape design. Worker pools
may offer private per-instance IPs and NFS mounts, but this reference does not
specify or validate their scrape routing, queue persistence, or lifecycle;
a separate worker-pool design is required before deploying telemetry beside
that workload. On AWS, an addressable ECS task or EKS pod can
host the edge, but task/pod ephemeral storage is likewise not durable across
replacement; attach persistent storage or use a managed telemetry gateway.
Keep `CYNAPSA_ROLE` to a stable, low-cardinality name
(`maps`, `orchestrator`, `files`, `client`, or `mesh-server`) and
`CLOUD_PROVIDER` to `gcp` or `aws`. The edge process needs its own writable,
private `/var/lib/otelcol/queue`; do not share that directory between replicas.

Each edge needs a direct, independent scrape path so an exporter outage does
not hide its own diagnostics. [Prometheus](prometheus.yml) scrapes
`/collector/metrics` and `/disk/metrics` through an
[mTLS edge proxy](edge-scrape-proxy.conf) at port `9443`; the proxy forwards
only to local Collector `:8888` and local node_exporter `:9100`. Restrict
`9443` to central Prometheus over private, encrypted routing, and firewall
raw `8888`/`9100` from remote clients. Provision client/server certificates
and trust bundles outside this tree. The [edge inventory](edge-scrape-targets.yml)
contains example DNS names and roles only: replace it with **every** expected
continuously running edge instance and keep it current as replicas change.
The disk exporter must see the actual queue volume mounted at
`/var/lib/otelcol/queue` in its own mount namespace; verify that the exact
`node_filesystem_*{mountpoint="/var/lib/otelcol/queue"}` series exists. If
that mount is unavailable, supply an equivalent independently scraped
free/total-byte source and adjust the rules. The private path, proxy, and
disk exporter are deployment prerequisites, not provisioned here.

Example producer endpoint values (set only for exporters deliberately enabled):

```dotenv
CYNAPSA_CORE_OTLP_METRICS_ENDPOINT=https://127.0.0.1:4318/v1/metrics
CYNAPSA_CORE_OTLP_TRACES_ENDPOINT=https://127.0.0.1:4318/v1/traces
CYNAPSA_SDK_OTLP_METRICS_ENDPOINT=https://127.0.0.1:4318/v1/metrics
CYNAPSA_SDK_OTLP_EVENTS_ENDPOINT=https://127.0.0.1:4318
```

The mesh-server adapter takes `--endpoint
https://127.0.0.1:4318/v1/metrics` and must run on a trusted operator host
or in an isolated network namespace beside its edge Collector. It does not
support client auth headers/certs; never give a telemetry container the Docker
socket or mesh runtime credentials. Its administrative permissions remain in
force. If the broker/adapter is not in use, remove its alerts.

## Persistence and retention

`edge-collector.yaml` persists network-export batches to a private filesystem
queue. The gateway persists **trace/log** export batches. Size and monitor
these volumes against peak ingest and longest tolerated outage; queue size is
in batches, not bytes. The pinned file-storage extension has no configured
aggregate byte ceiling here. The node-exporter rule warns below 15% free,
but neither it nor the queue batch limit prevents disk-full. Set an actual
volume/filesystem quota and reserve enough absolute free bytes for peak
ingest, compaction, and the longest tolerated outage; a full disk rejects
writes and can lose telemetry. The queues buffer recoverable network/backend
outages, but are finite and do not make ingestion lossless. If the edge cannot accept
a sample, the Core/SDK exporters can drop it before any queue, and their drop
gauges only reveal failures after a later successful export. Gateway metrics
use a Prometheus scrape exporter, so samples between scrapes can be missed if
the gateway dies or a network partition occurs. For stricter metrics delivery
or multi-zone HA, replace this single-gateway scrape path with a tested
remote-write/HA architecture before rollout.

Run Prometheus with an encrypted, persistent TSDB disk; set both
`--storage.tsdb.retention.time=30d` and a capacity-derived
`--storage.tsdb.retention.size` (for example `100GB`, **not** a universal
value). Back up the TSDB with Prometheus snapshots and test restore. Do not
share one TSDB volume between concurrent Prometheus replicas. The Prometheus
file does not provision the disk or flags. Tempo's WAL and Loki's active index,
cache, and compactor work directories also require durable, private writable
volumes; object storage alone does not protect in-flight data. Tempo
`block_retention: 168h` and Loki `retention_period: 168h` target roughly seven
days of **queryable** trace/log data when compaction and retention run. They
are not physical-erasure deadlines: deletion is asynchronous, local WAL/cache
and backups can outlive the query window, and object-store recovery features
can retain deleted bytes. With S3 versioning, deletion can create a delete
marker while noncurrent versions remain until a separately reviewed
`NoncurrentVersionExpiration` rule. GCS soft delete retains deleted objects
for its configured recovery period, and versioning can keep noncurrent
generations. Choose the physical-deletion, backup, legal-hold, and cost
policy separately; do not claim seven-day erasure from these YAML files.
Keep dedicated buckets, encryption at rest, least-privilege bucket access,
and a reviewed restore/deletion policy. Use AWS workload IAM for S3 or GCP
Workload Identity/ADC for GCS, not keys in these files. Loki retention
requires its singleton compactor and 24-hour TSDB index period. Do **not**
apply a blanket bucket lifecycle deletion rule to Loki's shared index/chunk
bucket: it can remove index or cluster-state objects before Loki safely
retires them. If a lifecycle safety net is required, scope it to verified
per-tenant chunk prefixes only and set expiry beyond retention plus compactor
deletion delay. A strict physical-erasure SLA needs a separately reviewed
bucket/version/soft-delete policy. These files show single-instance backends
(`replication_factor: 1`), not a highly available production deployment.

## Alert semantics and operator edits

The expressions in [alerts.yml](alerts.yml) use the metric names observed
through the **pinned** Collector Prometheus exporter in the local stack.
Both local and gateway Prometheus exporters set `add_metric_suffixes: false`:
without it, unit-`1` mesh gauges acquire a `_ratio` suffix and the mesh
alert queries silently match no series. A live scrape confirmed the
unsuffixed `cynapsa_mesh_server_mesh_available` series after this setting.
Collector `0.123.0` ignores `service.telemetry.metrics.address`; both
Collectors now use explicit `metrics.readers` with
`without_type_suffix: true` and `without_units: true`, so internal
`otelcol_*` counter names in these rules do not gain `_total` or unit
suffixes. Verify names and labels from a real scrape before enabling
notifications. `resource_to_telemetry_conversion` exposes the edge-injected
`cynapsa.role` as `cynapsa_role`. The producer `service.name` becomes `job`
and its random per-process `service.instance.id` becomes `instance`. Do not
pin an `instance` in no-data rules. Preserve `honor_labels: true`.

| Alert | What it proves | What it does not prove |
| --- | --- | --- |
| Core connectivity failures | A positive adjacent change in the per-Core RPC-timeout or outbox-expiry **snapshot gauge** was scraped in the last five minutes. | Not a Prometheus counter/rate, a handshake failure count, peer identity, or end-to-end delivery failure. A reset plus new events between scrapes can be missed. |
| Core/SDK telemetry drops | A positive adjacent change in a process-local export-drop gauge was later exported successfully. | Not proof that every lost sample was observed; a totally broken exporter may instead trigger no-data. |
| Mesh-server unavailable | The adapter reported `authority_ready=0` for two minutes (failed poll **or** not-ready). | Not proof that all mesh routing is down; readiness is only one administrative signal. |
| Mesh command unavailable | `mesh_available=0` for two minutes; the individual mesh event/session values are omitted. | Not the same as `authority_ready=0`; an older server without the command also triggers it. |
| Mesh-server telemetry failures | Positive change in adapter poll-failure or export-drop gauge. | Not a durable counter across adapter restarts. |
| No data | No matching snapshot for an **expected** role/server in a two-minute range, plus one minute pending. The gateway exporter first retains the last OTLP snapshot for up to one minute. | From the **last successful update**, detection is roughly four minutes plus scrape/evaluation jitter, not three minutes. Never-seen series can alert sooner. Opt-in disabled, exporter, ingress, scrape, or storage can all cause it. |
| Gateway/edge/backend unavailable | Prometheus target scrape `up == 0`. | Not an external user-path health check. |
| Collector failures/queue | Gateway or directly scraped edge internal counter rose, or a sending queue is >80% of batch capacity. | Does not measure producer-side drops or guarantee disk headroom; disk alerts and volume quotas are separate. |
| Edge queue disk | The independently scraped queue filesystem has <15% free, its scrape fails, or the expected mount series is missing. | Not a byte cap or evidence that the queue can survive a particular outage; verify absolute available bytes. |

The only prelisted always-on agents are Maps and Orchestrator. Copy the
no-data rule for **each** additional required role and cloud/deployment
combination; remove rules for optional/scale-to-zero roles. A role-level
no-data alert fires only when **all** replicas of that role vanish. Per-edge
`up` alerts catch an expected edge target disappearing only while the target
inventory remains accurate; they do not prove its application is healthy.
Use a separate expected-replica inventory and workload health checks if
partial application replica loss matters. The mesh-server alert assumes its
adapter is continuously enabled. Gauge-based
change detection deliberately uses `idelta` on adjacent gauge snapshots,
never `rate()`/`increase()` on Cynapsa gauges. Collector internal
`otelcol_*` metrics are actual counters/gauges and can use `rate()` on
counters. If an edge scrape fails, failure/refusal/queue series can vanish;
its `up == 0` alert is the fallback. Prometheus cannot report its own total
outage, so keep an independent external check.

The mesh-server adapter now defines metrics named
`cynapsa.mesh_server.auth_accepted_count`, `auth_rejected_count`,
`bind_accepted_count`, `bind_rejected_count`, `route_accepted_count`,
`route_rejected_count`, `delivery_accepted_count`,
`delivery_rejected_count`, `offline_storage_blocked_count`,
`control_issued_count`, `control_delivery_failed_count`,
`control_packet_acked_count`, `control_packet_rejected_count`,
`control_applied_count`, `control_action_timeout_count`,
`active_sessions`, and `pending_control_actions` (the common prefix applies
to each name), plus `mesh_available`. [Mesh rules](alerts-mesh.yml) use the
route/delivery rejection and control-failure snapshots and alert when the
mesh command reports unavailable. They are loaded by `prometheus.yml`.
Auth/bind rejections may be normal policy outcomes, so they are not paged
blindly. `offline_storage_blocked_count`
reflects the deliberately disabled generic offline-storage hook; it is **not**
queue depth. No offline queue-depth alert or panel is defined.

No notification destination is invented here. Configure Prometheus
`alerting.alertmanagers` and an authenticated, TLS-protected Alertmanager
route (PagerDuty/incident system/email as appropriate), secret delivery via
the platform secret manager, grouping, silences, and receiver ownership.
Prometheus is a single failure domain for its own alerts: add an independent
external availability/synthetic check for gateway, Prometheus, and mesh
connectivity. A healthy `authority_ready` gauge is not a full server SLO.

## Validation and promotion checklist

The files can be parsed and checked offline with the pinned local binaries:

```sh
docker run --rm --entrypoint /bin/promtool \
  -v "$PWD/alerts.yml:/etc/prometheus/alerts.yml:ro" \
  prom/prometheus:v3.2.1 check rules /etc/prometheus/alerts.yml
docker run --rm --entrypoint /bin/promtool \
  -v "$PWD/alerts-mesh.yml:/etc/prometheus/alerts-mesh.yml:ro" \
  prom/prometheus:v3.2.1 check rules /etc/prometheus/alerts-mesh.yml
docker run --rm --entrypoint /bin/promtool \
  -v "$PWD/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
  -v "$PWD/alerts.yml:/etc/prometheus/alerts.yml:ro" \
  -v "$PWD/alerts-mesh.yml:/etc/prometheus/alerts-mesh.yml:ro" \
  prom/prometheus:v3.2.1 check config --syntax-only /etc/prometheus/prometheus.yml
docker run --rm --entrypoint /bin/promtool \
  -v "$PWD:/work:ro" -w /work \
  prom/prometheus:v3.2.1 test rules alerts.test.yml
```

For the pinned Collector, Loki, and Tempo binaries, validate the chosen
variants (replace the S3 filename with its GCS counterpart when appropriate):

```sh
docker run --rm --network none --entrypoint /otelcol-contrib \
  -e CYNAPSA_ROLE=maps -e CLOUD_PROVIDER=gcp \
  -e OTEL_GATEWAY_HOST=collector.example.test \
  -v "$PWD/edge-collector.yaml:/config.yaml:ro" \
  otel/opentelemetry-collector-contrib:0.123.0 validate --config=/config.yaml
docker run --rm --network none --entrypoint /otelcol-contrib \
  -v "$PWD/gateway-collector.yaml:/config.yaml:ro" \
  otel/opentelemetry-collector-contrib:0.123.0 validate --config=/config.yaml
docker run --rm --network none --entrypoint /tempo \
  -e TEMPO_BUCKET=validation-only -e AWS_REGION=us-east-1 \
  -v "$PWD/storage/tempo-s3.yaml:/config.yaml:ro" \
  grafana/tempo:2.7.2 -config.file=/config.yaml \
  -config.expand-env=true -config.verify=true
docker run --rm --network none --entrypoint /usr/bin/loki \
  -e LOKI_BUCKET=validation-only -e AWS_REGION=us-east-1 \
  -v "$PWD/storage/loki-s3.yaml:/config.yaml:ro" \
  grafana/loki:3.4.2 -config.file=/config.yaml \
  -config.expand-env -verify-config
```

The Prometheus `--syntax-only` check skips referenced TLS files and target
inventory. A full `promtool check config` needs the inventory and temporary
test certificates mounted at the configured paths; both were used during
this review. Check the NGINX proxy with `nginx -t` and non-production test
certificates. These checks parse configuration; they do not prove availability
or security. Then stage a private test: confirm invalid/missing client
certificates are rejected, valid certs
ingest only safe synthetic data, retention works, queue recovery is bounded,
all alert expressions return the intended labels, and an actual notification
reaches the chosen on-call receiver. Rotate certs and simulate no-data and
backend outage before promotion. Do not call this production-ready without
those tests, persistent-volume/HA design, and capacity review.

References: [Collector TLS](https://github.com/open-telemetry/opentelemetry-collector/blob/main/config/configtls/README.md),
[Collector persistent queues](https://opentelemetry.io/docs/collector/resiliency/),
[Collector internal metrics](https://opentelemetry.io/docs/collector/internal-telemetry/),
[Prometheus rules](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/),
[Prometheus query functions](https://prometheus.io/docs/prometheus/latest/querying/functions/),
[Loki retention](https://grafana.com/docs/loki/latest/operations/storage/retention/),
[Loki storage](https://grafana.com/docs/loki/latest/configure/storage/),
[Tempo storage](https://grafana.com/docs/tempo/latest/configuration/hosted-storage/),
[S3 version deletion](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectVersions.html),
[GCS soft delete](https://cloud.google.com/storage/docs/soft-delete).
