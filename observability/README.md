# Local observability (opt-in)

This directory is a self-contained, local-only OpenTelemetry stack for
metrics, traces, and explicitly sanitized logs. It does **not** send telemetry
to a cloud service. Starting it is optional and does not instrument existing
agents by itself. Current Core/SDK branch exports metrics; trace/log producers
are separate opt-ins and are not yet an end-to-end distributed trace.

For cross-cloud storage and alerting, see the separate
[production reference](production/README.md). It is configuration guidance,
not a deployed service; its direct per-instance edge scrape design is not
compatible unchanged with Cloud Run services or worker pools.

## Start and inspect

1. From this directory, copy `.env.example` to `.env`, fill in a unique
   Grafana password you choose, and keep `.env` private.
   The root and local `.gitignore` files exclude it. Do not commit the password.
2. Check configuration without starting containers: `docker compose config -q`.
   Avoid plain `docker compose config`: it prints the interpolated password.
3. When you explicitly want to run it: `docker compose up -d`. This can pull
   five pinned images and use CPU, memory, and disk. Open
   <http://127.0.0.1:3000> and sign in as `admin` with your chosen password.
   Open **Dashboards → Local observability** for the stack and Cynapsa
   dashboards, or **Explore** to query the preconfigured Prometheus, Tempo,
   and Loki data sources. Tempo/Loki show no Cynapsa data until safe producers
   are explicitly enabled.
4. Stop it with `docker compose down` (without `-v`). The named volumes remain.

The image versions are pinned in `compose.yaml`; they are not automatically
updated. `restart: "no"` prevents the stack from restarting when Docker starts.
The Compose network uses the explicit `10.250.243.0/24` subnet because this
host's automatic Docker address pool is full. If that subnet overlaps your VPN
or local network, change it in `compose.yaml` before starting the stack.

## Routing and attaching independent agent runs

The Collector accepts **OTLP/HTTP only**, at
`http://127.0.0.1:4318` from a host process or
`http://otel-collector:4318` from a container attached to the
`cynapsa-observability` bridge network. For a standard OTLP/HTTP SDK, set
`OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf` and the base
`OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318`; the exporter
appends the appropriate `/v1/metrics`, `/v1/traces`, or `/v1/logs` path.
For a signal-specific URL, use that full path explicitly. Host-process
equivalents use `127.0.0.1` instead. Cynapsa's current metrics CLI uses the
specific opt-in variables below, not these generic OTLP variables. There is
**no** OTLP/gRPC listener on port 4317.

Agents launched by separate `docker run` commands do not join this network
automatically. Start this Compose stack first to create the network. For a
manually constructed `docker run`, add `--network cynapsa-observability`.
For an already-running agent container, find its exact container ID with
`docker ps`, then attach it with:

```sh
docker network connect cynapsa-observability <agent-container-id>
```

The demo `run.sh` scripts accept an optional host-shell
`CYNAPSA_DEMO_DOCKER_NETWORK=cynapsa-observability` variable. Set it before
each `./run.sh` to attach that container when it starts. If the network does
not exist, the script fails before starting the agent. Their containers use
`--rm`, so set the variable on every new run. An agent may remain attached to
its original network as well; Docker supports multiple network attachments.
Containers on this bridge can also reach the unauthenticated Collector and
Prometheus endpoints, so attach only trusted local containers.

**Telemetry opt-in:** Starting this stack alone does not
enable demo telemetry. In CLI-run mode, use rebuilt demo images that include
the SDK `[otel]` dependencies and current CLI/Core sources, set the desired
SDK and/or Core variables in that entity's ignored `.env` (below), and attach
its container using `CYNAPSA_DEMO_DOCKER_NETWORK` as described above. The
exporters are independent; enable only those you want. Verify the image's
`/opt/cynapsa/source-revisions` if a rebuilt image does not emit metrics.
The Collector has independent metrics, trace, and log pipelines. It never
scrapes Docker stdout or Python's general logging module. Application and
dependency logs may contain prompts, responses, tokens, or URLs; do **not**
forward them to Loki. The stack dashboard shows scrape health;
**Cynapsa Core and SDK metrics** shows agent series only when the opt-in
exporters send data. Traces in Tempo are process-local until a separate
cross-agent propagation contract exists; do not infer an end-to-end path from
matching timestamps or diagnostic IDs.

## Cynapsa metric queries

The dashboard names come from Core `telemetry_export.go` and SDK
`telemetry_otlp.py` (with counter and latency keys in SDK `telemetry.py`).
The observed Prometheus names translate dots to underscores; a synthetic Core
OTLP gauge with unit `1` appeared as `cynapsa_core_rpc_timeout_count` on the
running default Collector exporter configuration. Prometheus
`honor_labels: true` keeps exporter `job` (`cynapsa-core` or `cynapsa-sdk`)
and process `instance` labels. Core and SDK exporters emit distinct metric
families; Core measurements come only from the Core exporter.

Examples for Grafana **Explore** (Prometheus data source):

```promql
cynapsa_core_rpc_response_count
cynapsa_core_telemetry_export_dropped_count
cynapsa_sdk_telemetry_export_dropped_count
cynapsa_sdk_native_handler_failed
cynapsa_sdk_handler_queue_high_water
cynapsa_core_rpc_timeout_count{job="cynapsa-core"}
```

Local validation observed real SDK OTLP/protobuf as
`cynapsa_sdk_native_handler_started{job="cynapsa-sdk",instance="<random UUID>"}`
and synthetic Core OTLP as
`cynapsa_core_rpc_timeout_count{job="cynapsa-core",instance="<resource service.instance.id>"}`.
The dashboard already selects these translated names without pinning an
instance ID. This validates ingestion and naming, not whether each demo image
has been rebuilt and its exporters opted in.

Core and SDK cumulative counts, fixed latency buckets, and high-water marks
are snapshot gauges, not Prometheus counters or current queue depths. The
dashboard shows current lifetime snapshots over time; session or process
resets can lower them. Do not use `rate()` or `increase()` as though these
were counter-typed series. The panels omit
`or vector(0)`: **No data** means no matching exported series, not zero
events. For an SDK latency bucket, query
`cynapsa_sdk_native_handler_latency_ms_le_100ms` (a per-bucket count, not
a Prometheus histogram bucket). The SDK telemetry export drop gauge counts
failed or overlapping export attempts; a failed export's increment can only
be seen after a later successful export.

For a rebuilt CLI-run demo container, uncomment the desired lines in its
ignored `.env` (the examples are in each entity's `.env.example`):

```dotenv
CYNAPSA_SDK_OTLP_METRICS_ENDPOINT=http://otel-collector:4318/v1/metrics
CYNAPSA_SDK_OTLP_METRICS_INSECURE_DEV=1
CYNAPSA_CORE_OTLP_METRICS_ENDPOINT=http://otel-collector:4318/v1/metrics
CYNAPSA_CORE_OTLP_METRICS_INSECURE_DEV=1
CYNAPSA_CORE_OTLP_TRACES_ENDPOINT=http://otel-collector:4318/v1/traces
CYNAPSA_CORE_OTLP_TRACES_INSECURE_DEV=1
CYNAPSA_SDK_OTLP_EVENTS_ENDPOINT=http://otel-collector:4318
CYNAPSA_SDK_OTLP_EVENTS_INSECURE_DEV=1
```

The CLI reads the SDK metric and event pairs as separate environment opt-ins;
the Core reads its own metric and trace pairs. The SDK events origin exports
fixed-name logs to Loki
and process-local spans to Tempo through the Collector. Plain HTTP is allowed
only with the respective insecure-dev flag on this trusted local network;
Core additionally restricts development HTTP destinations to private or
loopback addresses. For a separate native SDK integration, attach
`start_otlp_metrics(..., endpoint=..., allow_insecure_dev=True)` and
`start_otlp_events(..., endpoint=..., allow_insecure_dev=True)` explicitly
after connecting, and close both before the session. SDK events are never a
capture of arbitrary application logs. Their spans and Core spans do not
form a distributed trace across agents.

The dedicated mesh server has a separate, opt-in exporter at
`ejabberd-remove-snapshot/scripts/export-mesh-telemetry.py`. Run it on a
trusted operator host with access to `ejabberdctl` or the exact server
container and point it at `http://127.0.0.1:4318/v1/metrics` with
`--insecure-dev`. Its own `docs/MESH_SERVER_TELEMETRY.md` lists the command,
requirements, and available aggregates. Merely starting this Compose stack
does not collect mesh-server telemetry; it never scrapes server logs.

## Data, retention, and exposure

- Prometheus stores metrics in the Docker named volume
  `cynapsa-observability-prometheus`, retaining at most 7 days or 1 GB of
  persistent blocks, whichever limit is hit first. Its WAL/head may briefly
  exceed the size limit. There is no automatic volume deletion.
- Grafana stores users, sessions, and UI-created dashboards in
  `cynapsa-observability-grafana` indefinitely until that named volume is
  explicitly removed. The provisioned dashboard/source definitions are code.
  Grafana uses `GF_SECURITY_ADMIN_PASSWORD` only to initialize a fresh admin
  account; changing `.env` later does not rotate the password in an existing
  Grafana volume. Change it in Grafana's UI instead.
- Tempo stores local trace blocks in `cynapsa-observability-tempo` with a
  24-hour block-retention setting. Loki stores logs in
  `cynapsa-observability-loki` with 24-hour retention via its compactor.
  Retention is not an immediate hard disk cap; monitor volume usage and stop
  the stack if this laptop becomes resource-constrained.
- Telemetry is not stored in this Git tree. Do not export, commit, or publish
  the named volumes; they may contain sensitive metric labels/attributes.
- Only Grafana (`127.0.0.1:3000`) and OTLP/HTTP (`127.0.0.1:4318`) are
  published to the host. Prometheus (`9090`) and the Collector scrape endpoint
  (`9464`) are reachable only by containers attached to the bridge network.
  Loopback binding is not authentication: other local processes can submit
  telemetry to port 4318. Do not use this stack on an untrusted machine.
- Tempo and Loki expose no host ports. Their receiver/query ports are
  reachable by trusted containers on the observability bridge. This is a
  single-user development setup, not a production access-control boundary.

## Validation without launching services

The credential-free smoke harness lives in `tests/`. Its offline checks are
`python3 -m unittest discover -s tests -p 'test_smoke.py'`; they start nothing.
The **live test is opt-in** and creates a random, isolated Compose project with
the four pinned Collector/Prometheus/Tempo/Loki images, loopback-only temporary
ports, and disposable volumes. It does not use this directory's `.env`, Grafana,
agent apps, or the named production-like local stack volumes. Images must
already be present (`--pull never`). It tears its project down with `down
--volumes` even on failure or interruption. Check the printed cleanup line and
`docker ps` if the host kills the harness process abruptly.

```sh
python3 tests/smoke.py --live \
  --core-root /path/to/cynapsagocore \
  --mesh-exporter /path/to/export-mesh-telemetry.py \
  --sdk-root /path/to/cynapsa-python-sdk \
  --sdk-python /path/to/python-with-sdk-otel-extras
```

All source flags are optional and should point only to trusted local code.
The base path injects uniquely marked synthetic OTLP metrics, traces, and logs
and queries all three backends. Core runs its existing opt-in Go exporter test;
SDK uses its actual exporters on a fake in-process handle; mesh-server uses its
real encoder/sender on fixed numeric fixture data, **not** a live `ejabberdctl`
poll. Missing optional paths are reported as skips. The SDK interpreter needs
the SDK's `[otel]` extras; specifying it without them fails explicitly.
If Docker's default network pool is exhausted, supply an unused private /24
with `--subnet` after checking it does not overlap local/VPN/Docker routes.
These checks do **not** prove live agents, mesh RPC, or cross-agent traces.

## Other validation options

`docker compose config -q` checks interpolation and Compose structure after
`.env` is set. A one-shot Collector binary validation, if you choose to pull
its pinned image, is:

```sh
docker compose run --rm --no-deps --entrypoint /otelcol-contrib \
  otel-collector validate --config=/etc/otelcol-contrib/config.yaml
```

This command does not launch the long-running stack, but may download the
Collector image; it is optional. It does not prove live network delivery.
For this revision, a temporary local Collector/Tempo/Loki run accepted
synthetic OTLP traces and sanitized log records. A real SDK-local exporter
also sent a fixed `outbound.native.request` event and span; direct Loki and
Tempo queries returned both with the same trace ID. Loki's indexed labels
contained only `service_name`, not the trace ID. All test containers were then
stopped. The named Tempo and Loki volumes retain those harmless synthetic
records until retention removes them or the operator explicitly removes the
volumes. This does not prove cross-agent tracing, a real agent run, or
production suitability.

References: [Collector configuration](https://opentelemetry.io/docs/collector/configuration/),
[pinned Prometheus exporter naming](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.123.0/exporter/prometheusexporter/README.md),
[OTLP/HTTP endpoint semantics](https://opentelemetry.io/docs/specs/otel/protocol/exporter/),
[Prometheus storage retention](https://prometheus.io/docs/prometheus/latest/storage/),
[Grafana provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/),
[Loki OTLP ingestion](https://grafana.com/docs/loki/latest/send-data/otel/),
[Tempo local storage](https://grafana.com/docs/tempo/latest/set-up-for-tracing/setup-tempo/deploy/locally/linux/).
