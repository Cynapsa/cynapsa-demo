# Disposable ejabberd E2E Environment

This folder contains the pinned, isolated ejabberd environment used by Pod 6. It generates a private CA and synthetic accounts per invocation, binds only random loopback ports, uses a unique internal Docker network, drops every capability, limits CPU/memory/PIDs, and puts database, log, and upload state in size-bounded tmpfs mounts. The container image is pinned by digest to ejabberd 26.04.

Supported profiles are `normal`, `offline`, `resume-rejected`, `upload-disabled`, `upload-unavailable`, `upload-failing`, `download-failing`, `quota-rejected`, `chunk-rejected`, `shared-group`, and the Coturn-owned `shared-group-extdisco`. `upload-unavailable` and `upload-failing` advertise an unreachable test-only upload endpoint. `download-failing` starts with working upload storage so a test can delete the object before GET. `chunk-rejected` lowers the server stanza bound to 1 KiB.

The `shared-group` profile independently compiles the candidate `mod_cynapsa_mesh` source inside the pinned image, executes its embedded tests, and refuses to start unless the deterministic beam SHA-256 is `149bd73a6e3752a8e81f0234386f92f1011fe4902337f5c52bab22c90e5fbfcb`. It adds a third registered nonmember account and exposes bounded, token-validated admin controls:

The dedicated `resource_mailbox_test.sh` exercises stable-ID admission,
exact-resource isolation, XEP-0198 acknowledgement retirement, resume-expiry
replay, restart durability, retained-stream continuity while one peer is
removed and another remains, and sender/recipient membership purge.

Current membership is installed once for each fresh authenticated logical
session and has no local lease or periodic refresh. A transport loss suspends
all peer publication immediately. Successful XEP-0198 resume restores the
exact suspended authority only after retained control replay and the strict
`resume-authority: ready` barrier; failed resume performs discovery and a
complete snapshot on the fresh session. Ordinary message and protocol
deadlines remain active.

Harness inputs are mode-deterministic even when the caller uses `umask 077`.
The secret-bearing XEP-0215 configuration is a mode-0600 private input; other
rendered profiles remain mode 0444. Every configuration is staged into the
server volume as uid 9000 mode 0400. Retained artifacts redact the TURN secret.
The public CA, staged module source, and compiled beam are explicit
read-only public inputs inside the mode-0700 disposable state.
The module is compiled only from the checksum-verified staged copy; the
repository source is never chmodded or mounted into a container. The combined
server certificate/private-key file remains mode 0600 on the host and is
installed into the disposable volume as uid 9000 mode 0400 before ejabberd
starts. Generated credentials and private-key intermediates remain mode 0600
and are removed with the volume during teardown.

```sh
test/e2e/ejabberd/run.sh admin "$state" ready
test/e2e/ejabberd/run.sh admin "$state" create Mesh-A
test/e2e/ejabberd/run.sh admin "$state" add agent-a Mesh-A
test/e2e/ejabberd/run.sh admin "$state" remove agent-a Mesh-A
test/e2e/ejabberd/run.sh admin "$state" remove-many agent-a,agent-b Mesh-A
test/e2e/ejabberd/run.sh admin "$state" snapshot Mesh-A
test/e2e/ejabberd/run.sh restart "$state"
test/e2e/ejabberd/run.sh external-service-mode "$state" relay-tcp
```

`restart` restarts the bounded ejabberd service inside the existing disposable container so the test exercises durable single-node Mnesia state. Recreating the container intentionally destroys its tmpfs database and is not a persistence test. The shared-group matrix proves pre-sync and recipient-unsynchronized routes are blocked, snapshot pages come from one immutable exact-session vector, a clean XEP-0198 resume preserves readiness only after the post-replay authority result, and a mutation during an outage invalidates readiness before its queued wakeup without destroying the retained stream. Batch removal still closes removed exact resources and purges mesh-scoped retention; a resumed invalidated session receives `not-ready`, performs a clean bind, and completes a fresh snapshot. Teardown removes the exact container, network, volume, credentials, and private keys. Sanitized output is written to the configured E2E artifact directory and the retained state `artifacts` folder.

`external-service-mode` is available only to the secret-bearing
`shared-group-extdisco` fixture and accepts `all`, `stun`, `relay-udp`, or
`relay-tcp`. It stages and reloads a bounded server configuration;
agents still learn the result only through authenticated XEP-0215. The ordinary
`shared-group` profile returns a legal authenticated empty service result, so
its isolated same-network functional test uses host candidates without a local
configuration fallback.

The safest interface wraps the test command and tears down unconditionally:

```sh
test/e2e/ejabberd/run.sh run normal -- go test ./integration -run TestRank2
```

For a separately managed process, `start` prints one private state-directory path. Source its mode-0600 `credentials.env`, and always call `stop` with that same directory:

```sh
state=$(test/e2e/ejabberd/run.sh start normal)
set -a; . "$state/credentials.env"; set +a
trap 'test/e2e/ejabberd/run.sh stop "$state"' EXIT
```

The environment contract provides `CYNAPSA_EJABBERD_ENDPOINT`, `CYNAPSA_EJABBERD_HTTPS`, `CYNAPSA_EJABBERD_CA`, two bare synthetic agent identities and their generated passwords, plus the profile and state path. No password is written to stdout, placed in a process argument, or retained after teardown. Sanitized container logs, state, rendered non-secret configuration, and profile are retained under `$state/artifacts`. When `CYNAPSA_E2E_ARTIFACT_DIR` is set, the same sanitized files are also copied into a private per-run subdirectory there so CI can retain them after the disposable environment is gone.

The `shared-group-extdisco` profile accepts its generated TURN REST secret only
through `CYNAPSA_EJABBERD_TURN_REST_SECRET_FILE`. The value must name an
absolute, non-symlink, mode-0600 file containing exactly one 64-character
lowercase hexadecimal line. Rendering reads that file as input data; the
secret is never interpolated into a host process argument. The caller owns and
deletes the private input file.

`run.sh probe STATE_DIRECTORY adapter` executes the production rank-2 TLS, SASL, exact-resource binding, XEP-0198 negotiation, and private `QueryServerTime` call against a running state. `application` sends one canonical Rank2 frame between provisioned members and verifies the exact received full JIDs, mesh, namespace, and bytes; set `CYNAPSA_EJABBERD_MESH` to the provisioned group. `client` runs production `Client.Start` and requires a ready UTC calibration with uncertainty at most one second. `external` additionally requires a complete exact-session current-membership sync and strict XEP-0215 discovery and writes one restricted TURN credential only to a caller-selected mode-0600 file for the Coturn allocation probe. `control` runs the standalone Mellium equivalent. `resume` proves an expired XEP-0198 resume is rejected in the `resume-rejected` profile. `time` proves unauthenticated rejection and then performs authenticated service discovery plus a strict correlated server-time request. All modes read the generated password only from the mode-0600 environment file, never from command arguments. See [PROTOCOL_REPRO.md](PROTOCOL_REPRO.md) and [TIME_PROBE.md](TIME_PROBE.md) for sanitized real-server results.

## Public messaging and live-link coverage

The external public-only two-Core runner now covers the complete V1 payload path. Its first same-mesh message carries a 96 KiB canonical payload before a live peer path exists, exercising the private upload attempt and server-visible plaintext XMPP-chunk fallback over TLS; the receiver verifies every byte and performs mandatory delivery acceptance. The same send starts a bounded XEP-0166 handshake; both agents then report the peer as available, and the following request/reply completes with the healthy DTLS-protected, infrastructure-confidential RTCDataChannel eligible as the preferred carrier. The handshake binds the two exact full JIDs, mesh ID, SID, and authenticated DTLS fingerprints. Transport-neutral public results are preserved. The shared-group matrix separately proves that a current-membership change kicks only removed exact resources, purges their mesh-scoped retained state, rejects a later bind while they remain absent, and leaves present members authorized regardless of remove/re-add history. Every fresh bind requires a current-membership snapshot; a successful XEP-0198 resume preserves readiness only when no mutation invalidated that mesh, while an invalidated resumed session processes queued control and resynchronizes before peer traffic.

This disposable functional profile has direct host candidates because both agents run on one isolated Docker network. Production qualification now supplies private server configuration through the separate [`test/production/coturn`](../../production/coturn/README.md) environment: two independent agent bridges, a STUN-only process, and an authenticated forced-relay process. This functional profile still makes no NAT-traversal claim of its own.

Do not add production credentials, privileged mode, host networking, Docker socket mounts, or broad host filesystem mounts.
