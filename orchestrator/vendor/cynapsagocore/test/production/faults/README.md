# Disposable TCP and HTTP Fault Environment

This package qualifies the production XMPP and XEP-0363 network clients across
real sockets and a pinned
[`Toxiproxy 2.12.0`](https://github.com/Shopify/toxiproxy/releases/tag/v2.12.0)
container. It reuses the disposable shared-group ejabberd authority and starts a
separate bounded object endpoint for V1 HTTPS-protected, server-visible
plaintext object transfer.

The immutable proxy image is:

```text
ghcr.io/shopify/toxiproxy@sha256:9378ed52a28bc50edc1350f936f518f31fa95f0d15917d6eb40b8e376d1a214e
```

`profiles-v1.json` is the authoritative versioned profile set. It currently
proves:

- clean production XMPP TLS/SASL/resource-bind/XEP-0198 establishment;
- bidirectional XMPP latency with bounded success;
- a half-open XMPP stream with exact caller-deadline termination;
- TCP peer reset with bounded failure;
- clean XEP-0363-client upload and download through a real HTTP proxy;
- upload bandwidth restriction and real TCP backpressure;
- HTTP half-open normalization to `xep0363.ErrTimeout`;
- HTTP reset normalization to `xep0363.ErrTransfer`; and
- abrupt proxy-process restart, old-stream rejection, and clean fresh-session
  recovery.

The environment creates one unique Docker bridge through the existing
ejabberd harness. Toxiproxy and the object endpoint run in separate read-only,
resource-bounded containers with all Linux capabilities dropped. Only the
proxy API, XMPP proxy, and HTTP proxy receive random loopback-only host ports.
It does not use host networking, privileged mode, Docker socket mounts, or host
network impairment.

Run the package with:

```sh
GOTOOLCHAIN=go1.26.6 go test -v -count=1 ./test/production/faults
```

The test holds the repository-wide production Docker lock, captures sanitized
container logs and the exact profile file, removes credentials and the helper
binary, and verifies that all containers, the bridge, and the ejabberd volume
were removed.

This is the first controlled-fault slice, not completion of the entire fault
matrix. Packet loss/duplication/reordering and UDP jitter belong in the
dedicated `CAP_NET_ADMIN`-scoped packet-impairment container. DNS failure,
certificate failure, and service-specific abrupt restart profiles remain
separate follow-up work.
