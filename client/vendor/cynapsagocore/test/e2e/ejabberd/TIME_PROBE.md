# Authenticated server-time evidence

This fixture proves that the exact pinned test server can act as a trusted, globally formatted time source for later server-calibrated TTL design. It does not implement or freeze production TTL semantics.

## Exact server

- Image: `ghcr.io/processone/ejabberd@sha256:68482e33ff11934e73ef2881cf455ccefc1e03b31203f3ab651f4b2b1152b4a8`
- Server: ejabberd 26.4.0 on Linux arm64, Erlang/OTP 28
- Module file present in the image: `/opt/ejabberd-26.04/lib/ejabberd-26.4.0/ebin/mod_time.beam`
- Configuration: `mod_time: {}` in every Pod 6 profile
- Protocol: XEP-0202 / `urn:xmpp:time`
- IQ target: the authenticated session's bare server domain, `mesh.test`

## Probe

```sh
state=$(test/e2e/ejabberd/run.sh start normal)
test/e2e/ejabberd/run.sh probe "$state" time
test/e2e/ejabberd/run.sh stop "$state"
```

The probe performs these bounded protocol operations:

1. Opens a CA-verified STARTTLS stream without authenticating, sends a time IQ, and requires an explicit protocol rejection. A successful time result fails the probe.
2. Opens a fresh CA-verified stream and authenticates with SCRAM-SHA-256-PLUS.
3. Sends a service-discovery IQ to `mesh.test` and requires the `urn:xmpp:time` feature.
4. Sends an IQ `get` with a fixed non-secret correlation ID to `mesh.test`.
5. Requires a result from `mesh.test` to the authenticated bound identity with the exact request ID.
6. Requires exactly one `tzo` and one `utc` child, no unknown fields, a strict `Z`-normalized RFC 3339 UTC value, and a bounded `±HH:MM` offset.
7. Measures local request start/finish, midpoint, round-trip duration, and server-minus-midpoint offset.

The parser's executable tests reject unauthenticated results, mismatched IDs, mismatched sender or recipient, missing/duplicate/unknown fields, truncated XML, non-UTC values, and malformed offsets.

## Observed behavior

A representative sanitized result was:

```text
time-unauthenticated-rejected=true mode=protocol-error
time-disco-feature=urn:xmpp:time
time-authenticated=true
time-response-id-match=true
time-utc-format=strict-rfc3339-utc
time-utc=2026-08-14T01:33:10.425379Z
time-utc-fraction-digits=6
time-tzo=+00:00
time-roundtrip-ns=2545000
time-midpoint-offset-ns=-975500
time-roundtrip-ms=2
time-midpoint-offset-ms=0
```

All nine profiles returned strict fractional UTC with exactly six fractional digits and `+00:00`. In the current-main sweep, standalone round trips were 1.777–2.910 ms and production `Client.Start` uncertainties were 0.312063–0.871583 ms. Every client snapshot was ready, UTC, positive, and below the one-second protocol maximum. A separate 12-sample run against one continuously running `normal` server returned strictly increasing UTC values, 0.330–2.545 ms round trips, and −0.9945 to −0.8815 ms offsets. The complete repeated transcript was captured before teardown as `time-repeat-probe.log`.

The matching ejabberd 26.04 `mod_time` implementation takes `os:timestamp()` and supplies that value as the XEP-0202 UTC result. The pinned server therefore emits integer-microsecond values as six decimal digits; every observed timestamp changed below the second boundary, so it is not truncated to whole seconds. The protocol observation cannot distinguish truncation from rounding of a hypothetical finer-than-microsecond host clock, and the module has no explicit rounding step. The observed microsecond quantization is much smaller than the network/scheduling offset, but it does not justify zero grace.

These measurements describe this disposable local run only. Production calibration must define sample count, RTT-based uncertainty, freshness, clock-jump handling, and failure policy separately; a single midpoint estimate is not a zero-error wall clock.

After teardown, sanitized transcripts are retained as `$state/artifacts/time-probe.log` and, when the repeated-sample loop is used, `$state/artifacts/time-repeat-probe.log`. Generated passwords and private keys are removed, and the container, network, and volume are absent.
