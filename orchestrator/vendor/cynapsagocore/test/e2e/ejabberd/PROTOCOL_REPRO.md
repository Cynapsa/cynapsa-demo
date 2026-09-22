# Rank-2 real-server binding reproduction

This reproduction is intentionally private test infrastructure. It contains no password, credential proof, application payload, full server-assigned resource, or production address.

## Pinned environment

- Image: `ghcr.io/processone/ejabberd@sha256:68482e33ff11934e73ef2881cf455ccefc1e03b31203f3ab651f4b2b1152b4a8`
- Image ID: `sha256:68482e33ff11934e73ef2881cf455ccefc1e03b31203f3ab651f4b2b1152b4a8`
- Server startup banner: `ejabberd 26.4.0`, Elixir 1.19.5, Erlang/OTP 28 / ERTS 16.3.1
- Platform: Linux arm64
- Go dependencies: `mellium.im/xmpp v0.23.0`, `mellium.im/sasl v0.3.2`
- Required modules: `mod_disco`, `mod_mam`, `mod_offline`, `mod_stream_mgmt`, `mod_time`, and normal-profile `mod_http_upload`
- Authentication: internal SCRAM with `auth_scram_hash: sha256`; C2S requires STARTTLS; stream management has `resend_on_timeout: if_offline` and `resume_timeout: 300`

## Commands

```sh
state=$(test/e2e/ejabberd/run.sh start normal)
test/e2e/ejabberd/run.sh probe "$state" control
test/e2e/ejabberd/run.sh probe "$state" adapter
test/e2e/ejabberd/run.sh stop "$state"
```

The generated account password is read from the state directory's mode-0600 `credentials.env`. It is not passed as a process argument or printed.

## Sanitized result

The standalone Mellium control completes the same TLS, SCRAM-SHA-256, resource-bind, and XEP-0198 sequence:

```text
control-phase=tls-sasl-bind-complete
control-bound-match=false local-match=true domain-match=true resource-match=false
control-phase=stream-management-enabled
```

The pinned server therefore supports the required authentication and stream-management protocol, but it replaces the requested resource with a server-assigned resource. The exact assigned resource is deliberately not logged.

The production adapter uses its exact-resource binding feature rather than the
standalone Mellium default, then completes XEP-0198 and its authenticated
production time query:

```text
adapter-phase=socket-open
adapter-phase=credentials-recorded
adapter-phase=resource-recorded
adapter-phase=stream-management-enabled
adapter-phase=time-query-complete
adapter-time-utc=2026-08-14T05:11:27.249381Z
```

Production `Client.Start` also completes and publishes a bounded calibration:

```text
client-phase=start-complete
client-time-ready=true
client-time-utc=2026-08-14T05:11:27.366400937Z
client-time-uncertainty-ns=445813
```

The `resume-rejected` profile retains resumable sessions for one second. Its
probe abruptly drops an enabled stream, waits 1.5 seconds, authenticates a new
stream, submits the old stream identity, and requires the server's XEP-0198
`failed` response:

```text
resume-expired-rejected=true
```

All nine profiles completed the standalone control, production adapter,
production client, and authenticated server-time paths. Each environment was
removed with its exact container, network, and volume absent afterward.

At info level the server logs contain only startup/module readiness and one accepted C2S connection per probe. There are no authentication secrets or stanza bodies. Harness readiness additionally records a successful CA-verified STARTTLS transcript (`Verification: OK`) before accounts are provisioned.
