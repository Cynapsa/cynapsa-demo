# ADR 0007: Authenticated External Service Discovery

- Status: Accepted
- Scope: Private STUN/TURN authority and Rank 1 ICE configuration
- Supersedes: local production connectivity-profile configuration

## Context

Rank 1 requires STUN and TURN endpoints and may require short-lived relay
credentials. A local file, environment profile, or caller-supplied endpoint is
not bound to the authenticated mesh session and can outlive authorization.

## Decision

The authenticated Mesh Server is the sole production authority for STUN/TURN
configuration. Core queries XEP-0215 External Service Discovery over the exact
Rank 2 session after TLS, SASL, exact resource binding with separately verified
mesh authority, XEP-0198 setup,
server-time calibration, authority-feature discovery, and installation of a
complete current-membership snapshot.

The public SDK, ABI, commands, events, diagnostics, and errors expose no
endpoint, credential, cache, or discovery detail. Production has no file,
environment, DNS, hard-coded, or caller-supplied fallback profile.

## Validation

Core accepts only a strictly correlated result from the authenticated bare
server domain. The response is bounded before allocation and accepts only the
supported XEP-0215 schema. Unknown services, malformed values, duplicate
attributes, unexpected credentials, unsupported transports, and endpoint
policy violations fail closed.

Endpoint validation covers:

- supported STUN/TURN service types and transports;
- valid host, IP, and port forms;
- no URL user-info or unrelated URI components;
- bounded value lengths and service counts;
- exact username/password requirements for restricted TURN; and
- expiry that leaves enough time for the configured operation.

Passwords used to authenticate the mesh session are never used as relay or
payload credentials.

## Ownership and lifetime

An accepted profile belongs to the exact authenticated logical session and
current authority capability that requested it. Process-local ownership tokens
fence late callbacks after reconnect, logout, peer removal, replacement, or
shutdown. Those tokens are private lifecycle state and never appear on the
wire or establish membership.

Core stores an immutable owned copy and clears credential bytes on replacement,
expiry, removal, and shutdown where Go ownership permits it. Pion receives a
freshly owned copy; the cached profile is never aliased to a transport adapter.

Restricted TURN credentials follow the standard TURN REST shape derived by the
server from its relay secret. Core never receives that secret and does not mint
credentials locally.

## Failure behavior

Unavailable, malformed, unauthenticated, expired, or policy-invalid discovery
blocks production Rank 1 establishment. Rank 2 remains available when its
authenticated session and current membership authority are ready. No discovery
failure broadens endpoint policy or causes a local fallback configuration to be
used.

## Acceptance evidence

Tests cover:

- exact IQ ownership and server-origin validation;
- bounded parsing and malformed or duplicate response fields;
- endpoint, transport, port, credential, and expiry policy;
- late responses after reconnect, removal, logout, replacement, and shutdown;
- immutable copies and credential clearing;
- valid UDP and TCP relay configuration reaching Pion; and
- public-boundary and secret-canary scans.
