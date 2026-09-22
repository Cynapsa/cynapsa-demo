# AZTM Command Gate Contract

Status: current

The command gate is the typed local boundary between a language SDK and the
embedded Go Core. The SDK submits application intent; Core owns transport,
authorization policy, authenticated identity, retries, deduplication, and
terminal command completion.

## Commands

The supported application commands authenticate a session, maintain HTTP
origin mappings, send one-way messages, issue RPC requests, reply through a
Core-issued request handle, accept inbound delivery, inspect normalized status,
and close the session.

`login` has no mode argument. The native-versus-monkey distinction exists only
in SDK hooks: native callers use a `Session`, while HTTP Bridge callers keep
using their HTTP client APIs. Core receives the same canonical message intent.

An HTTP Bridge mapping is an exact normalized origin mapped to an agent and one
SDK route kind, `rpc` or `msg`. Core receives the origin-to-agent policy; the SDK
hook chooses request or send from the route kind. URL path and query are not
part of the origin key and are preserved in the HTTP request payload.

## Validation and identifiers

Every command uses a closed, versioned schema. Unknown or duplicate fields,
invalid values, oversized input, and trailing data fail before side effects.
The boundary projects allowlisted result, error, event, status, and capability
fields; it never forwards internal objects or raw dependency errors.

Message IDs, conversation IDs, event IDs, request handles, and private carrier
identifiers are opaque Core values. Applications cannot choose or inject them.
The SDK may return an unmodified Core-issued request handle only through the
reply operation. Receiver deduplication keys on authenticated mesh,
authenticated sender, and message ID.

## Timeouts and completion

Command and RPC timeout defaults are 30 seconds. A configured timeout must be
greater than zero. Core enforces the RPC deadline. The SDK waits through the
Core deadline plus a fixed 5-second safety window so the Core-owned terminal
completion wins; expiry of that safety window is an SDK safety failure, not a
new Core result.

A successful send or reply means Core accepted ownership. It is not proof that
the remote application ran. If carrier loss leaves the remote outcome
uncertain, that fact remains private and may only become private operator
telemetry in the future. An RPC reaches `rpc_timeout`; a `msg` produces no later
application signal.

## Ownership

Core owns one bounded in-memory outbox for the process lifetime. The SDK does
not cache envelopes or retry sends independently. Core first attempts Rank 1
and falls back to Rank 2 under its private policy while retaining the same
message ID. The outbox survives replacement of an XMPP session and is released
only by a terminal Core outcome, expiry, authorization loss, or shutdown.

Cancellation and shutdown are explicit commands. Closing stops admission,
resolves owned waiters once, releases bounded resources, and prevents callbacks
after the SDK boundary is quiescent.
