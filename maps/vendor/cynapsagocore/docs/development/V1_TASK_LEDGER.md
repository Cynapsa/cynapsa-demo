# Cynapsa Go Core Task Ledger

Status: current contract and evidence index

This ledger records the active implementation model and the evidence required
before release. Git history is the source for earlier implementation work; this
document does not preserve retired protocol designs.

## Current architecture

- The authenticated exact server session and its separately verified mesh own one complete current
  membership snapshot.
- A dedicated control lane processes server authority notices independently of
  peer traffic.
- One lazy bounded actor lane owns each active peer's messages, RPCs, payload
  work, and transport lifecycle.
- Stable Core-created message IDs converge retries and carrier fallback.
- Receiver duplicate suppression uses authenticated mesh, authenticated sender,
  and message ID without comparing content.
- RPC uses unpredictable correlation IDs, exact reply identity, bounded tables,
  and one terminal winner.
- One bounded carrier-neutral outbox survives XMPP session replacement within
  the Core process.
- Rank 2 exact-resource mailbox storage survives destination session changes
  within the configured server retention period.
- Peer removal fences communication and purges exact peer/resource state.
- The exact current canonical payload ceiling is 134,217,696 bytes.

## Contract checkpoints

| Area | Normative documents | Required evidence |
| --- | --- | --- |
| SDK boundary | `AZTM_SDK_BOUNDARY.md`, `AZTM_SDK.md` | public schema, ABI, binding, and leakage tests |
| Command runtime | `AZTM_COMMAND_GATE.md`, ADR 0001, ADR 0004 | lifecycle, handle, queue, callback, and shutdown tests |
| Envelope and identity | `AZTM_ENVELOPE_PAYLOAD_PATCHING.md`, ADR 0002, ADR 0003 | codec, identity, binding, dedupe, RPC, and replay tests |
| Membership authority | ADR 0006, ADR 0008 | discovery, snapshot, control-lane, pause, removal, and recovery tests |
| Connectivity | `AZTM_NETWORKING.md`, ADR 0007 | Rank 1, Rank 2, resume, mailbox, relay, fallback, and recovery tests |
| Payloads | ADR 0005, V1 security model | canonicalization, transfer, object security, reassembly, and cleanup tests |
| Mesh Server | `server/ejabberd/README.md` | exact-resource bind, authority, mailbox, removal, and resume tests |
| Production | Pod 6, Pod 7, release docs | black-box E2E, fault, compatibility, security, performance, and soak evidence |

## Implemented contract highlights

### Session authority

Fresh authenticated sessions fail closed unless ejabberd advertises the exact
Cynapsa authority feature and serves a complete snapshot. Application work is
held until the snapshot is installed. The authority capability has no local
lease; transport loss pauses all peer publication and carrier success.

Successful XEP-0198 resume uses the same logical session only after exact
server-owned authority evidence is processed. Rejected, expired, malformed, or
stale resume evidence causes a clean session and fresh snapshot.

### Sender and destination offline behavior

When the sender loses the server connection, pending logical messages remain in
the Core outbox. After authority recovery, the same entries continue through
the available carrier. RPC deadlines continue while offline.

When the destination exact resource is offline, ejabberd stores eligible
application messages in that resource's mailbox. Mailbox ownership is based on
user, host, and resource rather than an XMPP session identifier. Membership
removal purges the exact resource's stored traffic.

### Delivery outcomes

One-way message hooks return local admission success through the SDK contract.
Core may retain private carrier uncertainty for future diagnostics, but no
later application error is emitted for that message. RPC returns its response,
a normalized terminal error, or `rpc_timeout` at the configured deadline.

### Handler replies

Handlers do not claim replies explicitly. For RPC, every normal return is
normalized once: `None` is a successful JSON-null response, supported shorthand
values become canonical success responses, and an explicit canonical response
is preserved. An intentional RPC exception becomes a safe canonical application
error; an unexpected exception becomes a sanitized `handler_error` response.
For one-way calls, the handler return is discarded and exceptions remain local.

## Required verification

### Core packages

```text
go test ./...
go test -race ./internal/conversation ./internal/rpc ./internal/mesh
go test -race ./internal/transport/rank2xmpp
go test -race ./internal/transport/rank1webrtc ./internal/peer ./internal/handshake
go test -race ./internal/payload ./internal/xep0363
```

### Server contracts

```text
go test ./server/ejabberd
server/ejabberd/build.sh
test/e2e/ejabberd/resource_mailbox_test.sh
go test ./test/e2e
```

### Documentation and boundary checks

```text
go test ./test/doccontract
scripts/verify-aztm-docs.sh <python-sdk-path>
git diff --check
```

### Deep E2E

The multi-container environment must prove native and monkey-patched sync and
async clients against native and monkey-patched servers, relay-only and
XMPP-only network profiles, short successful-resume outages, long clean-session
recovery, exact workload totals, duplicate suppression, and recovery without
agent awareness of injected faults.

## Release blockers

- Any public transport, credential, private identifier, dependency text, or
  storage-reference leak.
- Missing or non-exact authority ownership.
- Peer work published while authority is paused.
- Duplicate application invocation for one authenticated scoped message ID.
- Outbox or mailbox loss across the session-replacement cases they promise to
  survive.
- Unbounded queue, table, worker, retry, reconnect, frame, or byte state.
- Failure to release owned work after timeout, cancellation, removal, logout,
  or shutdown.
- Required normal, race, real-service, or black-box evidence not passing on the
  exact candidate.
