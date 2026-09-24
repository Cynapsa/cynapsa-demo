# AZTM Networking Contract — Short Form

Status: current

- Core owns transport, policy, authenticated identity, IDs, retries, RPC
  expiry, message-ID deduplication, and the bounded process-memory outbox.
- Core attempts Rank 1, then Rank 2 fallback, while retaining one message ID.
- The outbox survives XMPP session replacement but not process restart.
- First contact uses a server-authorized Cynapsa peer handshake; no complete
  mesh-member snapshot is downloaded or used for local admission.
- The dedicated server currently has no per-installation offline mailbox.
  XEP-0198 may replay during the 30-second resume window; an unacknowledged
  stanza may be lost after final session expiry.
- A fresh Rank 2 session requires TLS, SASL, exact-resource bind, XEP-0198,
  and server-time calibration; peers start closed.
- A successful resume keeps the server session but Core reauthorizes peers
  before releasing their work. A failed resume makes a clean bind. The server
  revokes an expired installation after a 30-second resume window.
- A dedicated bounded control lane action-ACKs revokes after peer cleanup.
  Both sides probe idle connections every 10 seconds with a 10-second
  response deadline.
- Transport uncertainty remains private. RPC ends with `rpc_timeout`; `msg`
  has no later application signal.
