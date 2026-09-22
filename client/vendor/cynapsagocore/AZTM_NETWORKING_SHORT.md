# AZTM Networking Contract — Short Form

Status: current

- Core owns transport, policy, authenticated identity, IDs, retries, RPC
  expiry, message-ID deduplication, and the bounded process-memory outbox.
- Core attempts Rank 1, then Rank 2 fallback, while retaining one message ID.
- The outbox survives XMPP session replacement but not process restart.
- Current membership is a complete server snapshot and has no membership
  lease.
- The exact-resource mailbox is independent of any XEP-0198 session identifier
  and is purged when its sender or recipient resource is removed.
- A fresh Rank 2 session requires TLS, SASL, exact-resource bind, XEP-0198,
  authority-feature discovery, server time, and a complete snapshot.
- A successful resume uses fresh TLS and SASL, skips bind, feature discovery,
  and snapshot download, waits for exact `resume-authority: ready`, completes
  retained transport replay, and then recalibrates server time before live
  publication.
- A failed resume performs the complete fresh-session setup.
- A dedicated bounded control lane commits each exact correlated IQ before its
  processed acknowledgement; failure closes the session.
- Transport uncertainty remains private. RPC ends with `rpc_timeout`; `msg`
  has no later application signal.
