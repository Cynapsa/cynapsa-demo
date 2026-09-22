# AZTM Deferred Work

Status: deferred; not part of the current contract

Only the following product work is deferred:

1. **Process-durable outbox.** The current bounded outbox is process memory and
   survives XMPP session replacement, not process restart. Durable persistence,
   recovery, retention, and at-rest protection remain future work.
2. **Automatic advanced large-payload handling.** Automatic streaming,
   multipart or resumable application APIs, adaptive thresholds, and a broader
   size policy are deferred. Current payloads are bounded bytes, text, JSON, or
   fully materialized HTTP bodies.

Transport uncertainty remains internal. Future work may add private operator
telemetry for it, but it must not create a new public message outcome or expose
carrier details.
