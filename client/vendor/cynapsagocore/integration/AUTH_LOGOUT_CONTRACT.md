# Auth logout integration contract

The approved terminal semantics are:

1. An accepted `auth.logout` emits exactly one successful command completion.
2. Only after that completion is observable does the Core begin graceful shutdown.
3. The same Core reaches `closed` and rejects reconnect or re-authentication.
4. Reconnection requires construction and startup of a new Core.

The black-box test must assert this ordering rather than merely observing both outcomes. Execution is presently downstream of the real-server exact-resource binding reproduction in `test/e2e/ejabberd/PROTOCOL_REPRO.md`; it must not weaken or bypass authenticated setup.
