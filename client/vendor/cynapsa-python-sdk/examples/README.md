# Examples

These examples use `CYNAPSA_MESH_ID` and `CYNAPSA_PROFILE_ID`. Set
`CYNAPSA_ENROLLMENT_TOKEN` on the first run; omit it on later runs to reopen
the Core-owned installed profile. Do not put the enrollment token in source
files. Set
`CYNAPSA_CORE_LIBRARY` when the native library is not discoverable through the
package or operating system.

- `sync_native.py`: synchronous send/request and inbound routes.
- `async_native.py`: asynchronous Session lifecycle.
- `native_http_payloads.py`: native Session use of universal request/response
  models without optional HTTP clients.
- `http_bridge_requests.py`: selective Requests interception.
- `http_bridge_httpx.py`: selective HTTPX interception.
- `http_bridge_urllib_request.py`: selective stdlib `urlopen` interception.
- `http_bridge_aiohttp.py`: selective asynchronous aiohttp interception.
- `explicit_asgi.py`: an explicitly supplied ASGI application.

These examples use `CynapsaRequest`, handler-returned `CynapsaResponse`,
per-origin `{recipient, mode}` bridge mappings, and `hook_asgi`/`asgi_app`.

The CLI is also implemented. An ordinary HTTP application that does not call
Cynapsa itself can be bootstrapped without source changes, for example:

```console
cynapsa run [Cynapsa options] -- python regular_http_agent.py
cynapsa run [Cynapsa options] -- uvicorn package:app
```

The examples above already create their own Cynapsa sessions or bridges, so
`cynapsa run` is an alternative bootstrap model rather than an additional
wrapper for those same processes. Automatic expanded-payload handling remains
deferred.
