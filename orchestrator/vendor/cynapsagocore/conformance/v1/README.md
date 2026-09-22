# AZTM V1 cross-language conformance vectors

These UTF-8 JSON files are committed binding inputs and outputs for Python,
TypeScript, and future SDK generators. Object field order and byte spelling are
intentional. `commands.json`, `results.json`, `events.json`, and `payloads.json`
are regenerated and byte-compared by the Go boundary tests.

`abi.json` is the language-neutral native loading contract. It freezes ABI
version 1, the status and callback integers, the library/header names, and the
exact 21-symbol export allowlist. Release bundles copy this entire directory
unchanged and identify it from `manifest.json`.

The `name` field is test metadata, not part of the native ABI document in
`json`. Bindings must parse or produce the nested `json` value according to the
strict V1 schema. They must not add unknown fields, collapse duplicate
HTTP headers, parse opaque handles, or expose implementation metadata.

The `http_response.error` object is optional and separate from the
application headers. Its `details_json` member is a bounded JSON object encoded
as text. It is valid only on a 4xx or 5xx response.
New decoders accept the legacy response form in which `error` is absent. An
older strict V1 decoder does not recognize the extended field, so transmitting
application-error metadata requires a coordinated Core and SDK version. The
field must not be used to claim mixed-version rolling-upgrade compatibility.
The `core_create.maximum_payload_bytes` metadata freezes the shared Go,
Python, TypeScript, and native-binding configuration ceiling at 134,217,696
bytes; bindings reject zero and larger values before calling the Core.

Regenerate the derived files intentionally with:

```bash
CYNAPSA_UPDATE_VECTORS=1 go test ./internal/sdkboundary \
  -run 'Test(DecodeABICommandCoversFrozenCatalog|CanonicalPayloadVariantsHaveStrictConformanceVectors|EveryFrozenResultMapsAndEncodes|EveryFrozenEventMapsAndEncodes)'
```

Review the complete byte diff before committing a vector change.
