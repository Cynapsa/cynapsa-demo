# Cynapsa Go core V1 release-candidate checklist

The release builder creates a local, unpublished SDK input bundle:

```sh
./scripts/release-candidate-v1.sh ./dist 0.1.0-rc.1
```

The bundle contains the native library, authoritative C header, V1 ABI
contract, all cross-language conformance vectors, deterministic manifest, and
`SHA256SUMS`. The builder requires a completely clean Git worktree, records
`HEAD` before compilation, and builds only a `git archive` of that immutable
commit. The build uses Go 1.26.6, `-trimpath`, an empty Go build ID, and the
platform export allowlist. It enumerates the built library's dynamic exports
and requires the exact 21-symbol allowlist, then compiles and executes the C11
lifecycle smoke program before publishing the bundle to the requested local
directory.

Before an SDK baseline is accepted:

- all Go-core source is committed; staged, tracked-dirty, and untracked paths
  are rejected;
- two builds of the same target are byte-identical;
- the ABI contract, header, and platform symbol lists contain exactly the same
  21 exports;
- `go test -race ./...`, `go vet ./...`, module verification, vulnerability
  scanning, and the real service matrix pass on the candidate;
- the SDK records the exact `core_commit`, ABI version, target, and checksums
  from `manifest.json`;
- every regular bundle file other than `SHA256SUMS` has exactly one sorted,
  path-safe, verified checksum entry;
- a clean Python environment loads only the manifested library and runs the
  shared conformance vectors;
- external package publication or signing is performed only after separate
  user authorization.
