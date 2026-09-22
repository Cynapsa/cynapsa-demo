# Language SDK integration scaffolds

These projects show the intended language-binding boundary for the Cynapsa Go
core. They are examples for SDK authors, not release packages.

```text
Python or TypeScript public SDK
        |
        v
private language-native loader
        |
        v
versioned C shared-library ABI
        |
        v
Go-owned public contract adapter
        |
        v
Cynapsa Go core
```

The language SDK owns developer ergonomics, public models, command identifiers,
and copying ABI-owned result buffers. The Go core owns validation, runtime
behavior, normalized results, and normalized errors. Neither SDK imports Go
packages or defines a second implementation path.

Both scaffolds use polling for completions and events. The ABI also supports
callbacks, but an SDK should choose one queue-consumption model per core
instance.

Run the complete cross-language scaffold gate from the repository root:

```bash
integrations/test.sh
```

## Build a development shared library

From the repository root on Linux:

```bash
mkdir -p /tmp/cynapsa-sdk-demo
go build \
  -buildmode=c-shared \
  -ldflags="-extldflags=-Wl,--version-script,$PWD/cmd/cynapsacore-shared/exports_linux.map" \
  -o /tmp/cynapsa-sdk-demo/libcynapsacore.so \
  ./cmd/cynapsacore-shared
```

On macOS, use the `.dylib` suffix and the platform export list:

```bash
mkdir -p /tmp/cynapsa-sdk-demo
go build \
  -buildmode=c-shared \
  -ldflags="-extldflags=-Wl,-exported_symbols_list,$PWD/cmd/cynapsacore-shared/exports_darwin.txt" \
  -o /tmp/cynapsa-sdk-demo/libcynapsacore.dylib \
  ./cmd/cynapsacore-shared
```

Pass that absolute library path to either scaffold. Release SDKs should consume
the platform-specific native package produced by the repository release
process, including its manifest and V1 conformance vectors.

## Projects

- `Python` uses only the Python standard library for native calls.
- `TypeScript` uses Koffi as a private Node.js C-ABI loader.
