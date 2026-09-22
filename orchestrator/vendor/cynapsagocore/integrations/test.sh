#!/bin/sh
set -eu

script_directory=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repository=$(CDPATH= cd -- "$script_directory/.." && pwd -P)
native_directory=$(mktemp -d "${TMPDIR:-/tmp}/cynapsa-sdk-tests.XXXXXX")
npm_cache="$native_directory/npm-cache"

cleanup() {
  case "$native_directory" in
    "${TMPDIR:-/tmp}"/cynapsa-sdk-tests.*) find "$native_directory" -depth -delete ;;
  esac
}
trap cleanup EXIT HUP INT TERM

goos=$(go env GOOS)
case "$goos" in
  linux)
    library="$native_directory/libcynapsacore.so"
    linker="-Wl,--version-script,$repository/cmd/cynapsacore-shared/exports_linux.map"
    ;;
  darwin)
    library="$native_directory/libcynapsacore.dylib"
    linker="-Wl,-exported_symbols_list,$repository/cmd/cynapsacore-shared/exports_darwin.txt"
    ;;
  *)
    echo "unsupported development platform: $goos" >&2
    exit 1
    ;;
esac

(
  cd "$repository"
  go build -buildmode=c-shared -ldflags="-extldflags=$linker" -o "$library" ./cmd/cynapsacore-shared
)

(
  cd "$script_directory/Python"
  CYNAPSA_CORE_LIBRARY="$library" PYTHONPATH=src python3 -m unittest discover -s tests -v
)

(
  cd "$script_directory/TypeScript"
  NPM_CONFIG_CACHE="$npm_cache" npm ci
  CYNAPSA_CORE_LIBRARY="$library" NPM_CONFIG_CACHE="$npm_cache" npm test
)

(
  cd "$repository"
  go test ./cmd/cynapsacore-shared ./internal/sdkboundary
)
