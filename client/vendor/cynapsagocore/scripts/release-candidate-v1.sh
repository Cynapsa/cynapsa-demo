#!/bin/sh
set -eu

usage() {
  echo "usage: $0 OUTPUT_DIRECTORY [VERSION]" >&2
  exit 2
}

fail() {
  echo "release-candidate-v1: $*" >&2
  exit 1
}

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  usage
fi

output_root=$1
version=${2:-0.1.0-rc.1}
case "$version" in
  ''|*[!A-Za-z0-9._-]*) fail "version must use only letters, digits, dot, underscore, or hyphen" ;;
esac

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd -P)
repository=$(CDPATH='' cd -- "$script_dir/.." && pwd -P)

if [ "${CYNAPSA_RELEASE_ALLOW_DIRTY_FOR_TESTS:-0}" != "1" ]; then
  [ -z "$(git -C "$repository" status --porcelain=v1 --untracked-files=all --ignore-submodules=none)" ] ||
    fail "worktree must be clean, including staged and untracked files"
fi

core_commit=$(git -C "$repository" rev-parse --verify 'HEAD^{commit}')
source_date_epoch=$(git -C "$repository" show -s --format=%ct "$core_commit")

mkdir -p -- "$output_root"
output_root=$(CDPATH='' cd -- "$output_root" && pwd -P)
case "$output_root" in
  /|"$HOME") fail "refusing broad output directory $output_root" ;;
esac

goos=$(GOTOOLCHAIN=go1.26.6 go env GOOS)
goarch=$(GOTOOLCHAIN=go1.26.6 go env GOARCH)
goversion=$(GOTOOLCHAIN=go1.26.6 go env GOVERSION)
case "$goos/$goarch" in
  darwin/arm64|linux/amd64|linux/arm64) ;;
  *) fail "unsupported release target $goos/$goarch" ;;
esac

extension=so
library_environment=LD_LIBRARY_PATH
if [ "$goos" = "darwin" ]; then
  extension=dylib
  library_environment=DYLD_LIBRARY_PATH
fi

package_name="cynapsacore-$version-$goos-$goarch"
final_directory="$output_root/$package_name"
[ ! -e "$final_directory" ] || fail "output already exists: $final_directory"

temporary_root=$(mktemp -d "${TMPDIR:-/tmp}/cynapsacore-release.XXXXXX") || fail "cannot create temporary directory"
case "$temporary_root" in
  "${TMPDIR:-/tmp}"/cynapsacore-release.*) ;;
  *) fail "unexpected temporary directory: $temporary_root" ;;
esac
cleanup() {
  case "$temporary_root" in
    "${TMPDIR:-/tmp}"/cynapsacore-release.*) rm -rf -- "$temporary_root" ;;
  esac
}
trap cleanup EXIT HUP INT TERM

source_tree="$temporary_root/source"
source_archive="$temporary_root/source.tar"
mkdir -p -- "$source_tree"
# Build exclusively from the recorded commit. This prevents untracked files,
# dirty source, or a concurrent checkout change from influencing an artifact
# whose manifest names a different commit.
git -C "$repository" archive --format=tar --output="$source_archive" "$core_commit"
tar -xf "$source_archive" -C "$source_tree"

linker_control="$source_tree/cmd/cynapsacore-shared/exports_linux.map"
external_linker="-Wl,--version-script,$linker_control"
if [ "$goos" = "darwin" ]; then
  linker_control="$source_tree/cmd/cynapsacore-shared/exports_darwin.txt"
  external_linker="-Wl,-exported_symbols_list,$linker_control"
fi

package_directory="$temporary_root/$package_name"
mkdir -p -- "$package_directory/lib" "$package_directory/include" "$package_directory/conformance"
library_relative="lib/libcynapsacore.$extension"
library="$package_directory/$library_relative"

export SOURCE_DATE_EPOCH="$source_date_epoch"
(
  cd "$source_tree"
  GOTOOLCHAIN=go1.26.6 CGO_ENABLED=1 go build \
    -buildvcs=false \
    -trimpath \
    -buildmode=c-shared \
    -ldflags="-buildid= -extldflags=$external_linker" \
    -o "$library" \
    ./cmd/cynapsacore-shared
)

generated_header="$package_directory/lib/libcynapsacore.h"
if [ -f "$generated_header" ]; then
  rm -- "$generated_header"
fi
cp -- "$source_tree/cmd/cynapsacore-shared/cynapsacore_v1.h" "$package_directory/include/cynapsacore_v1.h"
cp -R -- "$source_tree/conformance/v1" "$package_directory/conformance/v1"

compiler=$(command -v cc || true)
[ -n "$compiler" ] || fail "a C11 compiler is required for the clean-load smoke test"
smoke="$temporary_root/native-smoke"
"$compiler" -std=c11 -Wall -Wextra -Werror \
  -I "$package_directory/include" \
  "$source_tree/cmd/cynapsacore-shared/testdata/native_smoke.c" \
  -L "$package_directory/lib" -lcynapsacore -o "$smoke"
env "$library_environment=$package_directory/lib" "$smoke"

expected_exports="$temporary_root/expected-exports.txt"
actual_exports="$temporary_root/actual-exports.txt"
if [ "$goos" = "darwin" ]; then
  sed -n 's/^_\(cynapsa_v1_[A-Za-z0-9_]*\)$/\1/p' "$linker_control" | LC_ALL=C sort > "$expected_exports"
  nm -gU "$library" | awk '{name=$NF; sub(/^_/, "", name); print name}' | LC_ALL=C sort > "$actual_exports"
else
  sed -n 's/^[[:space:]]*\(cynapsa_v1_[A-Za-z0-9_]*\);[[:space:]]*$/\1/p' "$linker_control" | LC_ALL=C sort > "$expected_exports"
  nm -D --defined-only --format=posix "$library" | awk '{name=$1; sub(/@.*/, "", name); print name}' | LC_ALL=C sort > "$actual_exports"
fi
[ "$(wc -l < "$expected_exports" | tr -d ' ')" = "21" ] || fail "linker allowlist must contain exactly 21 exports"
cmp -s "$expected_exports" "$actual_exports" || fail "native library exports differ from the exact linker allowlist"

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

library_sha=$(sha256_file "$library")
header_sha=$(sha256_file "$package_directory/include/cynapsacore_v1.h")
cat > "$package_directory/manifest.json" <<EOF
{
  "schema_version": 1,
  "artifact_version": "$version",
  "abi_version": 1,
  "core_commit": "$core_commit",
  "source_date_epoch": $source_date_epoch,
  "go_version": "$goversion",
  "target": {"os": "$goos", "arch": "$goarch"},
  "library": {"path": "$library_relative", "sha256": "$library_sha"},
  "header": {"path": "include/cynapsacore_v1.h", "sha256": "$header_sha"},
  "conformance": {"path": "conformance/v1", "abi_schema": "conformance/v1/abi.json"}
}
EOF

(
  cd "$package_directory"
  : > SHA256SUMS
  find . -type f ! -name SHA256SUMS -print | LC_ALL=C sort | while IFS= read -r path; do
    hash=$(sha256_file "$path")
    printf '%s  %s\n' "$hash" "${path#./}" >> SHA256SUMS
  done
)

mv -- "$package_directory" "$final_directory"
printf '%s\n' "$final_directory"
