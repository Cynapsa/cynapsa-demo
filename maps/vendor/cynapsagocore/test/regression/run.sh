#!/bin/sh
set -eu

ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
export GOTOOLCHAIN="${GOTOOLCHAIN:-go1.26.6}"

die() {
  printf '%s\n' "regression: $*" >&2
  exit 1
}

require_tool() {
  command -v "$1" >/dev/null 2>&1 || die "required tool not found: $1"
}

non_docker_packages() {
  go list ./... | grep -Ev '(/integration|/test/e2e|/test/production/(coturn|faults|release))$'
}

run_manifest() {
  require_tool go
  cd "$ROOT"
  go test -count=1 ./test/regression
}

run_fast() {
  require_tool go
  cd "$ROOT"
  run_manifest
  packages=$(non_docker_packages)
  [ -n "$packages" ] || die 'non-Docker package set is empty'
  # Package import paths cannot contain shell whitespace. xargs prevents one
  # oversized argv without hiding a failing go test invocation.
  printf '%s\n' "$packages" | xargs go test -count=1
  go vet ./...
  go mod verify
}

run_race() {
  require_tool go
  cd "$ROOT"
  packages=$(non_docker_packages)
  [ -n "$packages" ] || die 'non-Docker package set is empty'
  printf '%s\n' "$packages" | xargs go test -race -count=1
}

run_ejabberd() {
  require_tool docker
  require_tool openssl
  cd "$ROOT"
  (
    umask 077
    go test -count=1 ./integration -run '^TestRank2Connectivity$'
  )
  (
    umask 077
    go test -count=1 ./test/e2e -run '^TestSharedGroupAuthorityAndRank2Recovery$'
  )
}

run_remote_mesh() {
  require_tool docker
  require_tool openssl
  cd "$ROOT"
  go test -v -count=1 ./test/production/coturn -run '^TestRemoteMeshSmoke$'
}

case "${1:-}" in
  manifest) run_manifest ;;
  fast) run_fast ;;
  race) run_race ;;
  ejabberd) run_ejabberd ;;
  remote-mesh) run_remote_mesh ;;
  *) die 'usage: test/regression/run.sh {manifest|fast|race|ejabberd|remote-mesh}' ;;
esac
