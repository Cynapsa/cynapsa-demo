#!/bin/sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
manifest="$repository/AZTM_DOCS_MANIFEST.sha256"

if [ "$#" -gt 1 ]; then
  echo "usage: $0 [core-checkout]" >&2
  exit 2
fi
if [ "$#" -eq 1 ]; then
  peer=$1
else
  peer=
  for candidate in \
    "$repository/../cynapsagocore" \
    "$repository/../cynapsa/cynapsagocore"
  do
    if [ -f "$candidate/AZTM_DOCS_MANIFEST.sha256" ]; then
      peer=$(CDPATH= cd -- "$candidate" && pwd)
      break
    fi
  done
  if [ -z "$peer" ]; then
    echo "cannot locate the Go Core checkout; pass it as the first argument" >&2
    exit 2
  fi
fi

cd "$repository"
if command -v shasum >/dev/null 2>&1; then
  shasum -a 256 -c "$manifest"
else
  sha256sum -c "$manifest"
fi

actual=$(find . -maxdepth 1 -type f -name 'AZTM_*.md' -print | LC_ALL=C sort | sed 's#^./##')
expected=$(awk '{print $2}' "$manifest")
if [ "$actual" != "$expected" ]; then
  echo "root AZTM document set differs from the manifest" >&2
  exit 1
fi

obsolete='ordered_delivery|delivery\.ordering_blocked|delivery\.unknown|(^|[^[:alnum:]_])(ordered|unordered|ordering|seq|sequence|sequencer|generation|reorder|reordering|gap|head)([^[:alnum:]_]|$)'
if LC_ALL=C grep -Ein "$obsolete" $expected; then
  echo "root AZTM documents contain obsolete public terminology" >&2
  exit 1
fi

cmp "$manifest" "$peer/AZTM_DOCS_MANIFEST.sha256"
for document in $expected; do
  cmp "$repository/$document" "$peer/$document"
done

echo "AZTM root documents are valid and byte-identical to $peer"
