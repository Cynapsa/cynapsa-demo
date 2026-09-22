#!/bin/sh
set -eu

old='window.location.href = "/";'
new='window.location.href = "/local-login.html";'

for source in /app/src/pages/SetupPage.tsx /app/src/auth.ts; do
  count=$(grep -Foc "$old" "$source")
  if [ "$count" -ne 1 ]; then
    echo "expected exactly one local login redirect to patch in $source, found $count" >&2
    exit 1
  fi
  sed -i "s|$old|$new|" "$source"
done

exec npm run dev -- --host 0.0.0.0
