#!/bin/sh
# BuildKit mounts the token only for the clone step; never put it in a URL.
case "$1" in
  *Username*) printf '%s\n' x-access-token ;;
  *Password*) cat /run/secrets/github_token ;;
  *) exit 1 ;;
esac
