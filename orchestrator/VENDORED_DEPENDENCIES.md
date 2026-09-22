# Vendored dependencies

This directory is self-contained and builds against these pinned sources:

- Cynapsa Python SDK: `de1ee6076d44d6e64962de5fa192bc17b8e272b4`
- Cynapsa Go Core: `e99ff96c1c5b569247195ff29c5b926a9dbbfb48`

The complete source trees are under `vendor/`. No external SDK or Go Core
checkout is used by `build.sh` or the Dockerfile.
