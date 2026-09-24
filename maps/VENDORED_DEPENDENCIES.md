# Vendored dependencies

This directory is self-contained and builds against these feature-branch
worktree snapshots (including their uncommitted changes, not just HEAD):

- Cynapsa Python SDK `remove-snapshot`, HEAD `d29da9525536a3b295f6d1e55afe7c662f27f79d`.
  Vendored file-manifest SHA-256: `d37e54b74bd8796fb85cc9009d1481d330ffdce9836ac2a36043d17aa26334a9`.
- Cynapsa Go Core `remove-snapshot`, HEAD `a1c64542d17b873b717cc453c98760d01791b6ae`.
  Vendored file-manifest SHA-256: `ffdef2a69f56aa44a72dab482dc8ba1121b8c570089d61ca7376e4e445a4460b`.

The complete source trees are under `vendor/`. No external SDK or Go Core
checkout is used by `build.sh` or the Dockerfile.

To reproduce either manifest hash, run from that vendor root:
`find . -type f -print0 | sort -z | xargs -0 shasum -a 256 | shasum -a 256`.
