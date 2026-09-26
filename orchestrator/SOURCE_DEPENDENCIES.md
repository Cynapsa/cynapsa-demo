# GitHub source dependencies

This directory builds independently. Its Dockerfile fetches the current
`remove-snapshot` branch heads from:

- `https://github.com/Cynapsa/cynapsagocore.git`
- `https://github.com/Cynapsa/cynapsa-python-sdk.git`

The build uses `CYNAPSA_GITHUB_TOKEN` from this directory's ignored `.env`
as a BuildKit secret. The token needs read access to both repositories. It is
not a build argument, image layer, or runtime environment variable.

`./run.sh --build` refetches both branches. The image records the two commit
IDs at `/opt/cynapsa/source-revisions`. The SDK build verifies its vendored
header/conformance hashes against the fetched Core working tree using
`verify_core_provenance.py --commit WORKTREE`. This permits implementation-only
branch updates without requiring the SDK's historical pinned commit in a
depth-1 clone. It is a public-contract compatibility check, not equality to
that historical commit; the recorded SHAs identify the actual sources.
Incompatible headers/conformance still fail the build.
## Source versus local changes

The Dockerfile clones remote `remove-snapshot` heads, never a sibling checkout,
vendored tree, or unpushed local SDK/Core edit. Local-policy removal, the SDK
async-loop fix, and Core connectivity events are published on those branches.
Existing containers are not updated by a source push: rebuild and verify the
recorded revisions. Source review and local tests do not qualify a deployment
or prove live revocation behavior.

## Runner/build details

Docker Buildx/BuildKit and network access to GitHub, base-image registries,
Go modules, and Python packages are required for a new build. A nonempty
GitHub-token entry in the selected environment file overrides the shell token;
blank entries fall back to the shell. Use simple `KEY=value` lines without
shell quoting/export syntax: the build script reads the token literally.
The token is mounted at `/run/secrets/github_token` only for cloning via
`github-askpass.sh`, not put in repository URLs. The runner filters that key out
of the temporary mode-0600 runtime env file. Do not enable shell tracing or
publish Docker/process environment inspection.

`SOURCE_REFRESH` changes on every build-script invocation to bypass cached
clone steps. Other layers may be cached. The recorded SDK/Core SHAs identify
those two sources only; mutable base images and package versions mean this is
not a fully reproducible dependency lock. The provenance check fails if the
fetched SDK manifest hashes and Core contract files do not agree; never bypass it.

`run.sh` builds only for a missing selected image or `--build`; `--force-enroll`
is not a rebuild flag. Direct `build.sh` defaults to the role's `:local` tag,
while `run.sh` uses `:github-remove-snapshot-<Docker-server-arch>`. Set the same
role-specific image override for both, or use `run.sh --build`. Platform,
image, environment-file, and volume overrides are shell controls, not loaded
from the entity runtime env file. Existing containers retain their original
image. Stop them before replacing an installation or restarting on a new image.
