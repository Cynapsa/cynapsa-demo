# Cynapsa V1 native support matrix

This matrix describes the native artifact builder, not yet a public production
support promise. A target becomes release-supported only after its artifact is
built in a clean environment, passes the C11 smoke program and language-SDK
conformance suite, and is listed in the exact release manifest.

| Target | Builder support | Current evidence |
| --- | --- | --- |
| macOS 14+ / arm64 | enabled | local reproducible build, exact 21-symbol allowlist, C11 load/lifecycle smoke |
| Linux / amd64 | enabled | builder contract; clean manylinux/Python qualification pending |
| Linux / arm64 | enabled | builder contract; clean Python qualification pending |
| Windows | disabled | no V1 release artifact claim |

The first Python SDK qualification target is Linux/amd64. macOS/arm64 remains
the local developer target. No platform is published from this repository by
the release-candidate script.
