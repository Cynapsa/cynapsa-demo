# syntax=docker/dockerfile:1.7

ARG GO_IMAGE=golang:1.26-bookworm
ARG PYTHON_IMAGE=python:3.12-slim-bookworm
FROM alpine:3.23 AS sources
RUN apk add --no-cache ca-certificates git
COPY github-askpass.sh /usr/local/bin/cynapsa-git-askpass
RUN chmod 0555 /usr/local/bin/cynapsa-git-askpass
ARG SOURCE_REFRESH
RUN --mount=type=secret,id=github_token,required=true \
    set -eu; \
    : "$SOURCE_REFRESH"; \
    export GIT_ASKPASS=/usr/local/bin/cynapsa-git-askpass GIT_TERMINAL_PROMPT=0; \
    git clone --quiet --depth 1 --single-branch --branch remove-snapshot \
      https://github.com/Cynapsa/cynapsagocore.git /src/core; \
    git clone --quiet --depth 1 --single-branch --branch remove-snapshot \
      https://github.com/Cynapsa/cynapsa-python-sdk.git /src/sdk; \
    printf 'go_core=%s\nsdk=%s\n' \
      "$(git -C /src/core rev-parse HEAD)" "$(git -C /src/sdk rev-parse HEAD)" \
      >/src/source-revisions

FROM ${GO_IMAGE} AS core-builder
WORKDIR /src/core
COPY --from=sources /src/core/ ./
RUN GOTOOLCHAIN=auto CGO_ENABLED=1 go build -trimpath -buildmode=c-shared \
    -o /out/libcynapsacore.so ./cmd/cynapsacore-shared

FROM ${PYTHON_IMAGE} AS sdk-builder
WORKDIR /src/sdk
RUN apt-get update && apt-get install --no-install-recommends -y git \
    && rm -rf /var/lib/apt/lists/* \
    && python -m pip install --no-cache-dir build
COPY --from=sources /src/sdk/ ./
COPY --from=sources /src/core/ /src/core/
RUN python scripts/verify_core_provenance.py --core-checkout /src/core
RUN python -m build --wheel --outdir /out

FROM ${PYTHON_IMAGE} AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    CYNAPSA_CORE_LIBRARY=/opt/cynapsa/libcynapsacore.so \
    CYNAPSA_STATE_DIRECTORY=/var/lib/cynapsa \
    DEMO_PRIVATE_DIRECTORY=/tmp/cynapsa-private

RUN groupadd --gid 65532 cynapsa \
    && useradd --uid 65532 --gid 65532 --no-create-home --shell /usr/sbin/nologin cynapsa \
    && mkdir -p /app /opt/cynapsa /var/lib/cynapsa \
    && chown -R 65532:65532 /app /var/lib/cynapsa

WORKDIR /app
COPY requirements.txt ./
COPY --from=sdk-builder /out/*.whl /tmp/wheels/
RUN python -m pip install --no-cache-dir -r requirements.txt /tmp/wheels/*.whl \
    && rm -rf /tmp/wheels

COPY --from=core-builder /out/libcynapsacore.so /opt/cynapsa/libcynapsacore.so
COPY --from=sources /src/source-revisions /opt/cynapsa/source-revisions
COPY app.py bootstrap.py runtime.py entrypoint.sh ./
RUN chmod 0555 /app/entrypoint.sh /opt/cynapsa/libcynapsacore.so

USER 65532:65532
VOLUME ["/var/lib/cynapsa"]
ENTRYPOINT ["/app/entrypoint.sh"]
