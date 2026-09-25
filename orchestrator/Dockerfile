# syntax=docker/dockerfile:1.7

ARG GO_IMAGE=golang:1.26-bookworm
ARG PYTHON_IMAGE=python:3.12-slim-bookworm
FROM ${GO_IMAGE} AS core-builder
WORKDIR /src/core
COPY vendor/cynapsagocore/ ./
RUN GOTOOLCHAIN=auto CGO_ENABLED=1 go build -trimpath -buildmode=c-shared \
    -o /out/libcynapsacore.so ./cmd/cynapsacore-shared

FROM ${PYTHON_IMAGE} AS sdk-builder
WORKDIR /src/sdk
RUN python -m pip install --no-cache-dir build
COPY vendor/cynapsa-python-sdk/ ./
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
COPY app.py bootstrap.py llm.py runtime.py entrypoint.sh ./
RUN chmod 0555 /app/entrypoint.sh /opt/cynapsa/libcynapsacore.so

USER 65532:65532
VOLUME ["/var/lib/cynapsa"]
ENTRYPOINT ["/app/entrypoint.sh"]
