#!/usr/bin/env python3
"""Exercise SDK's actual opt-in exporters with a fixed, fake local handle."""

import argparse
from pathlib import Path
import sys
import time
from types import SimpleNamespace


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sdk-root", type=Path, required=True)
    parser.add_argument("--collector", required=True)
    args = parser.parse_args()
    sys.path.insert(0, str((args.sdk_root / "src").resolve()))
    from cynapsa.telemetry import _SdkTelemetry
    from cynapsa.telemetry_otlp import start_otlp_metrics
    from cynapsa.telemetry_events import start_otlp_events

    telemetry = _SdkTelemetry()

    class FixtureHandle:
        _inbound = SimpleNamespace(telemetry=telemetry)

        def telemetry_snapshot(self):
            return telemetry.snapshot()

    handle = FixtureHandle()
    telemetry.count("native.handler_started")
    metrics = start_otlp_metrics(
        handle, endpoint=args.collector + "/v1/metrics", allow_insecure_dev=True,
        poll_interval_seconds=1, export_interval_seconds=1,
    )
    events = start_otlp_events(handle, endpoint=args.collector, allow_insecure_dev=True)
    try:
        telemetry.record("outbound.native.request", "ok", started=time.monotonic() - 0.005)
        time.sleep(2)
    finally:
        events.close()
        metrics.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
