"""Console observation of the SDK event stream, independent of handlers."""
from __future__ import annotations

import asyncio
import queue
import threading
from contextlib import asynccontextmanager, contextmanager


def report_event(entity, event):
    payload = event.payload
    if event.event_name in {"connectivity.state_changed", "session.state_changed"}:
        label = "mesh connectivity" if event.event_name.startswith("connectivity.") else "SDK session"
        print(
            f"[{entity}] ALERT {label}: {payload.previous} -> {payload.current}",
            flush=True,
        )
    elif event.event_name == "core.error":
        error = payload.error
        # Report the actual canonical SDK error; do not infer a revoke reason.
        print(
            f"[{entity}] ALERT core error: code={error.code} "
            f"stage={error.stage} retryable={error.retryable} "
            f"source={error.local_or_remote} message={error.message}",
            flush=True,
        )


def report_failure(entity, error):
    print(
        f"[{entity}] ALERT connection event monitor stopped: "
        f"{type(error).__name__}: {error}",
        flush=True,
    )


@contextmanager
def watch_connection(session, entity):
    """Own the session's next_event consumer, not its request-handler stream."""
    stopped = threading.Event()

    def consume():
        while not stopped.is_set():
            try:
                event = session.next_event(timeout=0.25)
            except queue.Empty:
                continue
            except Exception as error:
                if not stopped.is_set():
                    report_failure(entity, error)
                return
            report_event(entity, event)

    worker = threading.Thread(target=consume, name=f"{entity}-connection-alerts", daemon=True)
    worker.start()
    try:
        yield
    finally:
        stopped.set()
        # Public next_event has a bounded timeout. Stop before closing the SDK.
        worker.join()


@asynccontextmanager
async def watch_connection_async(session, entity):
    async def consume():
        while True:
            try:
                event = await session.next_event(timeout=0.25)
            except queue.Empty:
                continue
            except Exception as error:
                report_failure(entity, error)
                return
            report_event(entity, event)

    worker = asyncio.create_task(consume(), name=f"{entity}-connection-alerts")
    try:
        yield
    finally:
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass

