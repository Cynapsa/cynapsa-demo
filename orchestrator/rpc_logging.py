"""Application-level Cynapsa traffic logs; never inspect payloads or headers.

Kept in each entity directory so it can be copied and built independently.
These are observations, not transport delivery receipts or SDK event consumers.
"""

from __future__ import annotations

import functools
import inspect
import json
import logging
import time
import uuid
from contextlib import contextmanager


LOG = logging.getLogger("demo.cynapsa")


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO)
    # Keep useful warnings/errors without access logs for LLM/Places calls.
    for name in ("httpx", "httpcore", "urllib3", "requests"):
        logging.getLogger(name).setLevel(logging.WARNING)


def _text(value):
    return value[:256] if isinstance(value, str) else None


def _status(value):
    status = getattr(value, "status_code", None)
    return status if type(status) is int else None


def _emit(event, fields, **extra):
    LOG.info("CYNAPSA %s", json.dumps({"event": event, **fields, **extra}, ensure_ascii=True))


@contextmanager
def _traffic(role, *, incoming, peer, path, message_id=None, mode="rpc"):
    fields = {
        "entity": role, "log_id": uuid.uuid4().hex,
        "peer": _text(peer), "path": _text(path), "mode": _text(mode),
        "message_id": _text(message_id),
    }
    start = time.monotonic()
    _emit("request.received" if incoming else "request.started", fields)
    outcome = {}
    try:
        yield outcome
    except BaseException as exc:
        response = getattr(exc, "response", None)
        _emit("handler.failed" if incoming else "request.failed", fields,
              elapsed_ms=round((time.monotonic() - start) * 1000),
              error_type=type(exc).__name__, code=_text(getattr(exc, "code", None)),
              status=_status(response) or _status(exc))
        raise
    else:
        value = outcome.get("value")
        # Normalization/transmission follow this return. Invalid values can
        # still become SDK errors; do not infer canonical status or wire ACK.
        event = ("handler.completed" if mode == "msg" else "handler.returned") if incoming else "response.received"
        _emit(event, fields, elapsed_ms=round((time.monotonic() - start) * 1000),
              status=None if incoming and mode == "msg" else _status(value))


def log_handler(role):
    """Wrap before @session.on; preserve async dispatch and exceptions unchanged."""
    def decorate(handler):
        def traffic(request):
            return _traffic(role, incoming=True,
                            peer=getattr(request, "from_agent_id", None),
                            path=getattr(request, "path", None),
                            message_id=getattr(request, "message_id", None),
                            mode=getattr(request, "mode", None) or "rpc")

        if inspect.iscoroutinefunction(handler):
            @functools.wraps(handler)
            async def async_handler(request):
                with traffic(request) as outcome:
                    outcome["value"] = await handler(request)
                    return outcome["value"]
            return async_handler

        @functools.wraps(handler)
        def sync_handler(request):
            with traffic(request) as outcome:
                outcome["value"] = handler(request)
                return outcome["value"]
        return sync_handler
    return decorate


def request_sync(session, role, target, payload, **options):
    with _traffic(role, incoming=False, peer=target, path=options.get("path")) as outcome:
        outcome["value"] = session.request(target, payload, **options)
        return outcome["value"]


async def request_async(session, role, target, payload, **options):
    with _traffic(role, incoming=False, peer=target, path=options.get("path")) as outcome:
        outcome["value"] = await session.request(target, payload, **options)
        return outcome["value"]
