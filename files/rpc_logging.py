"""Simple application-level Cynapsa request and response logs.

Kept in each entity directory so it can be copied and built independently.
These are observations, not transport delivery receipts or SDK event consumers.
"""

from __future__ import annotations

import functools
import inspect
import json
import logging
import os
from contextlib import contextmanager


LOG = logging.getLogger("demo.cynapsa")


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO)
    # Keep useful warnings/errors without access logs for LLM/Places calls.
    for name in ("httpx", "httpcore", "urllib3", "requests"):
        logging.getLogger(name).setLevel(logging.WARNING)


def _peer(identity):
    for name in ("client", "orchestrator", "maps", "files"):
        if identity and identity == os.environ.get(f"DEMO_{name.upper()}_AGENT_ID"):
            return f"{name} ({identity})"
    return identity or "unknown"


def _content(value):
    body = getattr(value, "body", value)
    if isinstance(body, bytes):
        decoded = body.decode("utf-8", errors="replace")
        try:
            body = json.loads(decoded)
        except ValueError:
            body = decoded
    return json.dumps(body, ensure_ascii=False, default=str)


@contextmanager
def _traffic(role, *, incoming, peer, path, content, mode="rpc"):
    source = role.removeprefix("demo-")
    target = _peer(peer)
    direction = "from" if incoming else "to"
    LOG.info("[%s] request %s %s %s %s: %s",
             source, "received" if incoming else "sent",
             direction, target, path or "/", _content(content))
    outcome = {}
    try:
        yield outcome
    except BaseException as exc:
        LOG.info("[%s] request failed %s %s %s: %s: %s",
                 source, direction, target, path or "/",
                 type(exc).__name__, exc)
        raise
    else:
        value = outcome.get("value")
        if incoming:
            if mode != "msg":
                # The SDK still needs to validate and transmit this return value.
                LOG.info("[%s] response returned to %s %s: %s",
                         source, target, path or "/", _content(value))
        else:
            LOG.info("[%s] response received from %s %s: %s",
                     source, target, path or "/", _content(value))


def log_handler(role):
    """Wrap before @session.on; preserve async dispatch and exceptions unchanged."""
    def decorate(handler):
        def traffic(request):
            return _traffic(role, incoming=True,
                            peer=getattr(request, "from_agent_id", None),
                            path=getattr(request, "path", None),
                            content=getattr(request, "body", None),
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
    with _traffic(role, incoming=False, peer=target, path=options.get("path"),
                  content=payload) as outcome:
        outcome["value"] = session.request(target, payload, **options)
        return outcome["value"]


async def request_async(session, role, target, payload, **options):
    with _traffic(role, incoming=False, peer=target, path=options.get("path"),
                  content=payload) as outcome:
        outcome["value"] = await session.request(target, payload, **options)
        return outcome["value"]
