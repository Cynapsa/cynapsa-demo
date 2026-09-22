"""Bounded application handoff for native deliveries."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import logging
import queue
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Awaitable

from cynapsa.events import AztmEvent, DiagnosticLog, MessageReceived, decode_event
from cynapsa.exceptions import RPCException
from cynapsa.http import CynapsaRequest, CynapsaResponse, HTTPRequestPayload
from cynapsa.messaging import NativePayload, _application_path, _canonical_size
from cynapsa.native.command import MAX_IDENTIFIER_BYTES, MAX_QUEUE_CAPACITY


_LOGGER = logging.getLogger("cynapsa.handlers")


_LOCAL_DIAGNOSTICS = {
    "automatic_reply_failed": "The automatic application reply could not be sent",
    "delivery_accept_failed": (
        "The SDK could not assume local responsibility for an inbound message"
    ),
    "diagnostic_decode_failed": (
        "The native diagnostic did not conform to the public diagnostic contract"
    ),
    "diagnostic_queue_full": "The bounded application diagnostic queue is full",
    "duplicate_event": "A duplicate inbound event was ignored",
    "event_decode_failed": (
        "The native event did not conform to the public event contract"
    ),
    "event_id_reused": "A reused inbound event identifier was rejected",
    "event_queue_full": "The bounded application event queue is full",
    "handler_error": "The application handler failed",
    "handler_dispatch_failed": "The application handler could not be dispatched",
    "handler_not_found": (
        "No application handler owns this message; delivery remains unaccepted"
    ),
    "handler_queue_full": "The bounded application handler queue is full",
    "payload_too_large": "The inbound payload exceeds the configured limit",
}


def _optional_identifier(value: str | None, field: str) -> None:
    if value is None:
        return
    if type(value) is not str or not value:
        raise ValueError(f"{field} must be nonempty text when present")
    try:
        size = len(value.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise ValueError(f"{field} contains invalid Unicode") from exc
    if size > MAX_IDENTIFIER_BYTES:
        raise ValueError(f"{field} exceeds its limit")


@dataclass(frozen=True, slots=True)
class AztmLocalDiagnostic:
    code: str
    message: str
    event_id: str | None = None
    message_id: str | None = None

    def __post_init__(self) -> None:
        if self.code not in _LOCAL_DIAGNOSTICS:
            raise ValueError("local diagnostic code is unknown")
        if self.message != _LOCAL_DIAGNOSTICS[self.code]:
            raise ValueError("local diagnostic message is not normalized")
        _optional_identifier(self.event_id, "event_id")
        _optional_identifier(self.message_id, "message_id")


@dataclass(slots=True)
class _DispatchItem:
    event: AztmEvent
    message: MessageReceived
    handler: Callable[[Any], Any]
    accepted: threading.Event
    accept_succeeded: bool = False


class InboundRuntime:
    """Own stream consumers, bounded handler workers, and local routing."""

    def __init__(self, owner: Any, session: Any, *, asynchronous: bool) -> None:
        self._owner = owner
        self._session = session
        self._asynchronous = asynchronous
        self._capacity = owner.core._queue_limit
        self._handlers: dict[str, Callable[[Any], Any]] = {}
        self._handlers_lock = threading.RLock()
        self._handlers_changed = threading.Condition(self._handlers_lock)
        self._handler_queue: queue.Queue[_DispatchItem] = queue.Queue(self._capacity)
        self._events: queue.Queue[AztmEvent] = queue.Queue(self._capacity)
        self._diagnostics: queue.Queue[AztmEvent] = queue.Queue(self._capacity)
        self._local_diagnostics: queue.Queue[AztmLocalDiagnostic] = queue.Queue(
            self._capacity
        )
        self._local_diagnostics_lock = threading.Lock()
        self._stop = threading.Event()
        self._seen_lock = threading.Lock()
        # Retain cryptographic tombstones independently of the configured
        # dispatch capacity. This remains bounded at the frozen V1 maximum but
        # does not immediately forget old IDs in deliberately tiny test or
        # application queues.
        self._seen: OrderedDict[bytes, bytes] = OrderedDict()
        worker_count = min(8, self._capacity)
        self._workers = tuple(
            threading.Thread(
                target=self._handler_worker,
                name=f"cynapsa-handler-{index + 1}",
                daemon=True,
            )
            for index in range(worker_count)
        )
        self._consumers = (
            threading.Thread(
                target=self._consume_events,
                name="cynapsa-event-consumer",
                daemon=True,
            ),
            threading.Thread(
                target=self._consume_diagnostics,
                name="cynapsa-diagnostic-consumer",
                daemon=True,
            ),
        )
        for thread in (*self._workers, *self._consumers):
            thread.start()

    @staticmethod
    def _route_path(path: object) -> str:
        if path == "*":
            return "*"
        return _application_path(path)

    def register(self, path: object, handler: object) -> Callable[[Any], Any]:
        self._owner.require_open()
        selected_path = self._route_path(path)
        if not callable(handler) or inspect.isasyncgenfunction(handler):
            raise TypeError("handler must be a synchronous or asynchronous callable")
        with self._handlers_changed:
            if selected_path in self._handlers:
                raise ValueError("a handler is already registered for this path")
            if len(self._handlers) >= self._capacity:
                raise RuntimeError("the bounded handler registry is full")
            self._handlers[selected_path] = handler
            self._handlers_changed.notify_all()
        return handler  # type: ignore[return-value]

    def unregister(
        self,
        path: object,
        handler: object | None = None,
    ) -> bool:
        self._owner.require_open()
        key = self._route_path(path)
        with self._handlers_changed:
            current = self._handlers.get(key)
            if current is None:
                return False
            if handler is not None and current is not handler:
                return False
            del self._handlers[key]
            self._handlers_changed.notify_all()
            return True

    def _handler_for_locked(self, path: str) -> Callable[[Any], Any] | None:
        return self._handlers.get(path) or self._handlers.get("*")

    def _local(
        self,
        code: str,
        *,
        event_id: str | None = None,
        message_id: str | None = None,
    ) -> None:
        diagnostic = AztmLocalDiagnostic(
            code, _LOCAL_DIAGNOSTICS[code], event_id, message_id
        )
        with self._local_diagnostics_lock:
            try:
                self._local_diagnostics.put_nowait(diagnostic)
                return
            except queue.Full:
                # Keep the stream bounded while ensuring the newest failure is
                # observable instead of being hidden behind stale diagnostics.
                try:
                    self._local_diagnostics.get_nowait()
                except queue.Empty:
                    pass
            try:
                self._local_diagnostics.put_nowait(diagnostic)
            except queue.Full:
                # A concurrent reader/producers race can only retain another
                # normalized diagnostic in the same bounded queue.
                pass

    def _remember(self, event_id: str, raw: bytes) -> str:
        identifier = hashlib.sha256(event_id.encode("utf-8")).digest()
        fingerprint = hashlib.sha256(raw).digest()
        with self._seen_lock:
            previous = self._seen.get(identifier)
            if previous is not None:
                return "duplicate" if previous == fingerprint else "reused"
            self._seen[identifier] = fingerprint
            while len(self._seen) > MAX_QUEUE_CAPACITY:
                self._seen.popitem(last=False)
            return "new"

    def _forget(self, event_id: str) -> None:
        identifier = hashlib.sha256(event_id.encode("utf-8")).digest()
        with self._seen_lock:
            self._seen.pop(identifier, None)

    def _accept(self, item: _DispatchItem) -> None:
        try:
            completion = self._owner.execute(
                "delivery.accept", {"event_id": item.event.event_id}
            )
            if (
                not completion.ok
                or completion.result_type != "empty"
                or completion.result is None
                or len(completion.result) != 0
            ):
                raise RuntimeError("nonconforming delivery acceptance")
            item.accept_succeeded = True
        except BaseException:
            self._local(
                "delivery_accept_failed",
                event_id=item.event.event_id,
                message_id=item.message.message_id,
            )
        finally:
            item.accepted.set()

    def _consume_events(self) -> None:
        core = self._owner.core
        while not self._stop.is_set() and core is not None:
            try:
                raw = core._next_callback_event(diagnostic=False, timeout=0.005).payload
            except queue.Empty:
                continue
            except RuntimeError:
                return
            try:
                event = decode_event(raw, diagnostic=False)
            except (TypeError, ValueError):
                self._local("event_decode_failed")
                continue
            if self._stop.is_set():
                return
            if not isinstance(event.payload, MessageReceived):
                try:
                    self._events.put_nowait(event)
                except queue.Full:
                    self._local(
                        "event_queue_full",
                        event_id=event.event_id,
                    )
                continue
            payload = event.payload.payload
            if type(payload) is NativePayload:
                payload_size = _canonical_size(
                    payload.content_type,
                    payload.path,
                    payload.body,
                )
            elif type(payload) is HTTPRequestPayload:
                payload_size = payload.inline_size
            else:
                self._local(
                    "event_decode_failed",
                    event_id=event.event_id,
                    message_id=event.payload.message_id,
                )
                continue
            if payload_size > self._owner.payload_limit:
                self._local(
                    "payload_too_large",
                    event_id=event.event_id,
                    message_id=event.payload.message_id,
                )
                continue
            remembered = self._remember(event.event_id, raw)
            if remembered != "new":
                self._local(
                    "duplicate_event" if remembered == "duplicate" else "event_id_reused",
                    event_id=event.event_id,
                    message_id=event.payload.message_id,
                )
                continue
            handler = self._wait_for_handler(event.payload.payload.path, event=event)
            if handler is None:
                self._forget(event.event_id)
                return
            item = _DispatchItem(
                event,
                event.payload,
                handler,
                threading.Event(),
            )
            saturated = False
            while not self._stop.is_set():
                try:
                    self._handler_queue.put(item, timeout=0.005)
                    break
                except queue.Full:
                    if saturated:
                        continue
                    saturated = True
                    self._local(
                        "handler_queue_full",
                        event_id=event.event_id,
                        message_id=event.payload.message_id,
                    )
            else:
                self._forget(event.event_id)
                return
            # The event is now retained by the tracked SDK queue. Acceptance is
            # deliberately submitted here, before any worker may invoke it.
            self._accept(item)

    def _report_missing_handler(self, event: AztmEvent) -> None:
        assert isinstance(event.payload, MessageReceived)
        self._local(
            "handler_not_found",
            event_id=event.event_id,
            message_id=event.payload.message_id,
        )

    def _wait_for_handler(
        self, path: str, *, event: AztmEvent | None = None
    ) -> Callable[[Any], Any] | None:
        reported = False
        with self._handlers_changed:
            while not self._stop.is_set():
                handler = self._handler_for_locked(path)
                if handler is not None:
                    return handler
                if not reported and event is not None:
                    self._report_missing_handler(event)
                    reported = True
                self._handlers_changed.wait(0.05)
        return None

    def _consume_diagnostics(self) -> None:
        core = self._owner.core
        while not self._stop.is_set() and core is not None:
            try:
                raw = core._next_callback_event(diagnostic=True, timeout=0.005).payload
            except queue.Empty:
                continue
            except RuntimeError:
                return
            try:
                event = decode_event(raw, diagnostic=True)
            except (TypeError, ValueError):
                self._local("diagnostic_decode_failed")
                continue
            assert isinstance(event.payload, DiagnosticLog)
            try:
                self._diagnostics.put_nowait(event)
            except queue.Full:
                self._local(
                    "diagnostic_queue_full",
                    event_id=event.event_id,
                )

    async def _invoke_async(self, item: _DispatchItem, request: Any) -> None:
        try:
            result = item.handler(request)  # type: ignore[misc]
            if inspect.isawaitable(result):
                result = await result
        except RPCException as exc:
            if item.message.mode != "rpc":
                self._handler_failed(item)
                return
            try:
                response = CynapsaResponse.from_rpc_exception(exc)
            except BaseException as conversion_error:
                _LOGGER.exception(
                    "Cynapsa RPCException could not be normalized",
                    exc_info=conversion_error,
                )
                self._handler_failed(item)
                response = CynapsaResponse.handler_error()
        except BaseException as exc:
            _LOGGER.exception("Cynapsa application handler failed", exc_info=exc)
            self._handler_failed(item)
            if item.message.mode != "rpc":
                return
            response = CynapsaResponse.handler_error()
        else:
            if item.message.mode != "rpc":
                return
            try:
                response = CynapsaResponse.from_value(result)
            except BaseException as exc:
                _LOGGER.exception(
                    "Cynapsa handler return value could not be normalized", exc_info=exc
                )
                self._handler_failed(item)
                response = CynapsaResponse.handler_error()
        handle = item.message.request_handle
        if handle is None:
            self._handler_failed(item)
            return
        try:
            await self._session._reply(handle, response)
        except BaseException:
            self._local(
                "automatic_reply_failed",
                event_id=item.event.event_id,
                message_id=item.message.message_id,
            )

    def _handler_failed(self, item: _DispatchItem) -> None:
        self._local(
            "handler_error",
            event_id=item.event.event_id,
            message_id=item.message.message_id,
        )

    @staticmethod
    async def _await_handler(result: Awaitable[Any]) -> Any:
        return await result

    def _invoke_sync(self, item: _DispatchItem, request: Any) -> None:
        try:
            result = item.handler(request)  # type: ignore[misc]
            if inspect.isawaitable(result):
                result = asyncio.run(self._await_handler(result))
        except RPCException as exc:
            if item.message.mode != "rpc":
                self._handler_failed(item)
                return
            try:
                response = CynapsaResponse.from_rpc_exception(exc)
            except BaseException as conversion_error:
                _LOGGER.exception(
                    "Cynapsa RPCException could not be normalized",
                    exc_info=conversion_error,
                )
                self._handler_failed(item)
                response = CynapsaResponse.handler_error()
        except BaseException as exc:
            _LOGGER.exception("Cynapsa application handler failed", exc_info=exc)
            self._handler_failed(item)
            if item.message.mode != "rpc":
                return
            response = CynapsaResponse.handler_error()
        else:
            if item.message.mode != "rpc":
                return
            try:
                response = CynapsaResponse.from_value(result)
            except BaseException as exc:
                _LOGGER.exception(
                    "Cynapsa handler return value could not be normalized", exc_info=exc
                )
                self._handler_failed(item)
                response = CynapsaResponse.handler_error()
        handle = item.message.request_handle
        if handle is None:
            self._handler_failed(item)
            return
        try:
            self._session._reply(handle, response)
        except BaseException:
            self._local(
                "automatic_reply_failed",
                event_id=item.event.event_id,
                message_id=item.message.message_id,
            )

    def _handler_worker(self) -> None:
        while True:
            try:
                item = self._handler_queue.get(timeout=0.005)
            except queue.Empty:
                if self._stop.is_set() and all(
                    not thread.is_alive() for thread in self._consumers
                ):
                    return
                continue
            try:
                item.accepted.wait()
                if not item.accept_succeeded:
                    continue
                request = self._session._new_request(
                    message_id=item.message.message_id,
                    conversation_id=item.message.conversation_id,
                    from_agent_id=item.message.from_agent_id,
                    mesh_id=item.message.mesh_id,
                    payload=item.message.payload,
                    request_handle=item.message.request_handle,
                    mode=item.message.mode,
                )
                if self._asynchronous:
                    asyncio.run(self._invoke_async(item, request))
                else:
                    self._invoke_sync(item, request)
            except BaseException:
                self._local(
                    "handler_dispatch_failed",
                    event_id=item.event.event_id,
                    message_id=item.message.message_id,
                )
            finally:
                self._handler_queue.task_done()

    def _next(self, source: queue.Queue[Any], timeout: float | None) -> Any:
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be nonnegative or None")
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if self._stop.is_set() and source.empty():
                raise RuntimeError("the inbound runtime is closed")
            wait = 0.05
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return source.get_nowait()
                wait = min(wait, remaining)
            try:
                return source.get(timeout=wait)
            except queue.Empty:
                if deadline is not None and time.monotonic() >= deadline:
                    raise

    def next_event(self, timeout: float | None = None) -> AztmEvent:
        return self._next(self._events, timeout)

    def next_diagnostic(self, timeout: float | None = None) -> AztmEvent:
        return self._next(self._diagnostics, timeout)

    def next_local_diagnostic(
        self, timeout: float | None = None
    ) -> AztmLocalDiagnostic:
        return self._next(self._local_diagnostics, timeout)

    def close(self, timeout: float) -> None:
        self._stop.set()
        with self._handlers_changed:
            self._handlers_changed.notify_all()
        deadline = time.monotonic() + max(0.0, timeout)
        for thread in (*self._consumers, *self._workers):
            remaining = max(0.0, deadline - time.monotonic())
            thread.join(remaining)
        if any(thread.is_alive() for thread in self._workers):
            raise TimeoutError("application handlers did not drain before session close")


__all__ = ["AztmLocalDiagnostic", "InboundRuntime"]
