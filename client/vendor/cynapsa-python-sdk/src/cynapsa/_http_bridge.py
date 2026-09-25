"""HTTP Bridge lifecycle, selective client hooks, and explicit ASGI routing."""

from __future__ import annotations

import asyncio
import functools
import hashlib
import importlib
import inspect
import io
import logging
import math
import queue
import re
import socket
import sys
import threading
import time
import urllib.error as _urllib_error
import urllib.response as _urllib_response
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from email.message import Message
from http import HTTPStatus
from types import SimpleNamespace
from typing import Any, NoReturn
from urllib.parse import quote, urljoin, urlsplit

import cynapsa.session as _session
from cynapsa.events import MessageReceived, decode_event
from cynapsa.http import HTTPRequestPayload, HTTPResponsePayload
from cynapsa.native.command import (
    MAX_QUEUE_CAPACITY,
    MAX_TIMEOUT_MS,
    _validate_agent_id,
)


_LOGGER = logging.getLogger("cynapsa.asgi")


_DNS_LABEL = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$")
_PERCENT_ESCAPE = re.compile(r"%[0-9A-Fa-f]{2}")
_SYNTHETIC_RESPONSE = HTTPResponsePayload(200, "OK", (), b"")


@dataclass(frozen=True, slots=True)
class AddressMapping:
    virtual_origin: str
    recipient: str
    mode: str


@dataclass(frozen=True, slots=True)
class _Route:
    spelling: str
    key: str
    recipient: str
    mode: str


@dataclass(frozen=True, slots=True)
class _Address:
    key: str
    path: str
    query: str


def _valid_hostname(host: str) -> bool:
    if not host or len(host) > 253 or host.endswith("."):
        return False
    try:
        host.encode("ascii")
    except UnicodeEncodeError:
        return False
    return all(_DNS_LABEL.fullmatch(label) is not None for label in host.split("."))


def _escaped_path(value: str, field: str) -> str:
    """Mirror net/url.EscapedPath without decoding valid raw escapes."""

    index = 0
    while index < len(value):
        if value[index] == "%":
            if _PERCENT_ESCAPE.match(value, index) is None:
                raise ValueError(f"{field} is invalid")
            index += 3
            continue
        index += 1
    # Go's URL parser rejects ASCII controls before EscapedPath. Python's URL
    # splitter removes some of them, so reject the original input separately
    # in _parse_address and percent-encode the remaining path characters here.
    return quote(value, safe="/!$&'()*+,-.:;=@_%~")


def _parse_address(raw: object, *, origin_only: bool) -> _Address:
    field = "virtual_origin" if origin_only else "url"
    if type(raw) is not str or not raw:
        raise ValueError(f"{field} is invalid")
    try:
        encoded = raw.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError(f"{field} is invalid") from exc
    if len(encoded) > 8192 or any(
        ord(character) < 0x20 or ord(character) == 0x7F for character in raw
    ):
        raise ValueError(f"{field} is invalid")
    if raw.strip() != raw:
        raise ValueError(f"{field} is invalid")
    try:
        parsed = urlsplit(raw)
        port = parsed.port
    except (UnicodeError, ValueError) as exc:
        raise ValueError(f"{field} is invalid") from exc
    host = parsed.hostname
    if (
        parsed.scheme.lower() not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or host is None
        or not _valid_hostname(host)
        or parsed.netloc.endswith(":")
        or "[" in parsed.netloc
        or "]" in parsed.netloc
        or (port is not None and not 1 <= port <= 65_535)
        or (origin_only and (parsed.path or parsed.query))
    ):
        raise ValueError(f"{field} is invalid")
    scheme = parsed.scheme.lower()
    key = f"{scheme}://{host.lower()}"
    if port is not None and not (
        (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
    ):
        key += f":{port}"
    path = _escaped_path(parsed.path, field) if parsed.path else "/"
    return _Address(key, path, parsed.query)


def _origin_key_for_lookup(raw: object) -> str | None:
    try:
        return _parse_address(raw, origin_only=False).key
    except (TypeError, UnicodeError, ValueError):
        return None


def _potential_origin_key_for_lookup(raw: object) -> str | None:
    """Identify an owned HTTP origin before strict full-URL validation.

    A fragment or userinfo makes a Cynapsa URL invalid, but it must not turn a
    request for an otherwise mapped origin into an accidental real-network
    request. Full validation still happens in ``_parse_address`` before any
    canonical request is submitted.
    """

    if type(raw) is not str or not raw:
        return None
    try:
        parsed = urlsplit(raw)
        port = parsed.port
    except (UnicodeError, ValueError):
        return None
    host = parsed.hostname
    scheme = parsed.scheme.lower()
    if (
        scheme not in {"http", "https"}
        or host is None
        or not _valid_hostname(host)
    ):
        return None
    key = f"{scheme}://{host.lower()}"
    if port is not None and not (
        (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
    ):
        key += f":{port}"
    return key


def _recipient(value: object) -> str:
    return _validate_agent_id(value, "recipient")


def _address_map(value: object) -> tuple[_Route, ...]:
    if not isinstance(value, Mapping):
        raise TypeError("address_map must be a mapping")
    if len(value) > MAX_QUEUE_CAPACITY:
        raise ValueError("address_map exceeds its bounded mapping limit")
    output: list[_Route] = []
    normalized: set[str] = set()
    for spelling, raw_target in value.items():
        address = _parse_address(spelling, origin_only=True)
        if address.key in normalized:
            raise ValueError("address_map contains duplicate normalized origins")
        normalized.add(address.key)
        if not isinstance(raw_target, Mapping) or set(raw_target) != {
            "recipient",
            "mode",
        }:
            raise ValueError(
                "each address_map value must contain exactly recipient and mode"
            )
        mode = raw_target["mode"]
        if type(mode) is not str or mode not in {"rpc", "msg"}:
            raise ValueError("address mapping mode must be 'rpc' or 'msg'")
        output.append(
            _Route(spelling, address.key, _recipient(raw_target["recipient"]), mode)
        )
    return tuple(output)


def _bytes_body(value: object, *, library: str) -> bytes:
    if value is None:
        return b""
    if type(value) is bytes:
        return memoryview(value).tobytes()
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    if type(value) is str:
        try:
            return value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise ValueError(f"{library} request body contains invalid Unicode") from exc
    raise TypeError(f"streaming or consumed {library} request bodies are unsupported")


def _text_header(value: object, field: str) -> str:
    if type(value) is bytes:
        try:
            return value.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise ValueError(f"{field} is not UTF-8") from exc
    if type(value) is str:
        return value
    return str(value)


def _mapping_headers(value: object) -> tuple[tuple[str, str], ...]:
    if value is None:
        return ()
    if hasattr(value, "iteritems"):
        items = value.iteritems()  # urllib3.HTTPHeaderDict preserves duplicates
    elif hasattr(value, "items"):
        items = value.items()
    else:
        items = value
    return tuple(
        (_text_header(name, "header name"), _text_header(text, "header value"))
        for name, text in items
    )


def _timeout_seconds(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, tuple):
        value = value[-1] if value else None
    if isinstance(value, Mapping):
        value = value.get("read", value.get("total"))
    for field in ("total", "read", "read_timeout", "_read"):
        candidate = getattr(value, field, None)
        if callable(candidate):
            try:
                candidate = candidate()
            except (TypeError, ValueError):
                candidate = None
        if candidate is not None:
            value = candidate
            break
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError("HTTP client timeout must be a positive finite number")
    return number


def _ttl_ms(owner: _session._LifecycleOwner, timeout: float | None) -> int:
    if timeout is None:
        return owner.rpc_timeout_ms
    milliseconds = max(1, math.ceil(timeout * 1000))
    return min(milliseconds, MAX_TIMEOUT_MS)


def _requests_timeout_error(
    requests: Any, request: Any, error: BaseException
) -> BaseException:
    return requests.exceptions.ReadTimeout(str(error), request=request)


def _httpx_timeout_error(
    httpx: Any, request: Any, error: BaseException
) -> BaseException:
    return httpx.ReadTimeout(str(error), request=request)


def _urllib3_timeout_error(
    urllib3: Any, pool: Any, url: str, error: BaseException
) -> BaseException:
    return urllib3.exceptions.ReadTimeoutError(pool, url, str(error))


def _urllib_request_timeout_error() -> BaseException:
    # urlopen reports connection/request timeouts as URLError with a native
    # timeout reason. Do not carry Core diagnostics across the HTTP boundary.
    return _urllib_error.URLError(socket.timeout("timed out"))


def _aiohttp_timeout_error() -> BaseException:
    # aiohttp exposes cumulative request expiry as asyncio.TimeoutError. Keep
    # Core and SDK implementation details out of the monkey-patched surface.
    return asyncio.TimeoutError()


def _raise_http_timeout(error: BaseException) -> NoReturn:
    # This is called after leaving the NativeError handler so Python does not
    # retain that internal exception as the public HTTP exception's context.
    error.__cause__ = None
    error.__context__ = None
    raise error.with_traceback(None) from None


def _is_rpc_timeout(error: BaseException) -> bool:
    """Recognize both Core's deadline and the SDK's terminal-completion guard."""

    return (
        isinstance(error, _session.NativeError) and error.code == "rpc_timeout"
    ) or isinstance(error, _session.SdkSafetyTimeout)


def _canonical_response(completion: Any, payload_limit: int) -> HTTPResponsePayload:
    response = _session._response_model(
        completion,
        payload_limit,
        expected_payload=HTTPResponsePayload,
    )
    assert type(response.payload) is HTTPResponsePayload
    return response.payload


class _Bridge:
    def __init__(
        self,
        owner: _session._LifecycleOwner,
        identity: _session._Identity,
        mappings: tuple[_Route, ...],
        *,
        asgi_loop: asyncio.AbstractEventLoop | None,
    ) -> None:
        self.owner = owner
        self.identity = identity
        self.mapping_entries = mappings
        self.routes = {route.key: route for route in mappings}
        self._asgi_loop = asgi_loop
        self._asgi: _ASGIRuntime | None = None
        self._asgi_lock = threading.Lock()
        self._close_lock = threading.Lock()
        self._state = threading.Condition()
        self._closing = False
        self._active = 0
        self._closed = False
        self._registered = False

    @property
    def origin_keys(self) -> tuple[str, ...]:
        return tuple(self.routes)

    def require_open(self) -> None:
        with self._state:
            if self._closing or self._closed:
                raise _session._sdk_error(
                    "shutdown_in_progress",
                    _session.PUBLIC_ERROR_MESSAGES["shutdown_in_progress"],
                )
        self.owner.require_open()

    def _begin_operation(self) -> None:
        with self._state:
            if self._closing or self._closed:
                raise _session._sdk_error(
                    "shutdown_in_progress",
                    _session.PUBLIC_ERROR_MESSAGES["shutdown_in_progress"],
                )
            self._active += 1

    def _end_operation(self) -> None:
        with self._state:
            self._active -= 1
            self._state.notify_all()

    def attach_asgi(
        self, app: object, *, loop: asyncio.AbstractEventLoop | None = None
    ) -> None:
        self._begin_operation()
        try:
            self.owner.require_open()
            if not callable(app):
                raise TypeError("asgi_app must be callable")
            with self._asgi_lock:
                if self._asgi is not None:
                    raise ValueError("this HTTP Bridge already owns an ASGI app")
                self._asgi = _ASGIRuntime(
                    self,
                    app,
                    loop=self._asgi_loop if loop is None else loop,
                )
        finally:
            self._end_operation()

    def dispatch_sync(
        self,
        payload: HTTPRequestPayload,
        *,
        origin_key: str,
        timeout: float | None,
    ) -> HTTPResponsePayload:
        self._begin_operation()
        failure: BaseException | None = None
        try:
            return self._dispatch_sync(
                payload, origin_key=origin_key, timeout=timeout
            )
        except BaseException as exc:
            failure = exc
        finally:
            self._end_operation()
        assert failure is not None
        _session._raise_public(failure)

    def _dispatch_sync(
        self,
        payload: HTTPRequestPayload,
        *,
        origin_key: str,
        timeout: float | None,
    ) -> HTTPResponsePayload:
        if payload.inline_size > self.owner.payload_limit:
            raise _session._payload_too_large()
        route = self.routes.get(origin_key)
        if route is None:
            raise RuntimeError("HTTP Bridge routing ownership changed")
        if route.mode == "msg":
            completion = self.owner.execute(
                "message.send",
                {"to": route.recipient, "payload": payload._wire()},
                timeout=timeout,
            )
            _session._send_result_model(completion, "message.send")
            return _SYNTHETIC_RESPONSE
        ttl_ms = _ttl_ms(self.owner, timeout)
        completion = self.owner.execute(
            "message.request",
            {
                "to": route.recipient,
                "payload": payload._wire(),
                "ttl_ms": ttl_ms,
            },
            timeout=_session._request_wait_timeout_seconds(ttl_ms),
            timeout_error=_session._request_safety_timeout(),
        )
        return _canonical_response(completion, self.owner.payload_limit)

    async def dispatch_async(
        self,
        payload: HTTPRequestPayload,
        *,
        origin_key: str,
        timeout: float | None,
    ) -> HTTPResponsePayload:
        def operation(cancelled: threading.Event) -> HTTPResponsePayload:
            self._begin_operation()
            try:
                if payload.inline_size > self.owner.payload_limit:
                    raise _session._payload_too_large()
                route = self.routes.get(origin_key)
                if route is None:
                    raise RuntimeError("HTTP Bridge routing ownership changed")
                if route.mode == "msg":
                    completion = self.owner.execute(
                        "message.send",
                        {"to": route.recipient, "payload": payload._wire()},
                        cancel_event=cancelled,
                        timeout=timeout,
                    )
                    _session._send_result_model(completion, "message.send")
                    return _SYNTHETIC_RESPONSE
                ttl_ms = _ttl_ms(self.owner, timeout)
                completion = self.owner.execute(
                    "message.request",
                    {
                        "to": route.recipient,
                        "payload": payload._wire(),
                        "ttl_ms": ttl_ms,
                    },
                    cancel_event=cancelled,
                    timeout=_session._request_wait_timeout_seconds(ttl_ms),
                    timeout_error=_session._request_safety_timeout(),
                )
                return _canonical_response(completion, self.owner.payload_limit)
            finally:
                self._end_operation()

        try:
            return await _session._cancellable_offload(operation)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            _session._raise_public(exc)

    def status_sync(
        self, *, cancel_event: threading.Event | None = None
    ) -> _session.AztmStatus:
        self._begin_operation()
        try:
            result = _session._require_result(
                self.owner.execute("core.status", cancel_event=cancel_event),
                "status",
                "core.status",
            )
            return _session._status_model(result, self.identity)
        finally:
            self._end_operation()

    def mappings_sync(
        self, *, cancel_event: threading.Event | None = None
    ) -> tuple[AddressMapping, ...]:
        self._begin_operation()
        try:
            result = _session._require_result(
                self.owner.execute(
                    "address.map.list", cancel_event=cancel_event
                ),
                "address_mappings",
                "address.map.list",
            )
            if set(result) != {"mappings"} or not isinstance(result["mappings"], tuple):
                raise _session._contract_error(
                    "address.map.list", "invalid mapping result"
                )
            output: list[AddressMapping] = []
            returned: set[str] = set()
            try:
                for item in result["mappings"]:
                    if not isinstance(item, Mapping) or set(item) != {
                        "virtual_origin", "recipient"
                    }:
                        raise ValueError("invalid mapping")
                    address = _parse_address(item["virtual_origin"], origin_only=True)
                    recipient = _recipient(item["recipient"])
                    route = self.routes.get(address.key)
                    if (
                        route is None
                        or address.key in returned
                        or route.recipient != recipient
                    ):
                        raise ValueError("inconsistent mapping")
                    returned.add(address.key)
                    output.append(
                        AddressMapping(route.spelling, recipient, route.mode)
                    )
                if returned != set(self.routes):
                    raise ValueError("incomplete mapping result")
            except (TypeError, ValueError) as exc:
                raise _session._contract_error(
                    "address.map.list", "invalid mapping result"
                ) from exc
            return tuple(output)
        finally:
            self._end_operation()

    def close_sync(self) -> None:
        with self._close_lock:
            if self._closed:
                return
            with self._state:
                self._closing = True
                while self._active:
                    self._state.wait()
            _PATCH_MANAGER.unregister(self)
            first_error: BaseException | None = None
            if self._asgi is not None:
                try:
                    self._asgi.close(self.owner.command_timeout)
                except BaseException as exc:
                    first_error = exc
            if first_error is not None:
                # Accepted ASGI work must finish before mappings/auth/Core can
                # be retired. Keep the native owner recoverable for a later
                # idempotent close instead of overtaking the bounded drain.
                _session._retain_owner(self.owner)
                raise first_error
            if self.owner.core is not None:
                for route in reversed(self.mapping_entries):
                    try:
                        completion = self.owner.execute(
                            "address.map.remove", {"virtual_origin": route.spelling}
                        )
                        _session._require_result(
                            completion, "empty", "address.map.remove"
                        )
                    except BaseException as exc:
                        if first_error is None:
                            first_error = exc
            try:
                self.owner.close()
            except BaseException as exc:
                _session._retain_owner(self.owner)
                if first_error is None:
                    first_error = exc
            else:
                _session._release_owner(self.owner)
            with self._state:
                self._closed = self.owner.core is None
                if not self._closed:
                    # A recoverable native teardown failure can be joined by a
                    # later idempotent close, but HTTP operations stay closed.
                    self._closing = True
            if first_error is not None:
                raise first_error


class _PatchManager:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._bridges: set[_Bridge] = set()
        self._origins: dict[str, _Bridge] = {}
        self._originals: list[tuple[object, str, object, object]] = []

    def ensure_available(self, keys: tuple[str, ...]) -> None:
        with self._lock:
            conflict = next((key for key in keys if key in self._origins), None)
            if conflict is not None:
                raise ValueError("an HTTP Bridge already owns a mapped origin")

    def register(self, bridge: _Bridge) -> None:
        with self._lock:
            self.ensure_available(bridge.origin_keys)
            if not self._bridges:
                self._install()
            for key in bridge.origin_keys:
                self._origins[key] = bridge
            self._bridges.add(bridge)
            bridge._registered = True

    def unregister(self, bridge: _Bridge) -> None:
        with self._lock:
            if bridge not in self._bridges:
                bridge._registered = False
                return
            self._bridges.remove(bridge)
            for key, owner in tuple(self._origins.items()):
                if owner is bridge:
                    del self._origins[key]
            bridge._registered = False
            if not self._bridges:
                self._restore()

    def bridge_for(self, url: object) -> tuple[_Bridge, str] | None:
        key = _origin_key_for_lookup(url)
        if key is None:
            return None
        with self._lock:
            bridge = self._origins.get(key)
            return None if bridge is None else (bridge, key)

    def urllib_bridge_for(self, url: object) -> tuple[_Bridge, str] | None:
        route = self.bridge_for(url)
        if route is not None:
            return route
        key = _potential_origin_key_for_lookup(url)
        if key is None:
            return None
        with self._lock:
            bridge = self._origins.get(key)
            return None if bridge is None else (bridge, key)

    def only_bridge(self) -> _Bridge:
        with self._lock:
            if len(self._bridges) != 1:
                raise RuntimeError("ASGI ownership is ambiguous")
            return next(iter(self._bridges))

    def _patch(self, owner: object, name: str, replacement: object) -> None:
        original = getattr(owner, name)
        setattr(owner, name, replacement)
        self._originals.append((owner, name, original, replacement))

    @staticmethod
    def _optional_module(name: str) -> Any | None:
        try:
            return importlib.import_module(name)
        except ModuleNotFoundError as exc:
            # A genuinely absent optional top-level client is supported. A
            # broken installed client with a missing transitive dependency is
            # an installation error and must not be silently hidden.
            if exc.name == name:
                return None
            raise

    def _install(self) -> None:
        installed_at_start = len(self._originals)
        try:
            urllib_request = self._optional_module("urllib.request")
            if urllib_request is not None:
                urllib_request_original = urllib_request.urlopen

                @functools.wraps(urllib_request_original)
                def urllib_request_urlopen(*args: Any, **kwargs: Any) -> Any:
                    candidate = args[0] if args else kwargs.get("url")
                    lookup_url = (
                        candidate.full_url
                        if isinstance(candidate, urllib_request.Request)
                        else candidate
                    )
                    route = self.urllib_bridge_for(lookup_url)
                    if route is None:
                        # Preserve the caller's exact argument shape for all
                        # traffic Cynapsa does not own.
                        return urllib_request_original(*args, **kwargs)

                    (
                        url,
                        data,
                        timeout_value,
                        use_global_opener,
                    ) = _urllib_request_arguments(*args, **kwargs)
                    timeout = (
                        None
                        if timeout_value is socket._GLOBAL_DEFAULT_TIMEOUT
                        else _urllib_request_timeout_seconds(timeout_value)
                    )
                    original_origin = route[1]
                    initial_url = (
                        url.full_url
                        if isinstance(url, urllib_request.Request)
                        else url
                    )
                    redirects: set[str] = {initial_url}
                    redirect_handler = urllib_request.HTTPRedirectHandler()
                    while True:
                        raw_url = (
                            url.full_url
                            if isinstance(url, urllib_request.Request)
                            else url
                        )
                        target = _parse_address(raw_url, origin_only=False)
                        request = _prepare_urllib_request(
                            urllib_request,
                            url,
                            data,
                            timeout=timeout_value,
                            use_global_opener=use_global_opener,
                        )
                        bridge, key = route
                        payload = HTTPRequestPayload(
                            request.get_method(),
                            target.path,
                            target.query,
                            _urllib_request_headers(request),
                            _urllib_request_body(request.data),
                        )
                        timeout_error: BaseException | None = None
                        try:
                            canonical = bridge.dispatch_sync(
                                payload,
                                origin_key=key,
                                timeout=timeout,
                            )
                        except BaseException as exc:
                            if _is_rpc_timeout(exc):
                                timeout_error = _urllib_request_timeout_error()
                            else:
                                raise
                        if timeout_error is not None:
                            _raise_http_timeout(timeout_error)
                        response = _urllib_request_response(
                            canonical,
                            url=request.full_url,
                        )
                        status = canonical.status_code
                        if status in (301, 302, 303, 307, 308):
                            location = response.info().get("location") or response.info().get("uri")
                            if location:
                                next_url = urljoin(request.full_url, location)
                                # A mapped response must never escape to an
                                # ordinary HTTP connection or carry caller
                                # credentials to a different virtual origin.
                                next_route = self.urllib_bridge_for(next_url)
                                if (
                                    next_route is None
                                    or next_route[1] != original_origin
                                    or next_url in redirects
                                    or len(redirects) > redirect_handler.max_redirections
                                ):
                                    raise _urllib_error.HTTPError(
                                        request.full_url, status,
                                        "Unsafe or repeated mapped redirect",
                                        response.info(), response,
                                    )
                                next_request = redirect_handler.redirect_request(
                                    request, response, status, canonical.reason,
                                    response.info(), next_url,
                                )
                                if next_request is not None:
                                    redirects.add(next_url)
                                    response.close()
                                    url, data, route = next_request, None, next_route
                                    continue
                        if status == 304 or status >= 300:
                            raise _urllib_error.HTTPError(
                                request.full_url, status, canonical.reason,
                                response.info(), response,
                            )
                        return response

                self._patch(urllib_request, "urlopen", urllib_request_urlopen)

            requests = self._optional_module("requests")
            if requests is not None:
                requests_original = requests.sessions.Session.send

                def requests_send(session: Any, request: Any, **kwargs: Any) -> Any:
                    route = self.bridge_for(getattr(request, "url", None))
                    if route is None:
                        return requests_original(session, request, **kwargs)
                    if isinstance(request, requests.Request):
                        raise ValueError("You can only send PreparedRequests.")
                    bridge, key = route
                    target = _parse_address(request.url, origin_only=False)
                    payload = HTTPRequestPayload(
                        request.method,
                        target.path,
                        target.query,
                        _mapping_headers(request.headers),
                        _bytes_body(request.body, library="requests"),
                    )
                    started = requests.sessions.preferred_clock()
                    timeout_error: BaseException | None = None
                    try:
                        canonical = bridge.dispatch_sync(
                            payload,
                            origin_key=key,
                            timeout=_timeout_seconds(kwargs.get("timeout")),
                        )
                    except BaseException as exc:
                        if _is_rpc_timeout(exc):
                            timeout_error = _requests_timeout_error(
                                requests, request, exc
                            )
                        else:
                            raise
                    if timeout_error is not None:
                        _raise_http_timeout(timeout_error)
                    response = _requests_response(
                        requests, session, request, canonical
                    )
                    response.elapsed = timedelta(
                        seconds=requests.sessions.preferred_clock() - started
                    )
                    # Session.send normally owns response hooks. The bridge is
                    # installed at that prepared boundary, so preserve them.
                    response = requests.hooks.dispatch_hook(
                        "response", request.hooks, response, **kwargs
                    )
                    if hasattr(response, "cookies"):
                        session.cookies.update(response.cookies)
                    if not kwargs.get("stream", session.stream):
                        response.content
                    return response

                self._patch(requests.sessions.Session, "send", requests_send)

            httpx = self._optional_module("httpx")
            if httpx is not None:
                httpx_sync_original = httpx.Client.send
                httpx_async_original = httpx.AsyncClient.send

                def httpx_send(client: Any, request: Any, **kwargs: Any) -> Any:
                    route = self.bridge_for(str(request.url))
                    if route is None:
                        return httpx_sync_original(client, request, **kwargs)
                    bridge, key = route
                    payload = _httpx_payload(request)
                    timeout = _httpx_timeout(request, client)
                    timeout_error: BaseException | None = None
                    try:
                        canonical = bridge.dispatch_sync(
                            payload,
                            origin_key=key,
                            timeout=timeout,
                        )
                    except BaseException as exc:
                        if _is_rpc_timeout(exc):
                            timeout_error = _httpx_timeout_error(
                                httpx, request, exc
                            )
                        else:
                            raise
                    if timeout_error is not None:
                        _raise_http_timeout(timeout_error)
                    return _httpx_response(httpx, request, canonical)

                async def httpx_async_send(
                    client: Any, request: Any, **kwargs: Any
                ) -> Any:
                    route = self.bridge_for(str(request.url))
                    if route is None:
                        return await httpx_async_original(client, request, **kwargs)
                    bridge, key = route
                    payload = _httpx_payload(request)
                    timeout_error: BaseException | None = None
                    try:
                        canonical = await bridge.dispatch_async(
                            payload,
                            origin_key=key,
                            timeout=_httpx_timeout(request, client),
                        )
                    except BaseException as exc:
                        if _is_rpc_timeout(exc):
                            timeout_error = _httpx_timeout_error(
                                httpx, request, exc
                            )
                        else:
                            raise
                    if timeout_error is not None:
                        _raise_http_timeout(timeout_error)
                    return _httpx_response(httpx, request, canonical)

                self._patch(httpx.Client, "send", httpx_send)
                self._patch(httpx.AsyncClient, "send", httpx_async_send)

            # ClientSession.request/get/post/etc. all wrap this coroutine in
            # aiohttp's own _RequestContextManager. Patching here therefore
            # preserves both ``await`` and ``async with`` call shapes while
            # leaving every unmapped call on aiohttp's exact original path.
            aiohttp = self._optional_module("aiohttp")
            if aiohttp is not None:
                aiohttp_original = aiohttp.ClientSession._request
                aiohttp_signature = inspect.signature(aiohttp_original)

                @functools.wraps(aiohttp_original)
                async def aiohttp_request(
                    client: Any,
                    method: str,
                    str_or_url: Any,
                    **kwargs: Any,
                ) -> Any:
                    try:
                        lookup_url = str(client._build_url(str_or_url))
                    except (TypeError, ValueError):
                        # Preserve aiohttp's native URL exception and exact
                        # validation path for values not owned by Cynapsa.
                        return await aiohttp_original(
                            client, method, str_or_url, **kwargs
                        )
                    route = self.urllib_bridge_for(lookup_url)
                    if route is None:
                        return await aiohttp_original(
                            client, method, str_or_url, **kwargs
                        )

                    # Match the installed aiohttp version's accepted keyword
                    # surface before interpreting any mapped request.
                    aiohttp_signature.bind(client, method, str_or_url, **kwargs)
                    # Reject fragments, userinfo, malformed escapes, and other
                    # invalid owned URLs before ClientRequest can normalize
                    # them into a superficially valid network request.
                    _parse_address(lookup_url, origin_only=False)
                    bridge, key = route
                    prepared, timeout = _aiohttp_prepare_request(
                        aiohttp,
                        client,
                        method,
                        str_or_url,
                        kwargs,
                    )
                    target = _parse_address(str(prepared.url), origin_only=False)
                    payload = HTTPRequestPayload(
                        prepared.method,
                        target.path,
                        target.query,
                        tuple(
                            (
                                _text_header(name, "header name"),
                                _text_header(value, "header value"),
                            )
                            for name, value in prepared.headers.items()
                        ),
                        _aiohttp_request_body(prepared),
                    )
                    timeout_error: BaseException | None = None
                    try:
                        canonical = await bridge.dispatch_async(
                            payload,
                            origin_key=key,
                            timeout=timeout,
                        )
                    except BaseException as exc:
                        if _is_rpc_timeout(exc):
                            timeout_error = _aiohttp_timeout_error()
                        else:
                            raise
                    if timeout_error is not None:
                        _raise_http_timeout(timeout_error)

                    response = _aiohttp_response(
                        aiohttp,
                        client,
                        prepared,
                        canonical,
                    )
                    raise_for_status = kwargs.get("raise_for_status")
                    if raise_for_status is None:
                        raise_for_status = client._raise_for_status
                    try:
                        if callable(raise_for_status):
                            await raise_for_status(response)
                        elif raise_for_status:
                            response.raise_for_status()
                    except BaseException:
                        # There is no socket to leak, but an exception before
                        # returning the response must still leave the native
                        # aiohttp object in its terminal released state.
                        response.release()
                        raise
                    return response

                self._patch(aiohttp.ClientSession, "_request", aiohttp_request)

            # urllib3 has a stable prepared boundary and a native response
            # constructor, so direct PoolManager/connection-pool calls are
            # supported. requests calls are already intercepted at Session.send;
            # unmapped requests pass through this hook once and remain unmapped.
            urllib3 = self._optional_module("urllib3")
            if urllib3 is not None:
                pool_class = urllib3.connectionpool.HTTPConnectionPool
                urllib3_original = pool_class.urlopen

                def urllib3_urlopen(
                    pool: Any,
                    method: str,
                    url: str,
                    body: Any = None,
                    headers: Any = None,
                    **kwargs: Any,
                ) -> Any:
                    full_url = _urllib3_url(pool, url)
                    route = self.bridge_for(full_url)
                    if route is None:
                        return urllib3_original(
                            pool, method, url, body, headers, **kwargs
                        )
                    bridge, key = route
                    target = _parse_address(full_url, origin_only=False)
                    payload = HTTPRequestPayload(
                        method,
                        target.path,
                        target.query,
                        _mapping_headers(headers),
                        _bytes_body(body, library="urllib3"),
                    )
                    timeout_error: BaseException | None = None
                    try:
                        canonical = bridge.dispatch_sync(
                            payload,
                            origin_key=key,
                            timeout=_timeout_seconds(kwargs.get("timeout")),
                        )
                    except BaseException as exc:
                        if _is_rpc_timeout(exc):
                            timeout_error = _urllib3_timeout_error(
                                urllib3, pool, full_url, exc
                            )
                        else:
                            raise
                    if timeout_error is not None:
                        _raise_http_timeout(timeout_error)
                    return _urllib3_response(
                        urllib3,
                        canonical,
                        method=method,
                        url=full_url,
                        preload=kwargs.get("preload_content", True),
                    )

                self._patch(pool_class, "urlopen", urllib3_urlopen)
        except BaseException:
            while len(self._originals) > installed_at_start:
                owner, name, original, replacement = self._originals.pop()
                if getattr(owner, name) is replacement:
                    setattr(owner, name, original)
            raise

    def _restore(self) -> None:
        while self._originals:
            owner, name, original, replacement = self._originals.pop()
            # Never clobber a patch installed by another owner after Cynapsa.
            # Exact restoration is guaranteed while our replacement still
            # owns the attribute; otherwise the third party remains in place.
            if getattr(owner, name) is replacement:
                setattr(owner, name, original)


_PATCH_MANAGER = _PatchManager()


def _httpx_timeout(request: Any, client: Any) -> float | None:
    extensions = getattr(request, "extensions", None)
    if isinstance(extensions, Mapping) and "timeout" in extensions:
        return _timeout_seconds(extensions["timeout"])
    return _timeout_seconds(getattr(client, "timeout", None))


def _aiohttp_timeout_seconds(aiohttp: Any, client: Any, value: object) -> float | None:
    if value is aiohttp.helpers.sentinel:
        value = client._timeout
    if value is None:
        return None
    if isinstance(value, aiohttp.ClientTimeout):
        # Core owns one cumulative RPC deadline. Prefer aiohttp's cumulative
        # total and use sock_read only when callers explicitly disable total.
        value = value.total if value.total is not None else value.sock_read
    if value is None:
        return None
    if not isinstance(value, (int, float)):
        raise TypeError("aiohttp timeout must be a number, ClientTimeout, or None")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("aiohttp timeout must be finite")
    # aiohttp treats non-positive cumulative timeouts as disabled. Cynapsa
    # remains bounded by its configured Core RPC timeout in that case.
    return None if number <= 0 else number


def _aiohttp_prepare_request(
    aiohttp: Any,
    client: Any,
    method: str,
    str_or_url: object,
    kwargs: Mapping[str, Any],
) -> tuple[Any, float | None]:
    if client.closed:
        raise RuntimeError("Session is closed")
    if (
        kwargs.get("proxy") is not None
        or kwargs.get("proxy_auth") is not None
        or kwargs.get("proxy_headers") is not None
        or getattr(client, "_default_proxy", None) is not None
        or getattr(client, "_default_proxy_auth", None) is not None
    ):
        raise TypeError(
            "aiohttp proxy options are unsupported for mapped virtual origins"
        )
    data = kwargs.get("data")
    json_value = kwargs.get("json")
    if data is not None and json_value is not None:
        raise ValueError("data and json parameters can not be used at the same time")
    if kwargs.get("compress") is not None:
        raise TypeError("compressed aiohttp request bodies are unsupported")
    if kwargs.get("chunked") not in (None, False):
        raise TypeError("chunked aiohttp request bodies are unsupported")
    if kwargs.get("expect100"):
        raise TypeError("100-continue aiohttp request bodies are unsupported")
    if data is not None and not isinstance(
        data, (bytes, bytearray, memoryview, str)
    ):
        raise TypeError(
            "streaming, multipart, form, and file-like aiohttp request bodies "
            "are unsupported"
        )
    if json_value is not None:
        data = aiohttp.payload.JsonPayload(
            json_value,
            dumps=client._json_serialize,
        )

    url = client._build_url(str_or_url)
    headers = client._prepare_headers(kwargs.get("headers"))
    skip_auto_headers = kwargs.get("skip_auto_headers")
    if skip_auto_headers is not None:
        skip_auto_headers = set(skip_auto_headers) | client._skip_auto_headers
    elif client._skip_auto_headers:
        skip_auto_headers = client._skip_auto_headers
    else:
        # aiohttp 3.9's ClientRequest sorts this value unconditionally,
        # whereas newer versions also accept None.
        skip_auto_headers = set()

    auth = kwargs.get("auth")
    if auth is None and client._default_auth is not None:
        try:
            base_url = getattr(client, "_base_url", None)
            same_origin = base_url is None or base_url.origin() == url.origin()
        except ValueError:
            same_origin = False
        if same_origin:
            auth = client._default_auth

    cookies = client._cookie_jar.filter_cookies(url)
    request_options = {
        "params": kwargs.get("params"),
        "headers": headers,
        "skip_auto_headers": skip_auto_headers,
        "data": data,
        "cookies": cookies,
        "auth": auth,
        "version": client._version,
        "loop": client._loop,
        "response_class": client._response_class,
        "timer": aiohttp.helpers.TimerNoop(),
        "session": client,
        "ssl": kwargs.get("ssl", True),
        "traces": [],
        "trust_env": client.trust_env,
    }
    request_signature = inspect.signature(client._request_class)
    if not any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in request_signature.parameters.values()
    ):
        request_options = {
            name: value
            for name, value in request_options.items()
            if name in request_signature.parameters
        }
    prepared = client._request_class(method, url, **request_options)
    if kwargs.get("cookies") is not None:
        prepared.update_cookies(kwargs["cookies"])
    return prepared, _aiohttp_timeout_seconds(
        aiohttp,
        client,
        kwargs.get("timeout", aiohttp.helpers.sentinel),
    )


def _aiohttp_request_body(prepared: Any) -> bytes:
    body = prepared.body
    if body in (None, b""):
        return b""
    value = getattr(body, "_value", None)
    if isinstance(value, bytes):
        return memoryview(value).tobytes()
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    raise TypeError("streaming or non-buffered aiohttp request bodies are unsupported")


def _httpx_payload(request: Any) -> HTTPRequestPayload:
    target = _parse_address(str(request.url), origin_only=False)
    try:
        content = request.content
    except BaseException as exc:
        raise TypeError(
            "streaming or consumed httpx request bodies are unsupported"
        ) from exc
    raw_headers = getattr(request.headers, "raw", None)
    headers = (
        tuple(
            (_text_header(name, "header name"), _text_header(value, "header value"))
            for name, value in raw_headers
        )
        if raw_headers is not None
        else _mapping_headers(request.headers)
    )
    return HTTPRequestPayload(request.method, target.path, target.query, headers, content)


def _requests_response(
    requests: Any,
    session: Any,
    request: Any,
    payload: HTTPResponsePayload,
) -> Any:
    urllib3 = importlib.import_module("urllib3")
    headers = urllib3._collections.HTTPHeaderDict()
    for name, value in payload.headers:
        headers.add(name, value)
    response = requests.Response()
    response.status_code = payload.status_code
    response.reason = payload.reason
    response.url = request.url
    response.request = request
    # requests cannot represent duplicate fields in Response.headers. Match
    # HTTPAdapter.build_response by comma-combining there while preserving the
    # exact ordered fields in Response.raw.headers.
    response.headers = requests.structures.CaseInsensitiveDict(headers)
    response.encoding = requests.utils.get_encoding_from_headers(response.headers)
    response.raw = urllib3.response.HTTPResponse(
        body=payload.body,
        headers=headers,
        status=payload.status_code,
        version=11,
        version_string="HTTP/1.1",
        reason=payload.reason,
        preload_content=True,
        request_method=request.method,
        request_url=request.url,
    )
    response._content = payload.body
    response._content_consumed = True
    response.connection = session.get_adapter(url=request.url)
    cookie_headers = Message()
    for name, value in payload.headers:
        cookie_headers[name] = value
    response.cookies.extract_cookies(
        requests.cookies.MockResponse(cookie_headers),
        requests.cookies.MockRequest(request),
    )
    return response


def _httpx_response(httpx: Any, request: Any, payload: HTTPResponsePayload) -> Any:
    raw_headers = [
        (name.encode("utf-8"), value.encode("utf-8"))
        for name, value in payload.headers
    ]
    return httpx.Response(
        payload.status_code,
        headers=raw_headers,
        content=payload.body,
        request=request,
        extensions={
            "reason_phrase": payload.reason.encode("utf-8"),
            "http_version": b"HTTP/1.1",
        },
    )


def _aiohttp_response(
    aiohttp: Any,
    client: Any,
    request: Any,
    payload: HTTPResponsePayload,
) -> Any:
    multidict = importlib.import_module("multidict")
    response_headers = multidict.CIMultiDict()
    raw_headers: list[tuple[bytes, bytes]] = []
    raw_cookie_headers: list[str] = []
    for name, value in payload.headers:
        response_headers.add(name, value)
        raw_headers.append((name.encode("utf-8"), value.encode("utf-8")))
        if name.lower() == "set-cookie":
            raw_cookie_headers.append(value)
    response_headers_proxy = multidict.CIMultiDictProxy(response_headers)
    request_headers = multidict.CIMultiDictProxy(
        multidict.CIMultiDict(request.headers)
    )
    request_info = aiohttp.RequestInfo(
        request.url,
        request.method,
        request_headers,
        request.url,
    )
    loop = asyncio.get_running_loop()
    response_options = {
        "writer": None,
        "continue100": None,
        "timer": aiohttp.helpers.TimerNoop(),
        "request_info": request_info,
        "traces": [],
        "loop": loop,
        "session": client,
    }
    if "stream_writer" in inspect.signature(aiohttp.ClientResponse).parameters:
        # Required by aiohttp 3.14+. The bridged response has no socket writer,
        # but ClientResponse uses this value to initialize its output size.
        response_options["stream_writer"] = SimpleNamespace(output_size=0)
    response = client._response_class(
        request.method,
        request.url,
        **response_options,
    )
    response.version = aiohttp.HttpVersion11
    response.status = payload.status_code
    response.reason = payload.reason
    response._headers = response_headers_proxy
    response._raw_headers = tuple(raw_headers)
    response._raw_cookie_headers = tuple(raw_cookie_headers)
    response._connection = None
    response._closed = False
    response._released = False
    response._history = ()
    protocol = aiohttp.base_protocol.BaseProtocol(loop)
    content = aiohttp.StreamReader(
        protocol,
        2**16,
        timer=aiohttp.helpers.TimerNoop(),
        loop=loop,
    )
    response.content = content
    content.on_eof(response._response_eof)
    if payload.body:
        content.feed_data(payload.body)
    content.feed_eof()
    if raw_cookie_headers:
        # New aiohttp versions parse the retained raw Set-Cookie values on
        # demand; aiohttp 3.9 stores a mutable ``cookies`` instance directly.
        if "cookies" in response.__dict__:
            cookies = response.cookies
            for value in raw_cookie_headers:
                cookies.load(value)
        else:
            cookies = response.cookies
        if not cookies:
            from http.cookies import SimpleCookie

            parsed = SimpleCookie()
            for value in raw_cookie_headers:
                parsed.load(value)
            response._cookies = parsed
            cookies = parsed
        update_from_headers = getattr(
            client._cookie_jar, "update_cookies_from_headers", None
        )
        if callable(update_from_headers):
            update_from_headers(raw_cookie_headers, response.url)
        else:
            client._cookie_jar.update_cookies(cookies, response.url)
    return response


def _urllib_request_body(value: object) -> bytes:
    if value is None:
        return b""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    raise TypeError(
        "streaming or file-like urllib.request request bodies are unsupported; "
        "data must be bytes-like"
    )


def _urllib_request_arguments(
    *args: Any, **kwargs: Any
) -> tuple[object, object, object, bool]:
    """Bind the public stdlib ``urlopen`` signature for this Python version."""

    parameters = [
        inspect.Parameter("url", inspect.Parameter.POSITIONAL_OR_KEYWORD),
        inspect.Parameter(
            "data", inspect.Parameter.POSITIONAL_OR_KEYWORD, default=None
        ),
        inspect.Parameter(
            "timeout",
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            default=socket._GLOBAL_DEFAULT_TIMEOUT,
        ),
    ]
    # Python 3.13 removed these long-deprecated TLS arguments. Supporting the
    # runtime's own signature keeps one source compatible with Python 3.10-3.14.
    if sys.version_info < (3, 13):
        parameters.extend(
            [
                inspect.Parameter(
                    "cafile", inspect.Parameter.KEYWORD_ONLY, default=None
                ),
                inspect.Parameter(
                    "capath", inspect.Parameter.KEYWORD_ONLY, default=None
                ),
                inspect.Parameter(
                    "cadefault", inspect.Parameter.KEYWORD_ONLY, default=False
                ),
            ]
        )
    parameters.append(
        inspect.Parameter("context", inspect.Parameter.KEYWORD_ONLY, default=None)
    )
    bound = inspect.Signature(parameters).bind(*args, **kwargs)
    bound.apply_defaults()
    values = bound.arguments
    use_global_opener = not bool(values.get("context"))
    if sys.version_info < (3, 13):
        use_global_opener = use_global_opener and not bool(
            values.get("cafile") or values.get("capath") or values.get("cadefault")
        )
    return values["url"], values["data"], values["timeout"], use_global_opener


def _urllib_request_timeout_seconds(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("urlopen timeout must be a number or None")
    return _timeout_seconds(value)


def _prepare_urllib_request(
    urllib_request: Any,
    url: object,
    data: object,
    *,
    timeout: object,
    use_global_opener: bool,
) -> Any:
    if isinstance(url, str):
        request = urllib_request.Request(url, data=data)
    elif isinstance(url, urllib_request.Request):
        request = url
        if data is not None:
            # OpenerDirector.open applies the explicit urlopen data argument to
            # an existing Request before method and header processing.
            request.data = data
    else:  # The mapped-origin lookup cannot normally reach this branch.
        raise TypeError("urlopen expected a URL string or Request object")

    request.timeout = timeout
    body = _urllib_request_body(request.data)
    if request.data is not None:
        if not request.has_header("Content-type"):
            request.add_unredirected_header(
                "Content-type", "application/x-www-form-urlencoded"
            )
        if not request.has_header("Content-length") and not request.has_header(
            "Transfer-encoding"
        ):
            request.add_unredirected_header("Content-length", str(len(body)))

    if not request.has_header("Host"):
        request.add_unredirected_header("Host", request.host)

    opener = (
        getattr(urllib_request, "_opener", None) if use_global_opener else None
    )
    addheaders = (
        getattr(opener, "addheaders", None)
        if opener is not None
        else [("User-agent", f"Python-urllib/{urllib_request.__version__}")]
    )
    for name, value in addheaders or ():
        normalized = str(name).capitalize()
        if not request.has_header(normalized):
            request.add_unredirected_header(normalized, value)
    return request


def _urllib_request_headers(request: Any) -> tuple[tuple[str, str], ...]:
    # Match AbstractHTTPHandler.do_open: unredirected fields win over normal
    # fields with the same canonical name, then urllib forces connection close.
    selected = dict(request.unredirected_hdrs)
    selected.update(
        (name, value)
        for name, value in request.headers.items()
        if name not in selected
    )
    selected["Connection"] = "close"
    return tuple(
        (
            _text_header(name.title(), "header name"),
            _text_header(value, "header value"),
        )
        for name, value in selected.items()
    )


def _urllib_request_response(
    payload: HTTPResponsePayload,
    *,
    url: str,
) -> Any:
    headers = Message()
    for name, value in payload.headers:
        headers[name] = value
    response = _urllib_response.addinfourl(
        io.BytesIO(payload.body),
        headers,
        url,
        payload.status_code,
    )
    # addinfourl supplies the standard buffered urlopen file API. These
    # metadata attributes normally come from http.client.HTTPResponse.
    response.reason = payload.reason
    # HTTP urlopen responses expose the reason phrase through both ``reason``
    # and the historical ``msg`` attribute; response headers remain available
    # through ``headers`` and ``info()``.
    response.msg = payload.reason
    response.version = 11
    return response


def _urllib3_url(pool: Any, url: str) -> str:
    if _origin_key_for_lookup(url) is not None:
        return url
    scheme = str(getattr(pool, "scheme", "http"))
    host = str(getattr(pool, "host", ""))
    port = getattr(pool, "port", None)
    authority = host
    if port is not None and not (
        (scheme == "http" and int(port) == 80)
        or (scheme == "https" and int(port) == 443)
    ):
        authority += f":{int(port)}"
    path = url if url.startswith("/") else f"/{url}"
    return f"{scheme}://{authority}{path}"


def _urllib3_response(
    urllib3: Any,
    payload: HTTPResponsePayload,
    *,
    method: str,
    url: str,
    preload: bool,
) -> Any:
    headers = urllib3._collections.HTTPHeaderDict()
    for name, value in payload.headers:
        headers.add(name, value)
    return urllib3.response.HTTPResponse(
        body=payload.body if preload else io.BytesIO(payload.body),
        headers=headers,
        status=payload.status_code,
        version=11,
        version_string="HTTP/1.1",
        reason=payload.reason,
        preload_content=bool(preload),
        request_method=method,
        request_url=url,
    )


@dataclass(slots=True)
class _ASGIItem:
    event_id: str
    message: MessageReceived
    accepted: threading.Event
    accept_succeeded: bool = False


class _ASGIRuntime:
    def __init__(
        self,
        bridge: _Bridge,
        app: object,
        *,
        loop: asyncio.AbstractEventLoop | None,
    ) -> None:
        self.bridge = bridge
        self.app = app
        self.capacity = bridge.owner.core._queue_limit  # type: ignore[union-attr]
        self.items: queue.Queue[_ASGIItem] = queue.Queue(self.capacity)
        self.stop = threading.Event()
        self.seen_lock = threading.Lock()
        self.seen: OrderedDict[bytes, bytes] = OrderedDict()
        self.loop = loop
        self.loop_thread: threading.Thread | None = None
        if self.loop is not None and self.loop.is_closed():
            raise RuntimeError("the ASGI event loop is closed")
        if self.loop is None:
            self.loop = asyncio.new_event_loop()

            def run_loop() -> None:
                assert self.loop is not None
                asyncio.set_event_loop(self.loop)
                self.loop.run_forever()

            self.loop_thread = threading.Thread(
                target=run_loop, name="cynapsa-asgi-loop", daemon=True
            )
            self.loop_thread.start()
        self.workers = tuple(
            threading.Thread(
                target=self._worker,
                name=f"cynapsa-asgi-worker-{index + 1}",
                daemon=True,
            )
            for index in range(min(8, self.capacity))
        )
        self.consumer = threading.Thread(
            target=self._consume, name="cynapsa-http-event-consumer", daemon=True
        )
        started_workers: list[threading.Thread] = []
        try:
            for worker in self.workers:
                worker.start()
                started_workers.append(worker)
            self.consumer.start()
        except BaseException:
            self.stop.set()
            for worker in started_workers:
                worker.join(1)
            if self.loop_thread is not None:
                assert self.loop is not None
                self.loop.call_soon_threadsafe(self.loop.stop)
                self.loop_thread.join(1)
                if not self.loop_thread.is_alive():
                    self.loop.close()
            raise

    def _consume(self) -> None:
        core = self.bridge.owner.core
        while not self.stop.is_set() and core is not None:
            try:
                raw = core._next_callback_event(diagnostic=False, timeout=0.005).payload
            except queue.Empty:
                continue
            except RuntimeError:
                return
            try:
                event = decode_event(raw, diagnostic=False, http_bridge=True)
            except (TypeError, ValueError):
                continue
            if not isinstance(event.payload, MessageReceived):
                continue
            identifier = hashlib.sha256(event.event_id.encode("utf-8")).digest()
            fingerprint = hashlib.sha256(raw).digest()
            with self.seen_lock:
                if identifier in self.seen:
                    continue
                self.seen[identifier] = fingerprint
                while len(self.seen) > MAX_QUEUE_CAPACITY:
                    self.seen.popitem(last=False)
            payload = event.payload.payload
            if not isinstance(payload, HTTPRequestPayload):
                continue
            if payload.inline_size > self.bridge.owner.payload_limit:
                continue
            item = _ASGIItem(event.event_id, event.payload, threading.Event())
            while not self.stop.is_set():
                try:
                    self.items.put(item, timeout=0.005)
                    break
                except queue.Full:
                    continue
            else:
                return
            # Queue ownership precedes flow-control acceptance, and workers
            # cannot invoke the app until this exact acceptance completes.
            try:
                completion = self.bridge.owner.execute(
                    "delivery.accept", {"event_id": event.event_id}
                )
                _session._require_result(
                    completion, "empty", "delivery.accept"
                )
                item.accept_succeeded = True
            except BaseException:
                pass
            finally:
                item.accepted.set()

    def _worker(self) -> None:
        while not self.stop.is_set() or not self.items.empty():
            try:
                item = self.items.get(timeout=0.01)
            except queue.Empty:
                continue
            try:
                item.accepted.wait()
                if not item.accept_succeeded:
                    continue
                assert self.loop is not None
                future = asyncio.run_coroutine_threadsafe(self._invoke(item), self.loop)
                future.result()
            except BaseException:
                pass
            finally:
                self.items.task_done()

    async def _invoke(self, item: _ASGIItem) -> None:
        request = item.message.payload
        assert isinstance(request, HTTPRequestPayload)
        response: HTTPResponsePayload
        try:
            response = await self._call_app(request)
            response.validate_payload_limit(self.bridge.owner.payload_limit)
        except BaseException as exc:
            _LOGGER.exception("Cynapsa ASGI application failed", exc_info=exc)
            response = HTTPResponsePayload.handler_error()
            try:
                response.validate_payload_limit(self.bridge.owner.payload_limit)
            except ValueError:
                return
        if item.message.mode != "rpc":
            return
        handle = item.message.request_handle
        if handle is None:
            return
        try:
            completion = await asyncio.to_thread(
                self.bridge.owner.execute,
                "message.reply",
                {"request_handle": handle, "payload": response._wire()},
            )
            _session._require_result(completion, "send", "message.reply")
            _session._send_result_model(completion, "message.reply")
        except BaseException:
            return

    async def _call_app(self, request: HTTPRequestPayload) -> HTTPResponsePayload:
        body_sent = False

        async def receive() -> dict[str, Any]:
            nonlocal body_sent
            if not body_sent:
                body_sent = True
                return {"type": "http.request", "body": request.body, "more_body": False}
            return {"type": "http.disconnect"}

        status: int | None = None
        headers: tuple[tuple[str, str], ...] = ()
        chunks: list[bytes] = []
        size = 0
        complete = False

        async def send(message: dict[str, Any]) -> None:
            nonlocal status, headers, size, complete
            if type(message) is not dict or "type" not in message:
                raise RuntimeError("invalid ASGI response event")
            if message["type"] == "http.response.start":
                if status is not None:
                    raise RuntimeError("duplicate ASGI response start")
                selected_status = message.get("status")
                if (
                    type(selected_status) is not int
                    or not 100 <= selected_status <= 599
                ):
                    raise RuntimeError("invalid ASGI response status")
                trailers = message.get("trailers", False)
                if type(trailers) is not bool or trailers:
                    raise RuntimeError("ASGI response trailers are unsupported")
                raw_headers = message.get("headers", [])
                if not isinstance(raw_headers, (list, tuple)):
                    raise RuntimeError("invalid ASGI response headers")
                selected_headers = tuple(
                    (
                        _text_header(name, "ASGI response header name"),
                        _text_header(value, "ASGI response header value"),
                    )
                    for name, value in raw_headers
                )
                # Validate metadata before accepting response body chunks.
                HTTPResponsePayload(selected_status, "", selected_headers, b"")
                status = selected_status
                headers = selected_headers
                return
            if message["type"] == "http.response.body":
                if status is None:
                    raise RuntimeError("ASGI response body preceded response start")
                if complete:
                    raise RuntimeError("ASGI response body followed completion")
                chunk = message.get("body", b"")
                if type(chunk) is not bytes:
                    raise RuntimeError("ASGI response body is not bytes")
                more_body = message.get("more_body", False)
                if type(more_body) is not bool:
                    raise RuntimeError("invalid ASGI more_body flag")
                size += len(chunk)
                if size > 256 << 10:
                    raise RuntimeError("ASGI response body exceeds the inline limit")
                chunks.append(chunk)
                complete = not more_body
                return
            if message["type"] == "http.response.trailers":
                raise RuntimeError("ASGI response trailers are unsupported")
            raise RuntimeError("unsupported ASGI response event")

        query = request.query.encode("utf-8")
        path = request.path.encode("utf-8")
        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.5"},
            "http_version": "1.1",
            "method": request.method,
            "scheme": "http",
            "path": request.path,
            "raw_path": path,
            "query_string": query,
            "root_path": "",
            "headers": [
                (name.encode("utf-8"), value.encode("utf-8"))
                for name, value in request.headers
            ],
            "client": None,
            "server": None,
            "state": {},
        }
        result = self.app(scope, receive, send)  # type: ignore[operator]
        if not inspect.isawaitable(result):
            raise TypeError("ASGI app did not return an awaitable")
        await result
        if status is None or not complete:
            raise RuntimeError("ASGI app did not complete a response")
        try:
            reason = HTTPStatus(status).phrase
        except ValueError:
            reason = ""
        return HTTPResponsePayload(status, reason, headers, b"".join(chunks))

    def close(self, timeout: float) -> None:
        self.stop.set()
        deadline = time.monotonic() + timeout
        self.consumer.join(max(0.0, deadline - time.monotonic()))
        while self.items.unfinished_tasks and time.monotonic() < deadline:
            time.sleep(0.005)
        for worker in self.workers:
            worker.join(max(0.0, deadline - time.monotonic()))
        if self.consumer.is_alive() or any(worker.is_alive() for worker in self.workers):
            raise TimeoutError("HTTP Bridge ASGI drain did not finish before its timeout")
        if self.loop_thread is not None:
            assert self.loop is not None
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.loop_thread.join(max(0.0, deadline - time.monotonic()))
            if self.loop_thread.is_alive():
                raise TimeoutError("HTTP Bridge ASGI loop did not stop before its timeout")
            self.loop.close()


class HttpBridgeHandle:
    """Synchronous lifecycle handle returned by :func:`login`."""

    __slots__ = ("_bridge",)

    def __init__(self, bridge: _Bridge) -> None:
        self._bridge = bridge

    @property
    def auth_info(self) -> _session.AztmAuthInfo:
        return self._bridge.identity.auth_info

    @property
    def profile_id(self) -> str | None:
        return self.auth_info.profile_id

    @property
    def installation_id(self) -> str | None:
        return self.auth_info.installation_id

    def status(self) -> _session.AztmStatus:
        try:
            return self._bridge.status_sync()
        except BaseException as exc:
            _session._raise_public(exc)

    def mappings(self) -> tuple[AddressMapping, ...]:
        try:
            return self._bridge.mappings_sync()
        except BaseException as exc:
            _session._raise_public(exc)

    def close(self) -> None:
        try:
            self._bridge.close_sync()
        except BaseException as exc:
            _session._raise_public(exc)

    def __enter__(self) -> HttpBridgeHandle:
        self._bridge.require_open()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if exc_type is None:
            self.close()
            return
        try:
            self.close()
        except BaseException:
            pass


class AsyncHttpBridgeHandle:
    """Asynchronous lifecycle handle returned by :func:`login_async`."""

    __slots__ = ("_bridge",)

    def __init__(self, bridge: _Bridge) -> None:
        self._bridge = bridge

    @property
    def auth_info(self) -> _session.AztmAuthInfo:
        return self._bridge.identity.auth_info

    @property
    def profile_id(self) -> str | None:
        return self.auth_info.profile_id

    @property
    def installation_id(self) -> str | None:
        return self.auth_info.installation_id

    async def status(self) -> _session.AztmStatus:
        try:
            return await _session._cancellable_offload(
                lambda cancelled: self._bridge.status_sync(cancel_event=cancelled)
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            _session._raise_public(exc)

    async def mappings(self) -> tuple[AddressMapping, ...]:
        try:
            return await _session._cancellable_offload(
                lambda cancelled: self._bridge.mappings_sync(cancel_event=cancelled)
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            _session._raise_public(exc)

    async def close(self) -> None:
        try:
            await _session._uncancellable_offload(self._bridge.close_sync)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            _session._raise_public(exc)

    async def __aenter__(self) -> AsyncHttpBridgeHandle:
        self._bridge.require_open()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if exc_type is None:
            await self.close()
            return
        try:
            await self.close()
        except BaseException:
            pass


def _rollback_setup(bridge: _Bridge, configured: list[str]) -> None:
    _PATCH_MANAGER.unregister(bridge)
    if bridge._asgi is not None:
        try:
            bridge._asgi.close(bridge.owner.command_timeout)
        except BaseException:
            pass
    for origin in reversed(configured):
        try:
            completion = bridge.owner.execute(
                "address.map.remove", {"virtual_origin": origin}
            )
            _session._require_result(completion, "empty", "address.map.remove")
        except BaseException:
            pass
    _session._cleanup_setup(bridge.owner)


def _setup(
    config: _session._ConnectConfig,
    credential: str,
    mappings: tuple[_Route, ...],
    app: object | None,
    *,
    asgi_loop: asyncio.AbstractEventLoop | None,
    cancel_event: threading.Event | None = None,
) -> _Bridge:
    owner: _session._LifecycleOwner | None = None
    configured: list[str] = []
    bridge: _Bridge | None = None
    try:
        owner, identity = _session._authenticate_sync(
            config,
            credential,
            _session._PERSONALITY_HTTP_BRIDGE,
            core_factory=_session._default_core_factory,
            cancel_event=cancel_event,
        )
        credential = ""
        bridge = _Bridge(owner, identity, mappings, asgi_loop=asgi_loop)
        for route in mappings:
            completion = owner.execute(
                "address.map.put",
                {"virtual_origin": route.spelling, "recipient": route.recipient},
                cancel_event=cancel_event,
            )
            _session._require_result(completion, "empty", "address.map.put")
            configured.append(route.spelling)
        _PATCH_MANAGER.register(bridge)
        if app is not None:
            bridge.attach_asgi(app)
        return bridge
    except BaseException:
        credential = ""
        if bridge is not None:
            _rollback_setup(bridge, configured)
        elif owner is not None:
            _session._cleanup_setup(owner)
        raise


def _validated_login(
    *,
    mesh_endpoint: object,
    username: object,
    password: object,
    enrollment_token: object,
    force_enroll: object,
    profile_id: object,
    mesh_id: object,
    address_map: object,
    command_timeout_ms: object,
    rpc_timeout_ms: object,
    queue_limit: object,
    payload_limit: object,
) -> tuple[_session._ConnectConfig, tuple[_Route, ...]]:
    # Authentication-mode selection and validation belong to Session. The
    # Bridge only carries the selected secret to the common authentication
    # seam and installs no mappings or hooks until that seam succeeds.
    config = _session._validate_config(
        mesh_endpoint=mesh_endpoint,
        username=username,
        password=password,
        enrollment_token=enrollment_token,
        force_enroll=force_enroll,
        profile_id=profile_id,
        mesh_id=mesh_id,
        command_timeout_ms=command_timeout_ms,
        rpc_timeout_ms=rpc_timeout_ms,
        queue_limit=queue_limit,
        payload_limit=payload_limit,
    )
    mappings = _address_map(address_map)
    _PATCH_MANAGER.ensure_available(tuple(route.key for route in mappings))
    return config, mappings


def login(
    *,
    mesh_id: str,
    address_map: Mapping[str, Mapping[str, str]],
    mesh_endpoint: str | None = None,
    username: str | None = None,
    password: str | None = None,
    enrollment_token: str | None = None,
    force_enroll: bool = False,
    profile_id: str = "default",
    asgi_app: object | None = None,
    command_timeout_ms: int = 0,
    rpc_timeout_ms: int = 0,
    queue_limit: int = _session._DEFAULT_QUEUE_LIMIT,
    payload_limit: int = _session._DEFAULT_PAYLOAD_LIMIT,
) -> HttpBridgeHandle:
    """Authenticate, configure mappings, and install selective sync/async hooks."""

    bridge: _Bridge | None = None
    failure: BaseException | None = None
    credential = ""
    try:
        config, mappings = _validated_login(
            mesh_endpoint=mesh_endpoint,
            username=username,
            password=password,
            enrollment_token=enrollment_token,
            force_enroll=force_enroll,
            profile_id=profile_id,
            mesh_id=mesh_id,
            address_map=address_map,
            command_timeout_ms=command_timeout_ms,
            rpc_timeout_ms=rpc_timeout_ms,
            queue_limit=queue_limit,
            payload_limit=payload_limit,
        )
        credential = (
            enrollment_token if config.auth_mode == "token" else password or ""
        )
        password = None
        enrollment_token = None
        bridge = _setup(config, credential, mappings, asgi_app, asgi_loop=None)
        credential = ""
    except BaseException as exc:
        failure = exc
    credential = ""
    password = None
    enrollment_token = None
    if failure is not None:
        projected = _session._public_exception(failure)
        failure = None
        _session._raise_public(projected)
    assert bridge is not None
    return HttpBridgeHandle(bridge)


async def login_async(
    *,
    mesh_id: str,
    address_map: Mapping[str, Mapping[str, str]],
    mesh_endpoint: str | None = None,
    username: str | None = None,
    password: str | None = None,
    enrollment_token: str | None = None,
    force_enroll: bool = False,
    profile_id: str = "default",
    asgi_app: object | None = None,
    command_timeout_ms: int = 0,
    rpc_timeout_ms: int = 0,
    queue_limit: int = _session._DEFAULT_QUEUE_LIMIT,
    payload_limit: int = _session._DEFAULT_PAYLOAD_LIMIT,
) -> AsyncHttpBridgeHandle:
    """Asynchronously authenticate and install the same selective HTTP hooks."""

    bridge: _Bridge | None = None
    failure: BaseException | None = None
    secret_box: list[str] = []
    try:
        config, mappings = _validated_login(
            mesh_endpoint=mesh_endpoint,
            username=username,
            password=password,
            enrollment_token=enrollment_token,
            force_enroll=force_enroll,
            profile_id=profile_id,
            mesh_id=mesh_id,
            address_map=address_map,
            command_timeout_ms=command_timeout_ms,
            rpc_timeout_ms=rpc_timeout_ms,
            queue_limit=queue_limit,
            payload_limit=payload_limit,
        )
        loop = asyncio.get_running_loop()
        secret_box.append(
            enrollment_token if config.auth_mode == "token" else password or ""
        )
        password = None
        enrollment_token = None

        def operation(cancelled: threading.Event) -> _Bridge:
            credential = secret_box.pop()
            try:
                return _setup(
                    config,
                    credential,
                    mappings,
                    asgi_app,
                    asgi_loop=loop,
                    cancel_event=cancelled,
                )
            finally:
                credential = ""

        bridge = await _session._cancellable_offload(
            operation, cleanup_result=lambda value: value.close_sync()
        )
    except BaseException as error:
        failure = error
    password = None
    enrollment_token = None
    secret_box.clear()
    if failure is not None:
        projected = _session._public_exception(failure)
        failure = None
        _session._raise_public(projected)
    assert bridge is not None
    return AsyncHttpBridgeHandle(bridge)


def hook_asgi(app: object) -> None:
    """Attach an ASGI app only when exactly one live bridge owns the process."""

    loop: asyncio.AbstractEventLoop | None
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    bridge = _PATCH_MANAGER.only_bridge()
    bridge.attach_asgi(app, loop=loop)


__all__ = [
    "AddressMapping",
    "AsyncHttpBridgeHandle",
    "HttpBridgeHandle",
    "hook_asgi",
    "login",
    "login_async",
]
