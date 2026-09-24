"""Public exceptions raised by the native binding."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from cynapsa.http import CynapsaResponse


class RPCException(Exception):
    """An intentional, safe application error returned to an RPC caller."""

    def __init__(
        self,
        status_code: int,
        *,
        code: str,
        detail: str,
        details: Mapping[str, Any] | None = None,
        headers: tuple[tuple[str, str], ...] = (),
    ) -> None:
        if type(status_code) is not int or not 400 <= status_code <= 599:
            raise ValueError("status_code must be an integer in 400..599")
        if type(code) is not str or not code:
            raise TypeError("code must be nonempty text")
        if type(detail) is not str or not detail:
            raise TypeError("detail must be nonempty text")
        if details is not None and not isinstance(details, Mapping):
            raise TypeError("details must be a mapping or None")
        if not isinstance(headers, tuple):
            raise TypeError("headers must be an ordered tuple of name/value pairs")
        # Import lazily so the public exception module remains usable during
        # HTTP model initialization while still freezing the declaration at
        # construction time.
        from cynapsa.http import CynapsaApplicationError, _headers

        metadata = CynapsaApplicationError(code, detail, details or {})
        normalized_headers = _headers(headers)
        super().__init__(detail)
        self.status_code = status_code
        self.code = metadata.code
        self.detail = metadata.detail
        self.details = metadata.details
        self.headers = normalized_headers


class RemoteApplicationError(Exception):
    """Raised only when a caller explicitly checks an unsuccessful response."""

    def __init__(self, response: Any) -> None:
        self.response = response
        application_error = getattr(response, "error", None)
        detail = getattr(application_error, "detail", None)
        reason = getattr(response, "reason", "")
        status_code = getattr(response, "status_code", 0)
        message = detail or reason or f"remote application returned HTTP {status_code}"
        super().__init__(message)


class SdkSafetyTimeout(TimeoutError):
    """The Core did not publish an RPC terminal completion within the SDK guard."""


class NativeError(Exception):
    """A normalized failure reported by, or while calling, the native core."""

    _status: int
    _code: str
    _message: str
    _details: Mapping[str, Any]

    def __init__(
        self,
        status: int,
        code: str,
        message: str,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        if isinstance(status, bool) or not isinstance(status, int):
            raise TypeError("status must be an integer")
        if not isinstance(code, str) or not code:
            raise TypeError("code must be a nonempty string")
        if not isinstance(message, str) or not message:
            raise TypeError("message must be a nonempty string")
        if details is not None and not isinstance(details, Mapping):
            raise TypeError("details must be a mapping or None")
        super().__init__(message)
        self._status = status
        self._code = code
        self._message = message
        self._details = _freeze(dict(details or {}))

    @property
    def status(self) -> int:
        return self._status

    @property
    def code(self) -> str:
        return self._code

    @property
    def message(self) -> str:
        return self._message

    @property
    def details(self) -> Mapping[str, Any]:
        return self._details

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(status={self.status!r}, code={self.code!r}, "
            f"message={self.message!r}, details={self.details!r})"
        )


class RemoteNativeError(NativeError):
    """A remote application failure projected for a native RPC caller.

    The retained response is Cynapsa's dependency-free canonical model, not
    an HTTP-client-library object. Intermediaries may explicitly forward it.
    """

    _response: CynapsaResponse

    def __init__(self, status: int, response: CynapsaResponse) -> None:
        from cynapsa.http import CynapsaResponse

        if type(response) is not CynapsaResponse or response.status_code < 400:
            raise TypeError("response must be a canonical 4xx/5xx CynapsaResponse")
        if response.error is not None:
            code = response.error.code
            message = response.error.detail
            details = response.error.details
        elif response.status_code == 404:
            code, message, details = (
                "not_found",
                "The requested endpoint was not found",
                None,
            )
        else:
            code, message, details = (
                "remote_error",
                "The remote application returned an error",
                None,
            )
        super().__init__(status, code, message, details)
        self._response = response

    @property
    def response(self) -> CynapsaResponse:
        """The complete, immutable canonical response from the remote agent."""

        return self._response


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze(item) for item in value)
    return value
