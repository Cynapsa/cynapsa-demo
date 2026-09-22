"""Cynapsa native Session API."""

from ._inbound import AztmLocalDiagnostic
from .events import (
    AztmEvent,
    AztmEventError,
    ConnectivityStateChanged,
    CoreError,
    DiagnosticLog,
    MessageReceived,
    MessageStateChanged,
    PayloadTransferChanged,
    PeerReachability,
    PolicyRejected,
    QueueFull,
    RpcTimedOut,
    SessionStateChanged,
    decode_event,
)
from .messaging import AztmResponse, AztmSendResult, NativePayload
from .http import (
    CynapsaApplicationError,
    CynapsaRequest,
    CynapsaResponse,
    HTTPRequestPayload,
    HTTPResponsePayload,
)
from .exceptions import (
    NativeError,
    RemoteApplicationError,
    RPCException,
    SdkSafetyTimeout,
)
from ._http_bridge import (
    AddressMapping,
    AsyncHttpBridgeHandle,
    HttpBridgeHandle,
    hook_asgi,
    login,
    login_async,
)

from .session import (
    AsyncAztmSession,
    AztmAuthInfo,
    AztmCapabilities,
    AztmSession,
    AztmStatus,
    connect,
    connect_async,
)

__all__ = [
    "AsyncAztmSession",
    "AsyncHttpBridgeHandle",
    "AddressMapping",
    "AztmAuthInfo",
    "AztmCapabilities",
    "AztmEvent",
    "AztmEventError",
    "AztmLocalDiagnostic",
    "AztmSendResult",
    "AztmSession",
    "AztmStatus",
    "CynapsaApplicationError",
    "CynapsaRequest",
    "CynapsaResponse",
    "HttpBridgeHandle",
    "NativeError",
    "RemoteApplicationError",
    "RPCException",
    "ConnectivityStateChanged",
    "CoreError",
    "DiagnosticLog",
    "MessageReceived",
    "MessageStateChanged",
    "PayloadTransferChanged",
    "PeerReachability",
    "PolicyRejected",
    "QueueFull",
    "RpcTimedOut",
    "SessionStateChanged",
    "SdkSafetyTimeout",
    "connect",
    "connect_async",
    "decode_event",
    "hook_asgi",
    "login",
    "login_async",
]

__version__ = "0.1.0"

# Import-compatible names for applications crossing the contract boundary.
# They are intentionally absent from __all__ and documentation. In particular,
# the historical request classes now resolve to the canonical immutable model
# and therefore cannot expose request.reply().
AztmRequest = CynapsaRequest
AsyncAztmRequest = CynapsaRequest
