"""Native Cynapsa core library discovery and loading."""

from __future__ import annotations

import ctypes
import ctypes.util
import os
import sys
from pathlib import Path
from typing import Any

from cynapsa.exceptions import NativeError

from .abi import configure_library, validate_abi_version

ENV_LIBRARY = "CYNAPSA_CORE_LIBRARY"


def platform_library_filenames(platform: str | None = None) -> tuple[str, ...]:
    platform = sys.platform if platform is None else platform
    if platform == "win32":
        return ("cynapsacore.dll", "libcynapsacore.dll")
    if platform == "darwin":
        return ("libcynapsacore.dylib",)
    return ("libcynapsacore.so",)


def _package_candidates() -> list[Path]:
    package_dir = Path(__file__).resolve().parent.parent
    return [package_dir / name for name in platform_library_filenames()]


def discover_library(explicit_path: str | os.PathLike[str] | None = None) -> str:
    """Resolve a library according to the documented precedence order."""

    if explicit_path is not None:
        path = Path(explicit_path).expanduser()
        if not path.is_file():
            raise NativeError(
                1,
                "native_library_not_found",
                f"The explicit native core library does not exist: {path}",
                {"path": str(path), "source": "explicit"},
            )
        return str(path.resolve())

    configured = os.environ.get(ENV_LIBRARY)
    if configured:
        path = Path(configured).expanduser()
        if not path.is_file():
            raise NativeError(
                1,
                "native_library_not_found",
                f"{ENV_LIBRARY} points to a missing native core library: {path}",
                {"path": str(path), "source": ENV_LIBRARY},
            )
        return str(path.resolve())

    package_candidates = _package_candidates()
    for candidate in package_candidates:
        if candidate.is_file():
            return str(candidate)

    system_name = ctypes.util.find_library("cynapsacore")
    if system_name:
        return system_name

    searched = [str(path) for path in package_candidates]
    raise NativeError(
        1,
        "native_library_not_found",
        (
            "Could not find the Cynapsa native core library. Pass library_path, "
            f"set {ENV_LIBRARY}, place a platform library next to the cynapsa "
            "package, or install it in the system library path."
        ),
        {"searched": searched, "platform_filenames": platform_library_filenames()},
    )


def load_library(
    explicit_path: str | os.PathLike[str] | None = None,
) -> Any:
    """Discover, load, declare, and ABI-check the native core library."""

    resolved = discover_library(explicit_path)
    try:
        library = ctypes.CDLL(resolved)
    except OSError as exc:
        raise NativeError(
            1,
            "native_library_load_failed",
            f"Failed to load the Cynapsa native core library at {resolved}: {exc}",
            {"path": resolved, "reason": str(exc)},
        ) from exc

    configure_library(library)

    # Imported lazily so core.py can offer NativeCore.create() without a module
    # import cycle while ABI failures still use the same owned-buffer decoder.
    from .core import decode_native_error

    validate_abi_version(
        library,
        decode_error=lambda status, descriptor: decode_native_error(
            library, status, descriptor
        ),
    )
    return library

