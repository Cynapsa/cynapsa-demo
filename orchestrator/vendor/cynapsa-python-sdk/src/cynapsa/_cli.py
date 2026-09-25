"""Command-line bootstrap for same-process HTTP Bridge execution."""

from __future__ import annotations

import argparse
import builtins
import functools
import getpass
import importlib.metadata
import os
import re
import runpy
import sys
import threading
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

import cynapsa
from cynapsa import session as _session


class _UsageError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class _AuthenticationSelection:
    mode: str
    source_kind: str | None = None
    source_value: str | None = None


@dataclass(frozen=True, slots=True)
class _ScriptTarget:
    path: str
    args: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _ModuleTarget:
    module: str
    args: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _ConsoleTarget:
    name: str
    args: tuple[str, ...]
    entry_point: importlib.metadata.EntryPoint


_Target = _ScriptTarget | _ModuleTarget | _ConsoleTarget


def _positive_text(value: str, field: str) -> str:
    if not value:
        raise _UsageError(f"{field} must be nonempty")
    return value


def _nonnegative_int(value: str) -> int:
    try:
        parsed = int(value, 10)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return parsed


def _positive_int(value: str) -> int:
    parsed = _nonnegative_int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def _add_run_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--mesh-endpoint")
    parser.add_argument("--username")
    parser.add_argument("--mesh-id", required=True)
    parser.add_argument("--profile-id", default="default")
    password = parser.add_mutually_exclusive_group()
    password.add_argument("--password-env", metavar="NAME")
    password.add_argument("--password-file", metavar="PATH")
    password.add_argument("--password-stdin", action="store_true")
    password.add_argument("--password-prompt", action="store_true")
    token = parser.add_mutually_exclusive_group()
    token.add_argument("--token-env", metavar="NAME")
    token.add_argument("--token-file", metavar="PATH")
    token.add_argument("--token-stdin", action="store_true")
    token.add_argument("--token-prompt", action="store_true")
    parser.add_argument("--force-enroll", action="store_true")
    parser.add_argument(
        "--map",
        action="append",
        nargs=3,
        metavar=("ORIGIN", "RECIPIENT", "MODE"),
        default=[],
        help="add an origin mapping; MODE must be rpc or msg",
    )
    parser.add_argument(
        "--allow",
        action="append",
        nargs=2,
        metavar=("AGENT", "PATH"),
        default=[],
        help=(
            "add an application-policy allow rule; use * as the wildcard "
            "for either selector"
        ),
    )
    parser.add_argument(
        "--command-timeout-ms",
        type=_nonnegative_int,
        default=0,
    )
    parser.add_argument("--rpc-timeout-ms", type=_nonnegative_int, default=0)
    parser.add_argument(
        "--queue-limit",
        type=_positive_int,
        default=_session._DEFAULT_QUEUE_LIMIT,
    )
    parser.add_argument(
        "--payload-limit",
        type=_positive_int,
        default=_session._DEFAULT_PAYLOAD_LIMIT,
    )


def _build_run_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cynapsa run",
        description=(
            "Authenticate with Cynapsa, install HTTP Bridge hooks, and run a "
            "Python script, module, or Python console script in this process."
        ),
    )
    _add_run_arguments(parser)
    parser.epilog = (
        "Canonical: cynapsa run [options] -- python app.py [args]. "
        "Shorthand: cynapsa run app.py [options] -- [args]."
    )
    return parser


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cynapsa")
    subcommands = parser.add_subparsers(dest="command", required=True)
    run = subcommands.add_parser(
        "run",
        help="authenticate and run a Python target in the same process",
        description=_build_run_parser().description,
    )
    _add_run_arguments(run)
    run.epilog = _build_run_parser().epilog
    return parser


def _selected_source(
    args: argparse.Namespace, prefix: str
) -> tuple[str, str | None] | None:
    sources = (
        ("env", getattr(args, f"{prefix}_env")),
        ("file", getattr(args, f"{prefix}_file")),
        ("stdin", True if getattr(args, f"{prefix}_stdin") else None),
        ("prompt", True if getattr(args, f"{prefix}_prompt") else None),
    )
    selected = [(kind, value) for kind, value in sources if value is not None]
    if not selected:
        return None
    if len(selected) != 1:
        # argparse normally enforces this. Keep the boundary deterministic for
        # directly constructed Namespaces and future parser changes.
        raise _UsageError(f"select exactly one {prefix} source")
    kind, value = selected[0]
    return kind, value if isinstance(value, str) else None


def _authentication_selection(args: argparse.Namespace) -> _AuthenticationSelection:
    password_source = _selected_source(args, "password")
    token_source = _selected_source(args, "token")
    legacy_selected = any(
        value is not None
        for value in (args.mesh_endpoint, args.username, password_source)
    )
    enrollment_selected = token_source is not None
    if args.force_enroll and not enrollment_selected:
        raise _UsageError("--force-enroll requires an enrollment token source")
    if legacy_selected and enrollment_selected:
        raise _UsageError(
            "enrollment token sources cannot be combined with legacy credentials"
        )
    if legacy_selected:
        if (
            args.mesh_endpoint is None
            or args.username is None
            or password_source is None
        ):
            raise _UsageError(
                "--mesh-endpoint, --username, and exactly one password source "
                "must be supplied together"
            )
        if args.profile_id != "default":
            raise _UsageError(
                "--profile-id is unavailable with legacy credentials"
            )
        kind, value = password_source
        return _AuthenticationSelection("legacy", kind, value)
    if enrollment_selected:
        assert token_source is not None
        kind, value = token_source
        return _AuthenticationSelection("token", kind, value)
    return _AuthenticationSelection("installation")


def _secret_from_source(selection: _AuthenticationSelection) -> str:
    kind = selection.source_kind
    secret_name = "password" if selection.mode == "legacy" else "enrollment token"
    if kind == "env":
        name = _positive_text(
            selection.source_value or "", f"{secret_name} environment variable"
        )
        try:
            secret = os.environ[name]
        except KeyError as exc:
            raise _UsageError(
                f"{secret_name} environment variable is not set"
            ) from exc
    elif kind == "file":
        path = Path(
            _positive_text(selection.source_value or "", f"{secret_name} file")
        )
        try:
            secret = path.read_text(encoding="utf-8").rstrip("\r\n")
        except (OSError, UnicodeError) as exc:
            raise _UsageError(f"{secret_name} file could not be read") from exc
    elif kind == "stdin":
        secret = sys.stdin.read().rstrip("\r\n")
    elif kind == "prompt":
        prompt = (
            "Cynapsa password: "
            if selection.mode == "legacy"
            else "Cynapsa enrollment token: "
        )
        secret = getpass.getpass(prompt)
    else:
        raise _UsageError(f"{secret_name} source is missing")
    if not secret:
        raise _UsageError(f"{secret_name} must be nonempty")
    return secret


def _address_map(items: Sequence[Sequence[str]]) -> dict[str, dict[str, str]]:
    output: dict[str, dict[str, str]] = {}
    for origin, recipient, mode in items:
        if mode not in {"rpc", "msg"}:
            raise _UsageError("mapping MODE must be rpc or msg")
        if origin in output:
            raise _UsageError("mapping ORIGIN must be unique")
        output[origin] = {"recipient": recipient, "mode": mode}
    return output


def _is_python_command(value: str) -> bool:
    name = Path(value).name.lower()
    if name.endswith(".exe"):
        name = name[:-4]
    if name in {"python", "python3"} or re.fullmatch(r"python3\.\d+", name):
        return True
    try:
        return Path(value).resolve() == Path(sys.executable).resolve()
    except OSError:
        return False


def _strip_separator(raw: Sequence[str]) -> tuple[str, ...]:
    target = tuple(raw)
    if target and target[0] == "--":
        target = target[1:]
    if not target:
        raise _UsageError("run requires a Python target after the CLI flags")
    return target


_OPTION_ARITY = {
    "--mesh-endpoint": 1,
    "--username": 1,
    "--mesh-id": 1,
    "--profile-id": 1,
    "--password-env": 1,
    "--password-file": 1,
    "--password-stdin": 0,
    "--password-prompt": 0,
    "--token-env": 1,
    "--token-file": 1,
    "--token-stdin": 0,
    "--token-prompt": 0,
    "--force-enroll": 0,
    "--map": 3,
    "--allow": 2,
    "--command-timeout-ms": 1,
    "--rpc-timeout-ms": 1,
    "--queue-limit": 1,
    "--payload-limit": 1,
}


def _allow_rules(items: Sequence[Sequence[str]]) -> list[dict[str, str]]:
    rules: list[dict[str, str]] = []
    for agent, path in items:
        if not agent:
            raise _UsageError("allow AGENT must be nonempty; use * as the wildcard")
        if not path:
            raise _UsageError("allow PATH must be nonempty; use * as the wildcard")
        if path != "*" and (
            not path.startswith("/") or "?" in path or "#" in path
        ):
            raise _UsageError("allow PATH must be an absolute path or *")
        rules.append(
            {
                "action": "allow",
                "agent_id": "" if agent == "*" else agent,
                "path": "" if path == "*" else path,
            }
        )
    return rules


def _install_allow_policy(handle: Any, rules: Sequence[dict[str, str]]) -> None:
    if not rules:
        return
    completion = handle._bridge.owner.execute(
        "policy.set", {"rules": list(rules)}
    )
    _session._require_result(completion, "policy", "policy.set")


def _split_on_separator(
    raw: Sequence[str],
) -> tuple[tuple[str, ...], tuple[str, ...], bool]:
    try:
        index = raw.index("--")
    except ValueError:
        return tuple(raw), (), False
    return tuple(raw[:index]), tuple(raw[index + 1 :]), True


def _shorthand_script_index(tokens: Sequence[str]) -> int | None:
    candidates: list[int] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token.startswith("--"):
            option = token.split("=", 1)[0]
            arity = _OPTION_ARITY.get(option)
            if arity is None:
                index += 1
                continue
            consumed_values = 1 if "=" in token and arity > 0 else 0
            index += 1 + max(0, arity - consumed_values)
            continue
        if Path(token).suffix == ".py":
            candidates.append(index)
        index += 1
    if len(candidates) > 1:
        raise _UsageError("shorthand accepts exactly one .py target before --")
    return candidates[0] if candidates else None


def _parse_run_args(
    parser: argparse.ArgumentParser, raw: Sequence[str]
) -> tuple[argparse.Namespace, tuple[str, ...]]:
    before, after, has_separator = _split_on_separator(raw)
    for token in before:
        if token in {"--password", "--token"} or token.startswith(
            ("--password=", "--token=")
        ):
            raise _UsageError(
                "raw secrets are unsupported; select an environment, file, "
                "stdin, or prompt source"
            )
    if "-h" in before or "--help" in before:
        parser.parse_args(list(before))
    script_index = _shorthand_script_index(before)
    if script_index is not None:
        script = before[script_index]
        cli_tokens = [*before[:script_index], *before[script_index + 1 :]]
        target = (script, *after)
    else:
        if not has_separator:
            raise _UsageError(
                "canonical wrapped commands require -- before the Python target"
            )
        cli_tokens = list(before)
        target = after
    if not target:
        raise _UsageError("run requires a Python target")
    return parser.parse_args(cli_tokens), target


def _resolve_entry_point(name: str) -> importlib.metadata.EntryPoint | None:
    entry_points = importlib.metadata.entry_points()
    if hasattr(entry_points, "select"):
        selected = entry_points.select(group="console_scripts", name=name)
    else:
        selected = [
            item
            for item in entry_points.get("console_scripts", ())  # type: ignore[union-attr]
            if item.name == name
        ]
    matches = list(selected)
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise _UsageError(f"console script {name!r} is ambiguous")
    return None


def _resolve_target(raw: Sequence[str]) -> _Target:
    target = _strip_separator(raw)
    _reject_unsupported_server_config(target)
    first = target[0]
    if _is_python_command(first):
        if len(target) < 2:
            raise _UsageError("python target requires a script path or -m module")
        if target[1] == "-m":
            if len(target) < 3 or target[2].startswith("-"):
                raise _UsageError("python -m requires a module name")
            return _ModuleTarget(target[2], tuple(target[3:]))
        if target[1].startswith("-"):
            raise _UsageError("only python script.py and python -m module are supported")
        script = Path(target[1])
        if script.suffix != ".py":
            raise _UsageError("python target script must be a .py file")
        if not script.is_file():
            raise _UsageError("target script does not exist")
        return _ScriptTarget(str(script), tuple(target[2:]))

    script = Path(first)
    if script.suffix == ".py" and script.is_file():
        return _ScriptTarget(str(script), tuple(target[1:]))
    if os.sep in first or (os.altsep is not None and os.altsep in first):
        raise _UsageError("native executables cannot be wrapped in-process")
    entry_point = _resolve_entry_point(first)
    if entry_point is None:
        raise _UsageError(
            f"{first!r} is not a Python script, python -m target, or installed "
            "Python console script"
        )
    if ":" not in entry_point.value:
        raise _UsageError(f"console script {first!r} is not an importable Python callable")
    return _ConsoleTarget(first, tuple(target[1:]), entry_point)


def _option_value(args: Sequence[str], names: set[str]) -> str | None:
    for index, item in enumerate(args):
        for name in names:
            prefix = f"{name}="
            if item.startswith(prefix):
                return item[len(prefix) :]
            if name in {"-w", "-k"} and item.startswith(name) and len(item) > len(name):
                return item[len(name) :]
            if item == name and index + 1 < len(args):
                return args[index + 1]
    return None


def _has_flag(args: Sequence[str], names: set[str]) -> bool:
    return any(
        item in names or any(item.startswith(f"{name}=") for name in names)
        for item in args
    )


def _server_command(args: Sequence[str]) -> str | None:
    command = Path(args[0]).name.lower()
    if _is_python_command(args[0]) and len(args) >= 3 and args[1] == "-m":
        command = args[2].split(".", 1)[0].lower()
    if command in {"gunicorn", "hypercorn", "uvicorn"}:
        return command
    return None


def _server_option_args(args: Sequence[str]) -> tuple[str, ...]:
    try:
        separator = args.index("--")
    except ValueError:
        return tuple(args)
    return tuple(args[:separator])


def _reject_unsupported_server_config(args: Sequence[str]) -> None:
    command = _server_command(args)
    if command is None:
        return
    option_args = _server_option_args(args)
    if command == "gunicorn":
        raise _UsageError("gunicorn pre-fork execution is unsupported by cynapsa run")
    if _has_flag(option_args, {"--reload"}):
        raise _UsageError("reload execution is unsupported by cynapsa run")

    workers = _option_value(option_args, {"--workers", "-w"})
    if workers is None and command == "uvicorn":
        workers = os.environ.get("WEB_CONCURRENCY")
    if workers is not None:
        try:
            count = int(workers, 10)
        except ValueError as exc:
            raise _UsageError("worker count must be an integer") from exc
        if count != 1:
            raise _UsageError("multi-worker execution is unsupported by cynapsa run")

    lifespan = _option_value(option_args, {"--lifespan"})
    if lifespan is not None and lifespan.lower() == "off":
        raise _UsageError("lifespan-off execution is unsupported by cynapsa run")

    loop = _option_value(option_args, {"--loop"})
    worker_class = _option_value(option_args, {"--worker-class", "-k"})
    if (
        (loop is not None and loop.lower() == "trio")
        or (worker_class is not None and worker_class.lower() == "trio")
        or _has_flag(option_args, {"--trio"})
    ):
        raise _UsageError("Trio execution is unsupported by cynapsa run")


class _FastAPIHook:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._root: object | None = None
        self._original_import: object | None = None
        self._patches: list[tuple[type[Any], object, object]] = []

    def __enter__(self) -> _FastAPIHook:
        self._install_import_hook()
        try:
            self._patch_loaded_fastapi()
        except BaseException:
            self.restore()
            raise
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.restore()

    def _install_import_hook(self) -> None:
        original = builtins.__import__
        self._original_import = original

        @functools.wraps(original)
        def cynapsa_import(
            name: str,
            globals: dict[str, Any] | None = None,
            locals: dict[str, Any] | None = None,
            fromlist: tuple[str, ...] = (),
            level: int = 0,
        ) -> ModuleType:
            module = original(name, globals, locals, fromlist, level)  # type: ignore[misc]
            if level == 0 and (name == "fastapi" or name.startswith("fastapi.")):
                self._patch_loaded_fastapi()
            return module

        builtins.__import__ = cynapsa_import

    def _patch_loaded_fastapi(self) -> None:
        module = sys.modules.get("fastapi.applications")
        if module is None:
            return
        fastapi_class = getattr(module, "FastAPI", None)
        if not isinstance(fastapi_class, type):
            return
        current = getattr(fastapi_class, "__call__", None)
        for patched_class, _original, replacement in self._patches:
            if patched_class is fastapi_class and current is replacement:
                return
        if getattr(current, "__cynapsa_cli_fastapi_hook__", False):
            return
        original = current

        @functools.wraps(original)
        async def cynapsa_fastapi_call(
            app: object,
            scope: dict[str, Any],
            receive: Any,
            send: Any,
        ) -> Any:
            if isinstance(scope, dict) and scope.get("type") == "lifespan":
                self._attach(app)
            result = original(app, scope, receive, send)
            if hasattr(result, "__await__"):
                return await result
            return result

        cynapsa_fastapi_call.__cynapsa_cli_fastapi_hook__ = True  # type: ignore[attr-defined]
        setattr(fastapi_class, "__call__", cynapsa_fastapi_call)
        self._patches.append((fastapi_class, original, cynapsa_fastapi_call))

    def _attach(self, app: object) -> None:
        with self._lock:
            if self._root is app:
                return
            if self._root is not None:
                raise RuntimeError(
                    "cynapsa run detected a second independent FastAPI lifespan root"
                )
            cynapsa.hook_asgi(app)
            self._root = app

    def restore(self) -> None:
        while self._patches:
            fastapi_class, original, replacement = self._patches.pop()
            if getattr(fastapi_class, "__call__", None) is replacement:
                setattr(fastapi_class, "__call__", original)
        if (
            self._original_import is not None
            and builtins.__import__ is not self._original_import
        ):
            current = builtins.__import__
            if getattr(current, "__wrapped__", None) is self._original_import:
                builtins.__import__ = self._original_import  # type: ignore[assignment]
        self._original_import = None


def _with_argv(argv: list[str], operation: Any) -> Any:
    previous = sys.argv
    sys.argv = argv
    try:
        return operation()
    finally:
        sys.argv = previous


def _with_sys_path_head(head: str, operation: Any) -> Any:
    previous = list(sys.path)
    sys.path.insert(0, head)
    try:
        return operation()
    finally:
        sys.path[:] = previous


def _execute_target(target: _Target) -> int:
    if isinstance(target, _ScriptTarget):
        path = Path(target.path)
        if not path.is_file():
            raise _UsageError("target script does not exist")
        argv = [str(path), *target.args]

        def operation() -> Any:
            return runpy.run_path(str(path), run_name="__main__")

        script_dir = str(path.resolve().parent)
        _with_sys_path_head(script_dir, lambda: _with_argv(argv, operation))
        return 0

    if isinstance(target, _ModuleTarget):
        argv = [target.module, *target.args]

        def operation() -> Any:
            return runpy.run_module(target.module, run_name="__main__", alter_sys=True)

        _with_sys_path_head(os.getcwd(), lambda: _with_argv(argv, operation))
        return 0

    argv = [target.name, *target.args]
    entry_point = target.entry_point

    def operation() -> Any:
        return entry_point.load()()

    result = _with_argv(argv, operation)
    if result is None:
        return 0
    raise SystemExit(result)


def _run(args: argparse.Namespace, target_args: Sequence[str]) -> int:
    target = _resolve_target(target_args)
    mappings = _address_map(args.map)
    allow_rules = _allow_rules(args.allow)
    selection = _authentication_selection(args)
    mesh_id = _positive_text(args.mesh_id, "mesh_id")
    profile_id = _positive_text(args.profile_id, "profile_id")
    common = {
        "mesh_id": mesh_id,
        "address_map": mappings,
        "command_timeout_ms": args.command_timeout_ms,
        "rpc_timeout_ms": args.rpc_timeout_ms,
        "queue_limit": args.queue_limit,
        "payload_limit": args.payload_limit,
    }
    if selection.mode == "installation":
        handle = cynapsa.login(profile_id=profile_id, **common)
    else:
        secret_box = [_secret_from_source(selection)]
        try:
            if selection.mode == "token":
                handle = cynapsa.login(
                    enrollment_token=secret_box.pop(),
                    profile_id=profile_id,
                    **({"force_enroll": True} if args.force_enroll else {}),
                    **common,
                )
            else:
                handle = cynapsa.login(
                    mesh_endpoint=_positive_text(args.mesh_endpoint, "mesh_endpoint"),
                    username=_positive_text(args.username, "username"),
                    password=secret_box.pop(),
                    **common,
                )
        finally:
            secret_box.clear()
    failed = True
    try:
        _install_allow_policy(handle, allow_rules)
        with _FastAPIHook():
            result = _execute_target(target)
        failed = False
        return result
    finally:
        if failed:
            try:
                handle.close()
            except BaseException:
                pass
        else:
            handle.close()


def main(argv: Sequence[str] | None = None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)
    parser = _build_parser()
    if raw[:1] == ["run"]:
        run_parser = _build_run_parser()
        try:
            args, target = _parse_run_args(run_parser, raw[1:])
            return _run(args, target)
        except _UsageError as exc:
            run_parser.exit(2, f"cynapsa: error: {exc}\n")
    try:
        parser.parse_args(raw)
    except _UsageError as exc:
        parser.exit(2, f"cynapsa: error: {exc}\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
