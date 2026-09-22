from __future__ import annotations

import asyncio
import importlib
import io
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI

import cynapsa._cli as cli


class DummyHandle:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.closed = False

    def close(self) -> None:
        self.closed = True
        self.events.append("close")


def _base_args(secret_env: str = "CYNAPSA_TEST_PASSWORD") -> list[str]:
    return [
        "run",
        "--mesh-endpoint",
        "mesh.example.test:5222",
        "--username",
        "agent@example.test",
        "--mesh-id",
        "mesh-one",
        "--password-env",
        secret_env,
    ]


def _installed_args(profile_id: str | None = None) -> list[str]:
    args = ["run", "--mesh-id", "mesh-one"]
    if profile_id is not None:
        args.extend(("--profile-id", profile_id))
    return args


def _patch_login(
    monkeypatch: pytest.MonkeyPatch,
    *,
    events: list[str] | None = None,
    calls: list[dict[str, Any]] | None = None,
) -> DummyHandle:
    selected_events = [] if events is None else events
    handle = DummyHandle(selected_events)

    def login(**kwargs: Any) -> DummyHandle:
        if calls is not None:
            calls.append(kwargs)
        selected_events.append("login")
        return handle

    monkeypatch.setattr(cli.cynapsa, "login", login)
    return handle


def test_run_script_shorthand_wraps_zero_code_change_script(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[str] = []
    _patch_login(monkeypatch, events=events)
    monkeypatch.setenv("CYNAPSA_TEST_PASSWORD", "secret")
    output = tmp_path / "events.json"
    script = tmp_path / "app.py"
    script.write_text(
        "import json\n"
        "import sys\n"
        f"json.loads({str(output)!r}.read_text()) if False else None\n"
        f"open({str(output)!r}, 'w').write(json.dumps(sys.argv))\n",
        encoding="utf-8",
    )

    status = cli.main([*_base_args(), str(script), "--", "one", "--two"])

    assert status == 0
    assert events == ["login", "close"]
    assert json.loads(output.read_text(encoding="utf-8")) == [
        str(script),
        "one",
        "--two",
    ]


def test_run_script_shorthand_accepts_cli_options_after_target_and_separates_args(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict[str, Any]] = []
    _patch_login(monkeypatch, calls=calls)
    monkeypatch.setenv("CYNAPSA_TEST_PASSWORD", "secret")
    output = tmp_path / "argv.json"
    script = tmp_path / "myagent.py"
    script.write_text(
        "import json\n"
        "import sys\n"
        f"open({str(output)!r}, 'w').write(json.dumps(sys.argv))\n",
        encoding="utf-8",
    )

    status = cli.main(
        [
            "run",
            str(script),
            "--username",
            "agent@mesh.net",
            "--mesh-endpoint",
            "mesh.net:5222",
            "--mesh-id",
            "mesh",
            "--password-env",
            "CYNAPSA_TEST_PASSWORD",
            "--",
            "--username",
            "target-user",
            "payload",
        ]
    )

    assert status == 0
    assert calls[0]["username"] == "agent@mesh.net"
    assert calls[0]["mesh_endpoint"] == "mesh.net:5222"
    assert calls[0]["mesh_id"] == "mesh"
    assert json.loads(output.read_text(encoding="utf-8")) == [
        str(script),
        "--username",
        "target-user",
        "payload",
    ]


def test_run_script_shorthand_requires_separator_before_target_args(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_login(monkeypatch)
    monkeypatch.setenv("CYNAPSA_TEST_PASSWORD", "secret")
    script = tmp_path / "myagent.py"
    script.write_text("", encoding="utf-8")

    with pytest.raises(SystemExit) as raised:
        cli.main(
            [
                "run",
                str(script),
                "target-arg",
                "--username",
                "agent@mesh.net",
                "--mesh-endpoint",
                "mesh.net:5222",
                "--mesh-id",
                "mesh",
                "--password-env",
                "CYNAPSA_TEST_PASSWORD",
            ]
        )

    assert raised.value.code == 2


def test_run_script_shorthand_allows_target_options_after_separator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_login(monkeypatch)
    monkeypatch.setenv("CYNAPSA_TEST_PASSWORD", "secret")
    output = tmp_path / "argv.json"
    script = tmp_path / "myagent.py"
    script.write_text(
        "import json\n"
        "import sys\n"
        f"open({str(output)!r}, 'w').write(json.dumps(sys.argv[1:]))\n",
        encoding="utf-8",
    )

    assert (
        cli.main(
            [
                *_base_args(),
                str(script),
                "--",
                "--reload",
                "--workers",
                "2",
            ]
        )
        == 0
    )

    assert json.loads(output.read_text(encoding="utf-8")) == [
        "--reload",
        "--workers",
        "2",
    ]


def test_run_python_script_preserves_system_exit_and_closes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[str] = []
    _patch_login(monkeypatch, events=events)
    monkeypatch.setenv("CYNAPSA_TEST_PASSWORD", "secret")
    script = tmp_path / "exit_app.py"
    script.write_text("raise SystemExit(7)\n", encoding="utf-8")

    with pytest.raises(SystemExit) as raised:
        cli.main([*_base_args(), "--", "python", str(script)])

    assert raised.value.code == 7
    assert events == ["login", "close"]


def test_run_closes_bridge_after_target_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[str] = []
    _patch_login(monkeypatch, events=events)
    monkeypatch.setenv("CYNAPSA_TEST_PASSWORD", "secret")
    script = tmp_path / "boom.py"
    script.write_text("raise RuntimeError('target failed')\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="target failed"):
        cli.main([*_base_args(), "--", "python", str(script)])

    assert events == ["login", "close"]


def test_run_closes_bridge_when_fastapi_hook_install_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[str] = []
    _patch_login(monkeypatch, events=events)
    monkeypatch.setenv("CYNAPSA_TEST_PASSWORD", "secret")
    script = tmp_path / "app.py"
    script.write_text("", encoding="utf-8")

    class BrokenHook:
        def __enter__(self) -> None:
            raise RuntimeError("hook failed")

        def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
            events.append("hook-exit")

    monkeypatch.setattr(cli, "_FastAPIHook", BrokenHook)

    with pytest.raises(RuntimeError, match="hook failed"):
        cli.main([*_base_args(), "--", "python", str(script)])

    assert events == ["login", "close"]


def test_run_module_sets_target_argv_and_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_login(monkeypatch)
    monkeypatch.setenv("CYNAPSA_TEST_PASSWORD", "secret")
    package = tmp_path / "pkg"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    output = tmp_path / "argv.json"
    (package / "__main__.py").write_text(
        "import json\n"
        "import sys\n"
        f"open({str(output)!r}, 'w').write(json.dumps(sys.argv[1:]))\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    assert cli.main([*_base_args(), "--", "python", "-m", "pkg", "x"]) == 0
    assert json.loads(output.read_text(encoding="utf-8")) == ["x"]


def test_python_console_script_executes_importable_entry_point_same_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[list[str]] = []

    class EntryPoint:
        name = "demo-cli"
        value = "demo:main"

        def load(self) -> Any:
            def main() -> int:
                observed.append(sys.argv[:])
                return 5

            return main

    class EntryPoints(list[Any]):
        def select(self, *, group: str, name: str) -> list[Any]:
            if group == "console_scripts" and name == "demo-cli":
                return [EntryPoint()]
            return []

    monkeypatch.setattr(cli.importlib.metadata, "entry_points", lambda: EntryPoints())
    target = cli._resolve_target(["demo-cli", "arg"])

    with pytest.raises(SystemExit) as raised:
        cli._execute_target(target)

    assert raised.value.code == 5
    assert observed == [["demo-cli", "arg"]]


def test_python_module_main_is_import_safe() -> None:
    module = importlib.import_module("cynapsa.__main__")

    assert module.main is cli.main


def test_login_flags_build_expected_config_and_password_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict[str, Any]] = []
    _patch_login(monkeypatch, calls=calls)
    password = tmp_path / "password.txt"
    password.write_text("from-file\n", encoding="utf-8")
    script = tmp_path / "app.py"
    script.write_text("", encoding="utf-8")

    assert (
        cli.main(
            [
                "run",
                "--mesh-endpoint",
                "mesh.example.test:5222",
                "--username",
                "agent@example.test",
                "--mesh-id",
                "mesh-one",
                "--password-file",
                str(password),
                "--map",
                "https://orders.example.test",
                "orders@example.test",
                "rpc",
                "--map",
                "https://audit.example.test",
                "audit@example.test",
                "msg",
                "--command-timeout-ms",
                "123",
                "--rpc-timeout-ms",
                "456",
                "--queue-limit",
                "7",
                "--payload-limit",
                "8192",
                "--",
                "python",
                str(script),
            ]
        )
        == 0
    )
    assert calls == [
        {
            "mesh_endpoint": "mesh.example.test:5222",
            "username": "agent@example.test",
            "password": "from-file",
            "mesh_id": "mesh-one",
            "address_map": {
                "https://orders.example.test": {
                    "recipient": "orders@example.test",
                    "mode": "rpc",
                },
                "https://audit.example.test": {
                    "recipient": "audit@example.test",
                    "mode": "msg",
                },
            },
            "command_timeout_ms": 123,
            "rpc_timeout_ms": 456,
            "queue_limit": 7,
            "payload_limit": 8192,
        }
    ]


@pytest.mark.parametrize("profile_id", [None, "blue-profile"])
def test_installed_profile_login_is_default_and_forwards_profile(
    profile_id: str | None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []
    _patch_login(monkeypatch, calls=calls)
    script = tmp_path / "app.py"
    script.write_text("", encoding="utf-8")

    assert cli.main([*_installed_args(profile_id), str(script)]) == 0

    assert calls == [
        {
            "mesh_id": "mesh-one",
            "profile_id": "default" if profile_id is None else profile_id,
            "address_map": {},
            "command_timeout_ms": 0,
            "rpc_timeout_ms": 0,
            "queue_limit": cli._session._DEFAULT_QUEUE_LIMIT,
            "payload_limit": cli._session._DEFAULT_PAYLOAD_LIMIT,
        }
    ]


@pytest.mark.parametrize("source", ["env", "file", "stdin", "prompt"])
def test_enrollment_token_sources_forward_without_argv_secret(
    source: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "cpsa_enrollment_secret"
    calls: list[dict[str, Any]] = []
    _patch_login(monkeypatch, calls=calls)
    script = tmp_path / "app.py"
    script.write_text(
        "import sys\n"
        f"assert {secret!r} not in sys.argv\n",
        encoding="utf-8",
    )
    source_args: list[str]
    if source == "env":
        monkeypatch.setenv("CYNAPSA_TEST_TOKEN", secret)
        source_args = ["--token-env", "CYNAPSA_TEST_TOKEN"]
    elif source == "file":
        token_file = tmp_path / "token.txt"
        token_file.write_text(f"{secret}\n", encoding="utf-8")
        source_args = ["--token-file", str(token_file)]
    elif source == "stdin":
        monkeypatch.setattr(sys, "stdin", io.StringIO(f"{secret}\n"))
        source_args = ["--token-stdin"]
    else:
        monkeypatch.setattr(cli.getpass, "getpass", lambda prompt: secret)
        source_args = ["--token-prompt"]

    assert (
        cli.main(
            [
                "run",
                str(script),
                "--mesh-id",
                "mesh-one",
                "--profile-id",
                "blue-profile",
                *source_args,
            ]
        )
        == 0
    )

    assert calls == [
        {
            "enrollment_token": secret,
            "profile_id": "blue-profile",
            "mesh_id": "mesh-one",
            "address_map": {},
            "command_timeout_ms": 0,
            "rpc_timeout_ms": 0,
            "queue_limit": cli._session._DEFAULT_QUEUE_LIMIT,
            "payload_limit": cli._session._DEFAULT_PAYLOAD_LIMIT,
        }
    ]


def test_enrollment_shorthand_options_parse_before_and_after_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict[str, Any]] = []
    _patch_login(monkeypatch, calls=calls)
    monkeypatch.setenv("CYNAPSA_TEST_TOKEN", "token-value")
    script = tmp_path / "agent.py"
    script.write_text("", encoding="utf-8")

    assert (
        cli.main(
            [
                "run",
                "--mesh-id",
                "mesh-one",
                str(script),
                "--token-env",
                "CYNAPSA_TEST_TOKEN",
                "--profile-id=custom",
            ]
        )
        == 0
    )
    assert calls[0]["profile_id"] == "custom"
    assert calls[0]["enrollment_token"] == "token-value"


@pytest.mark.parametrize(
    "auth_args",
    [
        ["--mesh-endpoint", "mesh.example.test:5222"],
        ["--username", "agent@example.test"],
        ["--password-env", "CYNAPSA_TEST_PASSWORD"],
        [
            "--mesh-endpoint",
            "mesh.example.test:5222",
            "--username",
            "agent@example.test",
        ],
        [
            "--mesh-endpoint",
            "mesh.example.test:5222",
            "--username",
            "agent@example.test",
            "--password-env",
            "CYNAPSA_TEST_PASSWORD",
            "--token-env",
            "CYNAPSA_TEST_TOKEN",
        ],
        ["--password-stdin", "--token-stdin"],
        [
            "--mesh-endpoint",
            "mesh.example.test:5222",
            "--username",
            "agent@example.test",
            "--password-env",
            "CYNAPSA_TEST_PASSWORD",
            "--profile-id",
            "legacy-cannot-use-profiles",
        ],
    ],
)
def test_partial_or_mixed_auth_fails_before_reading_secret(
    auth_args: list[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class UnreadableInput(io.StringIO):
        def read(self, *args: Any, **kwargs: Any) -> str:
            raise AssertionError("stdin must not be read")

    script = tmp_path / "app.py"
    script.write_text("", encoding="utf-8")
    monkeypatch.setattr(sys, "stdin", UnreadableInput())
    monkeypatch.setenv("CYNAPSA_TEST_PASSWORD", "password")
    monkeypatch.setenv("CYNAPSA_TEST_TOKEN", "token")
    login_called = False

    def login(**kwargs: Any) -> None:
        nonlocal login_called
        login_called = True

    monkeypatch.setattr(cli.cynapsa, "login", login)

    with pytest.raises(SystemExit) as raised:
        cli.main(
            ["run", "--mesh-id", "mesh-one", *auth_args, "--", "python", str(script)]
        )

    assert raised.value.code == 2
    assert login_called is False


@pytest.mark.parametrize("source", ["env", "file", "stdin", "prompt"])
def test_empty_enrollment_token_source_is_rejected(
    source: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    script = tmp_path / "app.py"
    script.write_text("", encoding="utf-8")
    if source == "env":
        monkeypatch.setenv("EMPTY_TOKEN", "")
        source_args = ["--token-env", "EMPTY_TOKEN"]
    elif source == "file":
        token_file = tmp_path / "empty-token"
        token_file.write_text("\n", encoding="utf-8")
        source_args = ["--token-file", str(token_file)]
    elif source == "stdin":
        monkeypatch.setattr(sys, "stdin", io.StringIO("\n"))
        source_args = ["--token-stdin"]
    else:
        monkeypatch.setattr(cli.getpass, "getpass", lambda prompt: "")
        source_args = ["--token-prompt"]

    with pytest.raises(SystemExit) as raised:
        cli.main([*_installed_args(), *source_args, "--", "python", str(script)])

    assert raised.value.code == 2


def test_unreadable_enrollment_token_source_is_sanitized(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    script = tmp_path / "app.py"
    script.write_text("", encoding="utf-8")
    missing = tmp_path / "private-token-name"

    with pytest.raises(SystemExit) as raised:
        cli.main(
            [
                *_installed_args(),
                "--token-file",
                str(missing),
                "--",
                "python",
                str(script),
            ]
        )

    assert raised.value.code == 2
    assert str(missing) not in capsys.readouterr().err


def test_raw_token_value_is_rejected_without_echoing_secret(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret = "raw-token-must-not-leak"
    script = tmp_path / "app.py"
    script.write_text("", encoding="utf-8")

    with pytest.raises(SystemExit) as raised:
        cli.main(
            [
                *_installed_args(),
                "--token",
                secret,
                "--",
                "python",
                str(script),
            ]
        )

    assert raised.value.code == 2
    assert secret not in capsys.readouterr().err


def test_cli_does_not_retain_token_in_traceback_on_login_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = "traceback-token-secret"
    monkeypatch.setenv("CYNAPSA_TEST_TOKEN", secret)
    script = tmp_path / "app.py"
    script.write_text("", encoding="utf-8")

    def login(**kwargs: Any) -> None:
        assert kwargs["enrollment_token"] == secret
        raise ValueError("login failed")

    monkeypatch.setattr(cli.cynapsa, "login", login)
    with pytest.raises(ValueError) as raised:
        cli.main(
            [
                *_installed_args(),
                "--token-env",
                "CYNAPSA_TEST_TOKEN",
                "--",
                "python",
                str(script),
            ]
        )

    traceback = raised.value.__traceback__
    while traceback is not None:
        if traceback.tb_frame.f_code.co_filename.endswith("cynapsa/_cli.py"):
            assert secret not in repr(dict(traceback.tb_frame.f_locals))
        traceback = traceback.tb_next


def test_duplicate_mapping_origin_fails_before_login(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    script = tmp_path / "app.py"
    script.write_text("", encoding="utf-8")
    login_called = False

    def login(**kwargs: Any) -> None:
        nonlocal login_called
        login_called = True

    monkeypatch.setattr(cli.cynapsa, "login", login)
    monkeypatch.setenv("CYNAPSA_TEST_PASSWORD", "secret")

    with pytest.raises(SystemExit) as raised:
        cli.main(
            [
                *_base_args(),
                "--map",
                "https://orders.example.test",
                "orders-a@example.test",
                "rpc",
                "--map",
                "https://orders.example.test",
                "orders-b@example.test",
                "msg",
                "--",
                "python",
                str(script),
            ]
        )

    assert raised.value.code == 2
    assert login_called is False


def test_password_stdin_and_parser_secret_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    calls: list[dict[str, Any]] = []
    _patch_login(monkeypatch, calls=calls)
    monkeypatch.setattr(sys, "stdin", io.StringIO("from-stdin\n"))
    script = tmp_path / "app.py"
    script.write_text("", encoding="utf-8")

    assert (
        cli.main(
            [
                "run",
                "--mesh-endpoint",
                "mesh.example.test:5222",
                "--username",
                "agent@example.test",
                "--mesh-id",
                "mesh-one",
                "--password-stdin",
                str(script),
            ]
        )
        == 0
    )
    assert calls[0]["password"] == "from-stdin"

    with pytest.raises(SystemExit) as raised:
        cli.main(
            [
                "run",
                "--mesh-endpoint",
                "mesh.example.test:5222",
                "--username",
                "agent@example.test",
                "--mesh-id",
                "mesh-one",
                "--password",
                "plain-secret",
                str(script),
            ]
        )
    captured = capsys.readouterr()
    assert raised.value.code == 2
    assert "plain-secret" not in captured.err


def test_cli_does_not_retain_password_in_traceback_on_login_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = "traceback-secret"
    monkeypatch.setenv("CYNAPSA_TEST_PASSWORD", secret)
    script = tmp_path / "app.py"
    script.write_text("", encoding="utf-8")

    def login(**kwargs: Any) -> None:
        assert kwargs["password"] == secret
        raise ValueError("login failed")

    monkeypatch.setattr(cli.cynapsa, "login", login)
    with pytest.raises(ValueError) as raised:
        cli.main([*_base_args(), "--", "python", str(script)])

    traceback = raised.value.__traceback__
    while traceback is not None:
        if traceback.tb_frame.f_code.co_filename.endswith("cynapsa/_cli.py"):
            assert secret not in repr(dict(traceback.tb_frame.f_locals))
        traceback = traceback.tb_next


@pytest.mark.asyncio
async def test_cli_fastapi_hook_attaches_local_factory_app_on_lifespan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attached: list[tuple[FastAPI, asyncio.AbstractEventLoop]] = []

    def hook_asgi(app: FastAPI) -> None:
        app.state.cynapsa_hooked = True
        attached.append((app, asyncio.get_running_loop()))

    monkeypatch.setattr(cli.cynapsa, "hook_asgi", hook_asgi)

    def create_app() -> FastAPI:
        app = FastAPI()
        app.state.factory_local = True
        return app

    app = create_app()
    messages = iter(
        [
            {"type": "lifespan.startup"},
            {"type": "lifespan.shutdown"},
        ]
    )

    async def receive() -> dict[str, str]:
        return next(messages)

    async def send(message: dict[str, Any]) -> None:
        del message

    with cli._FastAPIHook():
        await app({"type": "lifespan"}, receive, send)

    assert app.state.cynapsa_hooked is True
    assert attached == [(app, asyncio.get_running_loop())]


@pytest.mark.asyncio
async def test_cli_fastapi_hook_rejects_second_independent_lifespan_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli.cynapsa, "hook_asgi", lambda app: None)

    async def receive() -> dict[str, str]:
        return {"type": "lifespan.shutdown"}

    async def send(message: dict[str, Any]) -> None:
        del message

    with cli._FastAPIHook():
        await FastAPI()({"type": "lifespan"}, receive, send)
        with pytest.raises(RuntimeError, match="second independent"):
            await FastAPI()({"type": "lifespan"}, receive, send)


def test_cli_fastapi_hook_restores_transactionally_after_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_login(monkeypatch)
    monkeypatch.setenv("CYNAPSA_TEST_PASSWORD", "secret")
    original = FastAPI.__call__
    script = tmp_path / "boom.py"
    script.write_text(
        "from fastapi import FastAPI\n"
        "_app = FastAPI()\n"
        "raise RuntimeError('boom')\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="boom"):
        cli.main([*_base_args(), "--", "python", str(script)])

    assert FastAPI.__call__ is original


@pytest.mark.parametrize(
    "target",
    [
        ["uvicorn", "app:app", "--workers", "2"],
        ["python", "-m", "gunicorn", "app:app"],
        ["uvicorn", "app:app", "--reload"],
        ["uvicorn", "app:app", "--lifespan=off"],
        ["hypercorn", "app:app", "--worker-class", "trio"],
        ["hypercorn", "app:app", "-ktrio"],
        ["gunicorn", "app:app"],
        ["/bin/echo", "hello"],
    ],
)
def test_unsupported_same_process_targets_are_diagnosed(target: list[str]) -> None:
    with pytest.raises(cli._UsageError):
        cli._resolve_target(target)


def test_unsupported_server_scan_ignores_target_args_after_separator() -> None:
    cli._reject_unsupported_server_config(["uvicorn", "app:app", "--", "--reload"])


def test_uvicorn_web_concurrency_cannot_bypass_single_worker_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WEB_CONCURRENCY", "2")

    with pytest.raises(cli._UsageError, match="multi-worker"):
        cli._reject_unsupported_server_config(["uvicorn", "app:app"])

    # An explicit supported worker count overrides Uvicorn's environment
    # fallback, matching Uvicorn's own configuration precedence.
    cli._reject_unsupported_server_config(["uvicorn", "app:app", "--workers", "1"])


def test_allow_rules_are_explicit_and_translate_star_selectors() -> None:
    assert cli._allow_rules(
        [
            ["*", "*"],
            ["orders@example.test", "/orders"],
        ]
    ) == [
        {"action": "allow", "agent_id": "", "path": ""},
        {
            "action": "allow",
            "agent_id": "orders@example.test",
            "path": "/orders",
        },
    ]
    assert cli._allow_rules([]) == []


@pytest.mark.parametrize(
    "agent,path", [("", "/orders"), ("agent", ""), ("agent", "orders")]
)
def test_allow_rules_reject_ambiguous_or_invalid_selectors(
    agent: str, path: str
) -> None:
    with pytest.raises(cli._UsageError):
        cli._allow_rules([[agent, path]])


def test_run_installs_allow_policy_before_target_and_closes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[str] = []
    marker = tmp_path / "target-ran"
    script = tmp_path / "app.py"
    script.write_text(f"open({str(marker)!r}, 'w').write('yes')\n", encoding="utf-8")

    class Owner:
        def execute(self, name: str, args: dict[str, Any]) -> Any:
            assert not marker.exists()
            events.append(name)
            assert args == {
                "rules": [
                    {"action": "allow", "agent_id": "", "path": "/reverse"}
                ]
            }
            return SimpleNamespace(
                ok=True,
                error=None,
                result_type="policy",
                result={"rules": args["rules"], "allowed": True},
            )

    class Handle:
        _bridge = SimpleNamespace(owner=Owner())

        def close(self) -> None:
            events.append("close")

    monkeypatch.setattr(cli.cynapsa, "login", lambda **kwargs: Handle())
    monkeypatch.setenv("CYNAPSA_TEST_PASSWORD", "secret")

    assert (
        cli.main(
            [
                *_base_args(),
                "--allow",
                "*",
                "/reverse",
                "--",
                "python",
                str(script),
            ]
        )
        == 0
    )
    assert marker.read_text(encoding="utf-8") == "yes"
    assert events == ["policy.set", "close"]


def test_policy_failure_closes_without_running_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[str] = []
    marker = tmp_path / "target-ran"
    script = tmp_path / "app.py"
    script.write_text(f"open({str(marker)!r}, 'w').write('yes')\n", encoding="utf-8")

    class Owner:
        def execute(self, name: str, args: dict[str, Any]) -> Any:
            events.append(name)
            return SimpleNamespace(
                ok=False,
                error=object(),
                result_type="empty",
                result=None,
            )

    class Handle:
        _bridge = SimpleNamespace(owner=Owner())

        def close(self) -> None:
            events.append("close")

    monkeypatch.setattr(cli.cynapsa, "login", lambda **kwargs: Handle())
    monkeypatch.setenv("CYNAPSA_TEST_PASSWORD", "secret")

    with pytest.raises(cli._session.NativeError) as raised:
        cli.main([*_base_args(), "--allow", "*", "*", "--", "python", str(script)])

    assert raised.value.code == "core_error"
    assert not marker.exists()
    assert events == ["policy.set", "close"]


def test_invalid_allow_rule_fails_before_login(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    script = tmp_path / "app.py"
    script.write_text("", encoding="utf-8")
    login_called = False

    def login(**kwargs: Any) -> None:
        nonlocal login_called
        login_called = True

    monkeypatch.setattr(cli.cynapsa, "login", login)
    monkeypatch.setenv("CYNAPSA_TEST_PASSWORD", "secret")

    with pytest.raises(SystemExit) as raised:
        cli.main(
            [
                *_base_args(),
                "--allow",
                "orders@example.test",
                "relative-path",
                "--",
                "python",
                str(script),
            ]
        )

    assert raised.value.code == 2
    assert login_called is False
