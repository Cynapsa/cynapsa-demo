#!/usr/bin/env python3
"""Provision the six-agent SDK E2E authentication fixture.

This controller is intentionally standalone so its short-lived container needs
only the Python standard library. Secrets stay in memory except for the six
bounded enrollment tokens written to the private agent input directories.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import ssl
import stat
import uuid
from pathlib import Path
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


AUTH0_URL = "https://auth0.cynapsa.test:8443"
MANAGEMENT_URL = "https://management.cynapsa.test:8443"
AGENT_UID = 10001
AGENT_GID = 10001
MAX_RESPONSE_BYTES = 1_048_576
AGENT_LABELS = (
    "native-server",
    "monkey-server",
    "native-sync-client",
    "native-async-client",
    "monkey-sync-client",
    "monkey-async-client",
)
MESH_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,255}$")
AGENT_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,255}@[A-Za-z0-9.-]{1,255}$")
SAFE_ENV_RE = re.compile(r"^[A-Za-z0-9_./:@-]+$")
TOKEN_RE = re.compile(
    r"^cpsa_e1\.([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12})\.([A-Za-z0-9_-]{43})$"
)


class JSONAPI(Protocol):
    def json(
        self,
        method: str,
        url: str,
        body: dict[str, Any] | None = None,
        *,
        bearer: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any: ...


class API:
    """Small JSON-over-HTTPS client that never includes response bodies in errors."""

    def __init__(self, ca_file: Path, timeout_seconds: float = 15.0):
        self._context = ssl.create_default_context(cafile=str(ca_file))
        self._timeout_seconds = timeout_seconds

    def json(
        self,
        method: str,
        url: str,
        body: dict[str, Any] | None = None,
        *,
        bearer: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        request_headers = {
            "Accept": "application/json",
            "User-Agent": "cynapsa-sdk-e2e-auth-controller/1",
        }
        data = None
        if body is not None:
            data = json.dumps(body, separators=(",", ":")).encode("utf-8")
            request_headers["Content-Type"] = "application/json"
        if bearer is not None:
            request_headers["Authorization"] = f"Bearer {bearer}"
        request_headers.update(headers or {})
        request = Request(url, data=data, headers=request_headers, method=method)
        try:
            with urlopen(request, context=self._context, timeout=self._timeout_seconds) as response:
                raw = response.read(MAX_RESPONSE_BYTES + 1)
                if len(raw) > MAX_RESPONSE_BYTES:
                    raise RuntimeError(f"oversized response: {method} {url}")
                if not raw:
                    return None
                try:
                    return json.loads(raw)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    raise RuntimeError(f"invalid JSON response: {method} {url}") from None
        except HTTPError as error:
            raise RuntimeError(f"request failed: {method} {url} status={error.code}") from None
        except (URLError, TimeoutError) as error:
            raise RuntimeError(
                f"request unavailable: {method} {url}: {type(error).__name__}"
            ) from None


def require_object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f"{label} is not an object")
    return value


def require_string(value: Any, label: str, *, maximum: int = 16_384) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise RuntimeError(f"{label} is not a valid string")
    return value


def canonical_uuid(value: Any, label: str) -> str:
    try:
        parsed = uuid.UUID(str(value))
    except (ValueError, TypeError, AttributeError):
        raise RuntimeError(f"{label} is not a canonical UUID") from None
    canonical = str(parsed)
    if value != canonical:
        raise RuntimeError(f"{label} is not a canonical UUID")
    return canonical


def base_url(value: str, label: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        raise RuntimeError(
            f"{label} must be an HTTPS origin without credentials, path, query, or fragment"
        )
    try:
        port = parsed.port
    except ValueError:
        raise RuntimeError(f"{label} has an invalid port") from None
    authority = parsed.hostname if port is None else f"{parsed.hostname}:{port}"
    return f"https://{authority}"


def enrollment_token(value: Any, grant_id: str, label: str) -> str:
    token = require_string(value, label, maximum=128)
    match = TOKEN_RE.fullmatch(token)
    if match is None or match.group(1) != grant_id:
        raise RuntimeError(f"{label} has an invalid canonical format or grant id")
    return token


def private_text(path: Path) -> str:
    try:
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
            raise RuntimeError(f"required private input is not a regular file: {path.name}")
        value = path.read_text(encoding="utf-8").strip()
    except OSError as error:
        raise RuntimeError(f"cannot read required private input: {path.name}") from error
    return require_string(value, f"private input {path.name}")


def _ensure_directory(
    path: Path, mode: int, uid: int | None = None, gid: int | None = None
) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=mode)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
        raise RuntimeError(f"unsafe output directory: {path.name}")
    os.chmod(path, mode)
    if uid is not None and gid is not None:
        if os.geteuid() == 0:
            os.chown(path, uid, gid)
        elif info.st_uid != uid or info.st_gid != gid:
            raise RuntimeError(f"cannot establish output directory ownership: {path.name}")


def atomic_private(
    path: Path, payload: str, *, uid: int | None = None, gid: int | None = None
) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(temporary, flags, 0o600)
    try:
        encoded = payload.encode("utf-8")
        written = 0
        while written < len(encoded):
            written += os.write(descriptor, encoded[written:])
        if uid is not None and gid is not None:
            if os.geteuid() == 0:
                os.fchown(descriptor, uid, gid)
            else:
                info = os.fstat(descriptor)
                if info.st_uid != uid or info.st_gid != gid:
                    raise RuntimeError(f"cannot establish output ownership: {path.name}")
        os.fchmod(descriptor, 0o600)
        os.fsync(descriptor)
    except BaseException:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    info = path.lstat()
    expected_uid = os.getuid() if uid is None else uid
    expected_gid = os.getgid() if gid is None else gid
    if (
        not stat.S_ISREG(info.st_mode)
        or stat.S_ISLNK(info.st_mode)
        or stat.S_IMODE(info.st_mode) != 0o600
        or info.st_uid != expected_uid
        or info.st_gid != expected_gid
    ):
        raise RuntimeError(f"unsafe private output: {path.name}")


def atomic_private_json(path: Path, payload: dict[str, Any]) -> None:
    atomic_private(path, json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")


def _validate_bearer(value: Any, label: str) -> str:
    return require_string(value, label, maximum=32_768)


def _validate_provisioned_agent(
    value: Any, *, label: str, tenant_id: str, environment_id: str
) -> tuple[dict[str, str], str]:
    response = require_object(value, f"{label} provision response")
    agent = require_object(response.get("agent"), f"{label} agent")
    auth0 = require_object(response.get("auth0"), f"{label} provider result")
    initial = require_object(response.get("enrollment"), f"{label} initial enrollment")

    management_agent_id = canonical_uuid(agent.get("id"), f"{label} Management agent id")
    if agent.get("name") != label:
        raise RuntimeError(f"{label} provision response has the wrong name")
    if (
        canonical_uuid(agent.get("org_id"), f"{label} tenant id") != tenant_id
        or canonical_uuid(agent.get("environment_id"), f"{label} environment id") != environment_id
    ):
        raise RuntimeError(f"{label} provision response has the wrong authority scope")

    agent_id = require_string(auth0.get("jid"), f"{label} AgentID", maximum=512)
    if AGENT_ID_RE.fullmatch(agent_id) is None:
        raise RuntimeError(f"{label} has an invalid public AgentID")
    _validate_bearer(auth0.get("access_token"), f"{label} provider access token")
    _validate_bearer(auth0.get("token"), f"{label} provider wrapper")
    expires_in = auth0.get("expires_in")
    if type(expires_in) is not int or expires_in < 1:
        raise RuntimeError(f"{label} provider expiry is invalid")

    location = agent.get("location_metadata")
    provider_user_id = require_string(
        location.get("auth0_user_id") if isinstance(location, dict) else None,
        f"{label} provider user id",
        maximum=256,
    )
    initial_grant_id = canonical_uuid(initial.get("grant_id"), f"{label} initial grant id")
    initial_token = enrollment_token(
        initial.get("token"), initial_grant_id, f"{label} initial token"
    )
    if initial.get("mesh_id") is not None or initial.get("max_installations") is not None:
        raise RuntimeError(f"{label} initial grant is not unbound and reusable")

    return (
        {
            "label": label,
            "profile_id": label,
            "management_agent_id": management_agent_id,
            "agent_id": agent_id,
            "provider_user_id": provider_user_id,
            "initial_grant_id": initial_grant_id,
        },
        initial_token,
    )


def _validate_bounded_grant(
    value: Any, *, management_agent_id: str, mesh_id: str, label: str
) -> tuple[str, str]:
    grant = require_object(value, f"{label} bounded grant")
    grant_id = canonical_uuid(grant.get("id"), f"{label} bounded grant id")
    token = enrollment_token(grant.get("token"), grant_id, f"{label} bounded token")
    if (
        canonical_uuid(grant.get("agent_id"), f"{label} bounded grant agent id")
        != management_agent_id
        or grant.get("mesh_id") != mesh_id
        or grant.get("max_installations") != 1
        or grant.get("status") != "active"
        or grant.get("scope_type") != "mesh"
    ):
        raise RuntimeError(f"{label} bounded grant has the wrong scope or status")
    return grant_id, token


def _validate_revocation(value: Any, *, agent: dict[str, str]) -> None:
    grant = require_object(value, f"{agent['label']} initial grant revocation")
    if (
        canonical_uuid(grant.get("id"), f"{agent['label']} revoked grant id")
        != agent["initial_grant_id"]
        or canonical_uuid(grant.get("agent_id"), f"{agent['label']} revoked grant agent id")
        != agent["management_agent_id"]
        or grant.get("mesh_id") is not None
        or grant.get("max_installations") is not None
        or grant.get("status") != "revoked"
        or grant.get("scope_type") != "agent"
    ):
        raise RuntimeError(f"{agent['label']} initial grant revocation was not authoritative")


def _env_name(label: str) -> str:
    return label.upper().replace("-", "_")


def _safe_env_line(key: str, value: str) -> str:
    if re.fullmatch(r"[A-Z][A-Z0-9_]*", key) is None or SAFE_ENV_RE.fullmatch(value) is None:
        raise RuntimeError(f"cannot safely encode environment value for {key}")
    return f"{key}={value}"


def _write_outputs(
    state: Path,
    *,
    tenant_id: str,
    environment_id: str,
    management_mesh_id: str,
    mesh_id: str,
    agents: list[dict[str, str]],
    tokens: dict[str, str],
    output_uid: int,
    output_gid: int,
) -> None:
    _ensure_directory(state / "agents", 0o700)
    env_lines = [_safe_env_line("CYNAPSA_MESH_ID", mesh_id)]
    for agent in agents:
        label = agent["label"]
        directory = state / "agents" / label
        _ensure_directory(directory, 0o700, AGENT_UID, AGENT_GID)
        atomic_private(
            directory / "token",
            tokens[label] + "\n",
            uid=AGENT_UID,
            gid=AGENT_GID,
        )
        prefix = f"CYNAPSA_{_env_name(label)}"
        env_lines.extend(
            (
                _safe_env_line(f"{prefix}_AGENT_ID", agent["agent_id"]),
                _safe_env_line(f"{prefix}_TOKEN_FILE", f"/state/agents/{label}/token"),
                _safe_env_line(f"{prefix}_PROFILE_ID", agent["profile_id"]),
            )
        )

    atomic_private(
        state / "sdk-e2e.env",
        "\n".join(env_lines) + "\n",
        uid=output_uid,
        gid=output_gid,
    )
    atomic_private(
        state / "sdk-e2e-expected.json",
        json.dumps({
            "tenant_id": tenant_id,
            "environment_id": environment_id,
            "management_mesh_id": management_mesh_id,
            "mesh_id": mesh_id,
            "agents": agents,
        }, sort_keys=True, separators=(",", ":")) + "\n",
        uid=output_uid,
        gid=output_gid,
    )


def provision(
    state: Path,
    *,
    auth0_url: str = AUTH0_URL,
    management_url: str = MANAGEMENT_URL,
    api: JSONAPI | None = None,
) -> None:
    state = state.resolve()
    if not state.is_dir():
        raise RuntimeError("state directory does not exist")
    auth0_url = base_url(auth0_url, "Auth0 URL")
    management_url = base_url(management_url, "Management URL")
    if api is None:
        configured_ca = Path(os.environ.get("SSL_CERT_FILE", state / "certs/ca.crt")).resolve()
        expected_ca = (state / "certs/ca.crt").resolve()
        if configured_ca != expected_ca:
            raise RuntimeError("SSL_CERT_FILE must name /state/certs/ca.crt")
        api = API(configured_ca)
    try:
        output_uid = int(os.environ.get("CYNAPSA_E2E_HOST_UID", str(os.getuid())))
        output_gid = int(os.environ.get("CYNAPSA_E2E_HOST_GID", str(os.getgid())))
    except ValueError:
        raise RuntimeError("host output UID/GID must be decimal integers") from None
    if output_uid < 0 or output_gid < 0:
        raise RuntimeError("host output UID/GID must be nonnegative")

    bootstrap = require_object(
        api.json(
            "POST",
            f"{auth0_url}/test/bootstrap-token",
            {"email": "sdk-e2e-superadmin@example.com", "name": "SDK E2E Superadmin"},
            headers={
                "X-Test-Bootstrap-Secret": private_text(
                    state / "secrets/auth0-bootstrap-secret"
                )
            },
        ),
        "provider bootstrap response",
    )
    bootstrap_token = _validate_bearer(bootstrap.get("access_token"), "provider bootstrap token")
    login = require_object(
        api.json(
            "POST",
            f"{management_url}/api/v1/auth/login",
            {"auth0_token": bootstrap_token},
        ),
        "Management login response",
    )
    global_token = _validate_bearer(login.get("access_token"), "Management global bearer")

    tenant = require_object(
        api.json(
            "POST",
            f"{management_url}/api/v1/tenants",
            {"name": "SDK E2E", "domain_slug": "sdk-e2e"},
            bearer=global_token,
        ),
        "tenant response",
    )
    tenant_id = canonical_uuid(tenant.get("id"), "tenant id")
    if tenant.get("domain_slug") != "sdk-e2e":
        raise RuntimeError("Management returned the wrong tenant slug")
    tenant_login = require_object(
        api.json(
            "POST",
            f"{management_url}/api/v1/auth/impersonate",
            {"target_org_id": tenant_id},
            bearer=global_token,
        ),
        "tenant impersonation response",
    )
    tenant_token = _validate_bearer(tenant_login.get("access_token"), "Management tenant bearer")
    defaults = require_object(
        api.json("GET", f"{management_url}/api/v1/meta/defaults", bearer=tenant_token),
        "tenant defaults",
    )
    environment_id = canonical_uuid(defaults.get("environment_id"), "default environment id")

    mesh = require_object(
        api.json(
            "POST",
            f"{management_url}/api/v1/meshes",
            {
                "name": "sdk-e2e",
                "description": "Disposable SDK six-agent E2E mesh",
                "environment_id": environment_id,
                "org_id": tenant_id,
                "allowed_geos": [],
                "data_zones_allowed": [],
            },
            bearer=tenant_token,
        ),
        "mesh response",
    )
    management_mesh_id = canonical_uuid(mesh.get("id"), "Management mesh id")
    if (
        canonical_uuid(mesh.get("org_id"), "mesh tenant id") != tenant_id
        or canonical_uuid(mesh.get("environment_id"), "mesh environment id") != environment_id
    ):
        raise RuntimeError("Management returned a mesh in the wrong authority scope")
    mesh_id = require_string(mesh.get("ejabberd_group_id"), "authoritative mesh id", maximum=255)
    if MESH_ID_RE.fullmatch(mesh_id) is None:
        raise RuntimeError("Management returned an invalid authoritative mesh id")

    agents: list[dict[str, str]] = []
    initial_tokens: list[str] = []
    for label in AGENT_LABELS:
        provisioned = api.json(
            "POST",
            f"{management_url}/api/v1/agents/provision",
            {
                "name": label,
                "environment_id": environment_id,
                "org_id": tenant_id,
                "location_type": "cloud",
                "framework": "cynapsa-core",
            },
            bearer=tenant_token,
        )
        agent, initial_token = _validate_provisioned_agent(
            provisioned,
            label=label,
            tenant_id=tenant_id,
            environment_id=environment_id,
        )
        agents.append(agent)
        initial_tokens.append(initial_token)

    for field in ("management_agent_id", "agent_id", "provider_user_id", "initial_grant_id"):
        if len({agent[field] for agent in agents}) != len(AGENT_LABELS):
            raise RuntimeError(f"Management did not provision six distinct {field} values")
    if len(set(initial_tokens)) != len(AGENT_LABELS):
        raise RuntimeError("Management did not provision six distinct initial enrollment tokens")

    bounded_tokens: dict[str, str] = {}
    for agent in agents:
        membership = require_object(
            api.json(
                "POST",
                f"{management_url}/api/v1/meshes/{management_mesh_id}/members"
                f"?agent_id={agent['management_agent_id']}&membership_type=direct",
                {},
                bearer=tenant_token,
            ),
            f"{agent['label']} membership response",
        )
        if membership.get("detail") != "Member added":
            raise RuntimeError(f"{agent['label']} was not added as a new direct mesh member")

        grant = api.json(
            "POST",
            f"{management_url}/api/v1/agents/{agent['management_agent_id']}"
            "/enrollment-tokens",
            {"mesh_id": mesh_id, "max_installations": 1},
            bearer=tenant_token,
        )
        grant_id, token = _validate_bounded_grant(
            grant,
            management_agent_id=agent["management_agent_id"],
            mesh_id=mesh_id,
            label=agent["label"],
        )
        agent["bounded_grant_id"] = grant_id
        bounded_tokens[agent["label"]] = token

        revoked = api.json(
            "POST",
            f"{management_url}/api/v1/agents/{agent['management_agent_id']}"
            f"/enrollment-tokens/{agent['initial_grant_id']}/revoke",
            {"revoke_installations": False},
            bearer=tenant_token,
        )
        _validate_revocation(revoked, agent=agent)

    if len(set(bounded_tokens.values())) != len(AGENT_LABELS):
        raise RuntimeError("Management did not issue six distinct bounded enrollment tokens")
    _write_outputs(
        state,
        tenant_id=tenant_id,
        environment_id=environment_id,
        management_mesh_id=management_mesh_id,
        mesh_id=mesh_id,
        agents=agents,
        tokens=bounded_tokens,
        output_uid=output_uid,
        output_gid=output_gid,
    )
    print("provisioned SDK E2E authentication state for six agents")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, default=Path("/state"))
    parser.add_argument("--auth0-url", default=AUTH0_URL)
    parser.add_argument("--management-url", default=MANAGEMENT_URL)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    provision(args.state, auth0_url=args.auth0_url, management_url=args.management_url)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
