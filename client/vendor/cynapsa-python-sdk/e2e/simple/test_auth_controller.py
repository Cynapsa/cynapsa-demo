from __future__ import annotations

import importlib.util
import io
import json
import os
import stat
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any


MODULE_PATH = Path(__file__).with_name("auth_controller.py")
SPEC = importlib.util.spec_from_file_location("sdk_e2e_auth_controller", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
auth_controller = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(auth_controller)


def identifier(number: int) -> str:
    return f"00000000-0000-4000-8000-{number:012d}"


def token(grant_id: str, character: str) -> str:
    return f"cpsa_e1.{grant_id}.{character * 43}"


class FakeAPI:
    def __init__(self) -> None:
        self.agent_index = 0
        self.agents: dict[str, tuple[str, str]] = {}
        self.memberships = 0
        self.grants = 0
        self.revocations = 0

    def json(
        self,
        method: str,
        url: str,
        body: dict[str, Any] | None = None,
        *,
        bearer: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        if url.endswith("/test/bootstrap-token"):
            return {"access_token": "provider-bootstrap-token"}
        if url.endswith("/api/v1/auth/login"):
            return {"access_token": "management-global-token"}
        if url.endswith("/api/v1/tenants"):
            return {"id": identifier(1), "name": "SDK E2E", "domain_slug": "sdk-e2e"}
        if url.endswith("/api/v1/auth/impersonate"):
            return {"access_token": "management-tenant-token"}
        if url.endswith("/api/v1/meta/defaults"):
            return {"environment_id": identifier(2)}
        if url.endswith("/api/v1/meshes"):
            return {
                "id": identifier(3),
                "org_id": identifier(1),
                "environment_id": identifier(2),
                "ejabberd_group_id": "sdk_e2e_sdk-e2e",
            }
        if url.endswith("/api/v1/agents/provision"):
            index = self.agent_index
            self.agent_index += 1
            label = body["name"]  # type: ignore[index]
            agent_id = identifier(100 + index)
            initial_id = identifier(200 + index)
            self.agents[agent_id] = (label, initial_id)
            return {
                "agent": {
                    "id": agent_id,
                    "name": label,
                    "org_id": identifier(1),
                    "environment_id": identifier(2),
                    "location_metadata": {"auth0_user_id": f"auth0|{index}"},
                },
                "auth0": {
                    "jid": f"sdk-e2e.agent-{agent_id}@mesh.test",
                    "access_token": f"provider-access-{index}",
                    "token": f"provider-wrapper-{index}",
                    "expires_in": 3600,
                },
                "enrollment": {
                    "grant_id": initial_id,
                    "token": token(initial_id, chr(ord("A") + index)),
                    "mesh_id": None,
                    "max_installations": None,
                },
            }
        if "/members?" in url:
            self.memberships += 1
            return {"detail": "Member added"}
        if url.endswith("/enrollment-tokens"):
            self.grants += 1
            agent_id = url.split("/agents/", 1)[1].split("/", 1)[0]
            index = list(self.agents).index(agent_id)
            grant_id = identifier(300 + index)
            return {
                "id": grant_id,
                "agent_id": agent_id,
                "mesh_id": "sdk_e2e_sdk-e2e",
                "max_installations": 1,
                "status": "active",
                "scope_type": "mesh",
                "token": token(grant_id, chr(ord("G") + index)),
            }
        if url.endswith("/revoke"):
            self.revocations += 1
            agent_id = url.split("/agents/", 1)[1].split("/", 1)[0]
            _label, initial_id = self.agents[agent_id]
            return {
                "id": initial_id,
                "agent_id": agent_id,
                "mesh_id": None,
                "max_installations": None,
                "status": "revoked",
                "scope_type": "agent",
            }
        raise AssertionError(f"unexpected fake request: {method} {url}")


class ProvisioningControllerTests(unittest.TestCase):
    def test_provisions_six_agents_and_writes_only_bounded_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            (state / "secrets").mkdir()
            (state / "secrets/auth0-bootstrap-secret").write_text("bootstrap-secret\n")
            api = FakeAPI()
            original_uid = auth_controller.AGENT_UID
            original_gid = auth_controller.AGENT_GID
            auth_controller.AGENT_UID = os.getuid()
            auth_controller.AGENT_GID = os.getgid()
            try:
                output = io.StringIO()
                with redirect_stdout(output):
                    auth_controller.provision(state, api=api)
            finally:
                auth_controller.AGENT_UID = original_uid
                auth_controller.AGENT_GID = original_gid

            self.assertEqual(
                (api.agent_index, api.memberships, api.grants, api.revocations),
                (6, 6, 6, 6),
            )
            self.assertEqual(
                output.getvalue(),
                "provisioned SDK E2E authentication state for six agents\n",
            )
            environment = (state / "sdk-e2e.env").read_text()
            expected = json.loads((state / "sdk-e2e-expected.json").read_text())
            self.assertEqual(expected["mesh_id"], "sdk_e2e_sdk-e2e")
            self.assertEqual(len(expected["agents"]), 6)
            self.assertNotIn("cpsa_e1.", environment)
            self.assertNotIn("access-token", environment)
            self.assertNotIn("token", json.dumps(expected).lower())
            for index, label in enumerate(auth_controller.AGENT_LABELS):
                token_path = state / "agents" / label / "token"
                info = token_path.stat()
                self.assertEqual(stat.S_IMODE(info.st_mode), 0o600)
                self.assertTrue(
                    token_path.read_text().startswith(f"cpsa_e1.{identifier(300 + index)}.")
                )
                prefix = label.upper().replace("-", "_")
                self.assertIn(
                    f"CYNAPSA_{prefix}_TOKEN_FILE=/state/agents/{label}/token\n",
                    environment,
                )
                self.assertIn(f"CYNAPSA_{prefix}_PROFILE_ID={label}\n", environment)

    def test_rejects_token_whose_embedded_grant_id_differs(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "grant id"):
            auth_controller.enrollment_token(
                token(identifier(7), "A"), identifier(8), "bounded token"
            )

    def test_env_values_cannot_inject_another_assignment(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "safely encode"):
            auth_controller._safe_env_line("CYNAPSA_MESH_ID", "mesh\nINJECTED=value")

    def test_endpoint_cannot_embed_credentials_or_a_path(self) -> None:
        for value in (
            "http://management.test:8443",
            "https://user:secret@management.test",
            "https://management.test/api",
        ):
            with self.subTest(value=value), self.assertRaisesRegex(RuntimeError, "HTTPS origin"):
                auth_controller.base_url(value, "Management URL")


if __name__ == "__main__":
    unittest.main()
