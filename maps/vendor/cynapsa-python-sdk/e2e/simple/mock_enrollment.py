"""Isolated test issuer for the SDK E2E's fixed production enrollment URL.

The server has no access to the public internet. Its DNS name is overridden
only inside test agent containers, and its certificate chains to the per-run CA.
"""

from __future__ import annotations

import base64
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import re
import ssl
from pathlib import Path
import subprocess
import time


ROOT = Path("/fixture")
CONFIG = json.loads((ROOT / "mock-enrollment.json").read_text(encoding="utf-8"))
ISSUER = "https://e2e-issuer.mesh.test/"
AUDIENCE = "cynapsa-e2e-runtime"
INSTALLATION = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def sign_jwt(payload: dict[str, object]) -> str:
    header = {"alg": "RS256", "kid": CONFIG["kid"], "typ": "JWT"}
    compact = b".".join(
        b64url(json.dumps(value, separators=(",", ":"), sort_keys=True).encode()).encode()
        for value in (header, payload)
    )
    signed = subprocess.run(
        ["openssl", "dgst", "-sha256", "-sign", str(ROOT / "runtime-signing.pem")],
        input=compact,
        capture_output=True,
        check=True,
    ).stdout
    return compact.decode("ascii") + "." + b64url(signed)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, _format: str, *_args: object) -> None:
        # Enrollment bearer material, identities, and HTTP bodies stay out of logs.
        return

    def do_GET(self) -> None:
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_error(404)

    def do_POST(self) -> None:
        if self.path != "/v2/enroll":
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("Content-Length", "-1"))
            if length < 1 or length > 8192:
                raise ValueError("invalid length")
            request = json.loads(self.rfile.read(length))
            if set(request) != {"token", "mesh_id", "installation_id", "installation_secret"}:
                raise ValueError("invalid fields")
            grant = CONFIG["grants"].get(request["token"])
            installation = request["installation_id"]
            if grant is None or request["mesh_id"] != "simple-e2e" or not INSTALLATION.fullmatch(installation):
                raise ValueError("invalid grant")
            if not isinstance(request["installation_secret"], str) or len(request["installation_secret"]) != 43:
                raise ValueError("invalid installation secret")
            user = grant["user"]
            agent_jid = f"{user}@mesh.test"
            nonce = b64url(hashlib.sha256((installation + request["token"]).encode()).digest())[:16]
            resource = f"r2.{installation}.{nonce}"
            now = int(time.time())
            runtime = {
                "v": 2, "organization_id": CONFIG["organization_id"],
                "agent_id": grant["agent_id"], "agent_jid": agent_jid,
                "installation_id": installation, "mesh_id": "simple-e2e",
                "token_id": "local-e2e-grant", "session_resource": resource,
                "installation_epoch": 1, "attachment_revision": 1,
                "policy_revision": 1, "session_expiry_mode": "continue",
                "offline_cold_start_target_seconds": 0,
            }
            claims = {
                "iss": ISSUER, "aud": AUDIENCE, "sub": f"local-e2e:{user}",
                "iat": now, "nbf": now, "exp": now + 3600,
                "email": agent_jid, "https://cynapsa.com/runtime": runtime,
            }
            response = {
                "version": "e2", "organization_id": CONFIG["organization_id"],
                "installation_id": installation, "agent_id": grant["agent_id"],
                "agent_jid": agent_jid, "mesh_id": "simple-e2e",
                "session_resource": resource, "mesh_endpoint": "10.240.70.2:5222",
                "username": agent_jid, "server": "10.240.70.2",
                "access_token": sign_jwt(claims), "expires_in": 3600,
                "wrapper": "aztm_local-e2e", "display": user,
                "installation_epoch": 1, "attachment_revision": 1,
                "token_id": "local-e2e-grant", "policy_revision": 1,
                "session_expiry_mode": "continue",
                "offline_cold_start_target_seconds": 0,
            }
            data = json.dumps(response, separators=(",", ":")).encode()
        except (KeyError, TypeError, ValueError, subprocess.CalledProcessError):
            self.send_error(403)
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def main() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", 443), Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(ROOT / "enrollment-cert.pem", ROOT / "enrollment-key.pem")
    server.socket = context.wrap_socket(server.socket, server_side=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
