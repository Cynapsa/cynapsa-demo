from __future__ import annotations

import json
import ssl
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import Request, urlopen


ALLOWED_ORIGINS = {"http://localhost:5173", "http://127.0.0.1:5173"}


def post(url: str, body: dict[str, object], *, headers: dict[str, str] | None = None) -> dict[str, object]:
    request_headers = {"content-type": "application/json", **(headers or {})}
    request = Request(
        url,
        data=json.dumps(body, separators=(",", ":")).encode(),
        headers=request_headers,
        method="POST",
    )
    context = ssl.create_default_context(cafile="/run/cynapsa/ca.pem")
    with urlopen(request, context=context, timeout=15) as response:
        value = json.load(response)
    if not isinstance(value, dict):
        raise RuntimeError("bootstrap endpoint returned a non-object")
    return value


def issue_session() -> dict[str, str]:
    bootstrap_secret = Path("/run/cynapsa/auth0-bootstrap-secret").read_text().strip()
    provider = post(
        "https://auth0.cynapsa.test:8443/test/bootstrap-token",
        {"email": "local-admin@example.com", "name": "Local administrator"},
        headers={"x-test-bootstrap-secret": bootstrap_secret},
    )
    provider_token = provider.get("access_token")
    if not isinstance(provider_token, str) or not provider_token:
        raise RuntimeError("Auth0 emulator did not return a bootstrap token")

    login = post(
        "https://management.cynapsa.test:8443/api/v1/auth/login",
        {"auth0_token": provider_token},
    )
    access_token = login.get("access_token")
    user = login.get("user")
    if not isinstance(access_token, str) or not access_token:
        raise RuntimeError("Management did not return a portal token")
    destination = "/dashboard" if isinstance(user, dict) and user.get("org_id") else "/setup"
    return {"access_token": access_token, "destination": destination}


class SessionHandler(BaseHTTPRequestHandler):
    server_version = "CynapsaLocalSession/1"

    def send_json(self, status: int, value: dict[str, str]) -> None:
        payload = json.dumps(value, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.send_header("cache-control", "no-store")
        origin = self.headers.get("origin")
        if origin in ALLOWED_ORIGINS:
            self.send_header("access-control-allow-origin", origin)
            self.send_header("vary", "origin")
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:
        if self.path == "/healthz":
            self.send_json(200, {"status": "ok"})
            return
        if self.path != "/session":
            self.send_json(404, {"error": "not_found"})
            return
        if self.headers.get("origin") not in ALLOWED_ORIGINS:
            self.send_json(403, {"error": "origin_not_allowed"})
            return
        try:
            self.send_json(200, issue_session())
        except Exception as error:
            print(f"local session refresh failed: {type(error).__name__}", flush=True)
            self.send_json(503, {"error": "session_refresh_failed"})

    def log_message(self, format: str, *args: object) -> None:
        print(f"local-session: {format % args}", flush=True)


def main() -> None:
    ThreadingHTTPServer(("0.0.0.0", 8081), SessionHandler).serve_forever()


if __name__ == "__main__":
    main()
