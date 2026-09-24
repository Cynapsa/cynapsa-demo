#!/usr/bin/env python3
"""Create disposable, local-only enrollment grants and a runtime JWT JWKS."""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import uuid


USERS = (
    "native-sync-client",
    "native-async-client",
    "monkey-sync-client",
    "monkey-async-client",
    "native-server",
    "monkey-server",
)


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: prepare_mock_enrollment.py PRIVATE_RUNTIME_DIR")
    runtime = Path(sys.argv[1]).resolve(strict=True)
    key = runtime / "runtime-signing.pem"
    subprocess.run(
        ["openssl", "genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048", "-out", str(key)],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    key.chmod(0o400)
    result = subprocess.run(
        ["openssl", "rsa", "-in", str(key), "-noout", "-modulus"],
        check=True,
        capture_output=True,
        text=True,
    )
    modulus = result.stdout.strip().removeprefix("Modulus=")
    if not modulus or len(modulus) != 512:
        raise RuntimeError("unexpected local RSA public modulus")
    kid = b64url(secrets.token_bytes(12))
    jwks = {
        "keys": [{
            "kty": "RSA", "kid": kid, "alg": "RS256", "use": "sig",
            "n": b64url(bytes.fromhex(modulus)), "e": "AQAB",
        }]
    }
    (runtime / "runtime-jwks.json").write_text(json.dumps(jwks), encoding="utf-8")
    (runtime / "runtime-jwks.json").chmod(0o444)
    entries: dict[str, dict[str, str]] = {}
    organization_id = str(uuid.uuid4())
    for user in USERS:
        token = "cpsa_e1." + str(uuid.uuid4()) + "." + b64url(secrets.token_bytes(32))
        path = runtime / f"{user}.token"
        path.write_text(token + "\n", encoding="ascii")
        path.chmod(0o444)
        entries[token] = {
            "user": user,
            "agent_id": str(uuid.uuid4()),
        }
    fixture = {"kid": kid, "organization_id": organization_id, "grants": entries}
    fixture_path = runtime / "mock-enrollment.json"
    fixture_path.write_text(json.dumps(fixture), encoding="utf-8")
    fixture_path.chmod(0o400)
    os.chmod(runtime, 0o700)


if __name__ == "__main__":
    main()
