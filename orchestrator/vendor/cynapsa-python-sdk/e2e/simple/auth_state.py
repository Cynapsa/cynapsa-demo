#!/usr/bin/env python3
"""Create one private, disposable local-authentication state tree for the E2E."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import secrets
import subprocess
from pathlib import Path

AGENTS = (
    "native-server",
    "monkey-server",
    "native-sync-client",
    "native-async-client",
    "monkey-sync-client",
    "monkey-async-client",
)
SECRET_UIDS = {
    "auth0-client-secret": 10001,
    "auth0-bootstrap-secret": 10001,
    "auth0-signing-key.pem": 10001,
    "enrollment-management-service-token": 65532,
    "ejabberd-api-password": 9000,
    "erlang-cookie": 9000,
    "turn-secret": 65534,
}


def run(command: list[str]) -> None:
    subprocess.run(
        command,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def secure_directory(path: Path) -> None:
    path.mkdir(parents=True, mode=0o700, exist_ok=True)
    path.chmod(0o700)


def write_private(path: Path, value: str | bytes, uid: int | None = None) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        payload = value.encode("utf-8") if isinstance(value, str) else value
        os.write(descriptor, payload)
        os.fsync(descriptor)
        if uid is not None and os.geteuid() == 0:
            os.fchown(descriptor, uid, uid)
    finally:
        os.close(descriptor)


def generate_key(path: Path, uid: int | None = None) -> None:
    run(
        [
            "openssl",
            "genpkey",
            "-algorithm",
            "RSA",
            "-pkeyopt",
            "rsa_keygen_bits:2048",
            "-out",
            str(path),
        ]
    )
    path.chmod(0o600)
    if uid is not None and os.geteuid() == 0:
        os.chown(path, uid, uid)


def generate_ca(certs: Path) -> None:
    generate_key(certs / "ca.key")
    run(
        [
            "openssl",
            "req",
            "-x509",
            "-new",
            "-sha256",
            "-key",
            str(certs / "ca.key"),
            "-subj",
            "/CN=Cynapsa SDK local E2E CA",
            "-days",
            "2",
            "-out",
            str(certs / "ca.crt"),
            "-addext",
            "basicConstraints=critical,CA:TRUE,pathlen:0",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        ]
    )
    (certs / "ca.crt").chmod(0o444)


def generate_leaf(certs: Path, name: str, dns_names: tuple[str, ...], uid: int) -> None:
    key = certs / f"{name}.key"
    request = certs / f"{name}.csr"
    certificate = certs / f"{name}.crt"
    generate_key(key, uid)
    run(
        [
            "openssl",
            "req",
            "-new",
            "-sha256",
            "-key",
            str(key),
            "-subj",
            f"/CN={dns_names[0]}",
            "-addext",
            "subjectAltName=" + ",".join(f"DNS:{value}" for value in dns_names),
            "-out",
            str(request),
        ]
    )
    run(
        [
            "openssl",
            "x509",
            "-req",
            "-sha256",
            "-in",
            str(request),
            "-CA",
            str(certs / "ca.crt"),
            "-CAkey",
            str(certs / "ca.key"),
            "-CAcreateserial",
            "-days",
            "2",
            "-copy_extensions",
            "copy",
            "-out",
            str(certificate),
        ]
    )
    request.unlink()
    certificate.chmod(0o444)


def read_tlv(data: bytes, offset: int) -> tuple[int, bytes, int]:
    tag = data[offset]
    length = data[offset + 1]
    cursor = offset + 2
    if length & 0x80:
        count = length & 0x7F
        length = int.from_bytes(data[cursor : cursor + count], "big")
        cursor += count
    return tag, data[cursor : cursor + length], cursor + length


def rsa_numbers(private_key: Path) -> tuple[int, int]:
    encoded = subprocess.check_output(
        ["openssl", "pkey", "-in", str(private_key), "-pubout", "-outform", "DER"],
        stderr=subprocess.DEVNULL,
    )
    tag, outer, _ = read_tlv(encoded, 0)
    if tag != 0x30:
        raise RuntimeError("unexpected public-key encoding")
    _, _, cursor = read_tlv(outer, 0)
    tag, bit_string, _ = read_tlv(outer, cursor)
    if tag != 0x03 or not bit_string or bit_string[0] != 0:
        raise RuntimeError("unexpected public-key bit string")
    tag, rsa, _ = read_tlv(bit_string, 1)
    if tag != 0x30:
        raise RuntimeError("unexpected RSA public-key encoding")
    tag, modulus, cursor = read_tlv(rsa, 0)
    exponent_tag, exponent, _ = read_tlv(rsa, cursor)
    if tag != 0x02 or exponent_tag != 0x02:
        raise RuntimeError("unexpected RSA public-key values")
    return int.from_bytes(modulus, "big"), int.from_bytes(exponent, "big")


def b64uint(value: int) -> str:
    width = max(1, (value.bit_length() + 7) // 8)
    return base64.urlsafe_b64encode(value.to_bytes(width, "big")).rstrip(b"=").decode()


def write_jwks(signing_key: Path, destination: Path) -> None:
    modulus, exponent = rsa_numbers(signing_key)
    n_value, e_value = b64uint(modulus), b64uint(exponent)
    thumbprint = json.dumps(
        {"e": e_value, "kty": "RSA", "n": n_value},
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    kid = base64.urlsafe_b64encode(hashlib.sha256(thumbprint).digest()).rstrip(b"=").decode()
    payload = {
        "keys": [
            {
                "alg": "RS256",
                "e": e_value,
                "kid": kid,
                "kty": "RSA",
                "n": n_value,
                "use": "sig",
            }
        ]
    }
    destination.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")
    destination.chmod(0o444)


def generate(state: Path, template: Path) -> None:
    state = state.resolve()
    if state.exists() and (not state.is_dir() or any(state.iterdir())):
        raise SystemExit(f"authentication state is not an empty directory: {state}")
    secure_directory(state)
    for relative in ("certs", "config", "public", "secrets", "agents"):
        secure_directory(state / relative)
    for agent in AGENTS:
        secure_directory(state / "agents" / agent)

    secrets_dir = state / "secrets"
    values = {
        "postgres-password": secrets.token_urlsafe(32),
        "auth0-client-secret": secrets.token_urlsafe(32),
        "auth0-bootstrap-secret": secrets.token_urlsafe(32),
        "management-jwt-secret": secrets.token_urlsafe(48),
        "enrollment-credential-key": base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode(),
        "management-enrollment-service-token": secrets.token_urlsafe(48),
        "ejabberd-api-password": secrets.token_urlsafe(32),
        "erlang-cookie": secrets.token_hex(24),
        "turn-secret": secrets.token_urlsafe(48),
    }
    values["enrollment-management-service-token"] = values[
        "management-enrollment-service-token"
    ]
    values["auth0-client-secret-management"] = values["auth0-client-secret"]
    values["ejabberd-api-password-management"] = values["ejabberd-api-password"]
    for name, value in values.items():
        # ejabberd reads its controller password as raw bytes while Management
        # obtains the same secret through shell command substitution. Keep the
        # bytes identical for both consumers.
        write_private(secrets_dir / name, value, SECRET_UIDS.get(name))
    (secrets_dir / "erlang-cookie").chmod(0o400)

    signing_key = secrets_dir / "auth0-signing-key.pem"
    generate_key(signing_key, SECRET_UIDS["auth0-signing-key.pem"])
    write_jwks(signing_key, state / "public" / "auth0.jwks")

    certs = state / "certs"
    generate_ca(certs)
    system_bundle = Path("/etc/ssl/certs/ca-certificates.crt")
    if system_bundle.is_file() and system_bundle.stat().st_size <= 4 * 1024 * 1024:
        combined = system_bundle.read_bytes().rstrip() + b"\n" + (certs / "ca.crt").read_bytes()
    else:
        combined = (certs / "ca.crt").read_bytes()
    (certs / "combined-ca.crt").write_bytes(combined)
    (certs / "combined-ca.crt").chmod(0o444)
    generate_leaf(certs, "auth0", ("auth0.cynapsa.test",), 10001)
    generate_leaf(certs, "management", ("management.cynapsa.test",), 0)
    generate_leaf(certs, "enrollment", ("enrollment.cynapsa.com",), 65532)
    generate_leaf(certs, "ejabberd", ("mesh.test", "devices.example.com"), 9000)

    rendered = template.read_text(encoding="utf-8")
    marker = "@CYNAPSA_TURN_STATIC_AUTH_SECRET@"
    if rendered.count(marker) != 1:
        raise RuntimeError("ejabberd template must contain exactly one TURN secret marker")
    write_private(
        state / "config" / "ejabberd.yml",
        rendered.replace(marker, values["turn-secret"]),
        9000,
    )
    (state / "config" / "ejabberd.yml").chmod(0o644)

    source_root = Path(__file__).resolve().parent
    stun_config = (source_root / "coturn-stun.conf").read_text(encoding="utf-8")
    turn_template = (source_root / "coturn-turn.conf.in").read_text(encoding="utf-8")
    if turn_template.count(marker) != 1:
        raise RuntimeError("Coturn template must contain exactly one TURN secret marker")
    write_private(state / "coturn-stun.conf", stun_config)
    write_private(
        state / "coturn-turn.conf",
        turn_template.replace(marker, values["turn-secret"]),
        SECRET_UIDS["turn-secret"],
    )
    (state / "coturn-stun.conf").chmod(0o644)
    (state / "coturn-turn.conf").chmod(0o644)
    metadata = {
        "identity_provider": "local-auth0-emulator",
        "qualification_scope": "local-offline-contract",
        "agents": list(AGENTS),
    }
    write_private(
        state / "auth-contract.json",
        json.dumps(metadata, sort_keys=True, separators=(",", ":")) + "\n",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument(
        "--template",
        type=Path,
        default=Path(__file__).with_name("ejabberd.yml"),
    )
    arguments = parser.parse_args()
    generate(arguments.state, arguments.template.resolve())
    print("local authentication state generated")


if __name__ == "__main__":
    main()
