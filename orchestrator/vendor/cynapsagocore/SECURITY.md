# Security Policy

## Reporting

Do not disclose suspected vulnerabilities in a public issue. Contact the repository owners through a private GitHub security advisory until a dedicated security contact is published.

Include the affected version or commit, reproduction conditions, impact, and any known workaround. Do not include production credentials, private keys, personal data, or live service tokens.

## Security-sensitive implementation areas

The SDK boundary, credential loading, policy enforcement, payload integrity and encryption, identifier validation, resource limits, diagnostic redaction, and native handle ownership are security-sensitive. Changes in these areas require focused negative tests and review against `AZTM_SDK_BOUNDARY.md` and the authoritative [`docs/security/V1_SECURITY_MODEL.md`](docs/security/V1_SECURITY_MODEL.md). The V1 preferred direct carrier is infrastructure-confidential; server-mediated fallback carriers are transport-protected but expose payload plaintext to their terminating service. Do not report transport TLS as payload E2EE.
