"""Minimal OpenAI-compatible gateway smoke test; credentials stay outside source."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


def main() -> int:
    parser = argparse.ArgumentParser(description="Test the configured OpenAI-compatible gateway")
    parser.add_argument("--tools", action="store_true", help="also verify a function-call round trip")
    args = parser.parse_args()
    base = os.environ.get("LITELLM_BASE_URL", "https://litellm.eladrave.com").rstrip("/")
    parsed = urlsplit(base)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        print("LITELLM_BASE_URL must be an HTTPS URL without embedded credentials.", file=sys.stderr)
        return 2
    endpoint = base + ("/chat/completions" if base.endswith("/v1") else "/v1/chat/completions")
    model = os.environ.get("LITELLM_MODEL", "gpt-5.6-terra-high")
    key_file = os.environ.get("LITELLM_API_KEY_FILE")
    key = Path(key_file).read_text(encoding="utf-8").strip() if key_file else os.environ.get("LITELLM_API_KEY", "")
    if not key:
        print("Set LITELLM_API_KEY or LITELLM_API_KEY_FILE before running.", file=sys.stderr)
        return 2

    messages = [{"role": "user", "content": "Call lookup for Central Park." if args.tools else "Reply with OK."}]
    payload: dict[str, object] = {"model": model, "messages": messages}
    if args.tools:
        payload["tools"] = [{
            "type": "function",
            "function": {
                "name": "lookup",
                "description": "Find a place by name.",
                "parameters": {
                    "type": "object",
                    "properties": {"query": {"type": "string"}},
                    "required": ["query"],
                    "additionalProperties": False,
                },
            },
        }]
        payload["tool_choice"] = "required"

    def call(body: dict[str, object]) -> dict[str, object]:
        request = Request(
            endpoint,
            data=json.dumps(body).encode("utf-8"),
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request, timeout=30) as response:
            return json.load(response)

    try:
        result = call(payload)
        if args.tools:
            choices = result.get("choices", [])
            message = choices[0].get("message", {}) if choices else {}
            calls = message.get("tool_calls") or []
            if len(calls) != 1 or calls[0].get("function", {}).get("name") != "lookup":
                print("Gateway did not return the requested function call.", file=sys.stderr)
                return 1
            print("Gateway returned a lookup function call.")
            messages.extend([
                message,
                {"role": "tool", "tool_call_id": calls[0]["id"], "content": '{"result":"Central Park, New York"}'},
            ])
            result = call({"model": model, "messages": messages})
    except HTTPError as error:
        try:
            body = json.load(error)
            details = body.get("error", {})
            code = details.get("code") if isinstance(details, dict) else None
        except (ValueError, AttributeError):
            code = None
        safe_code = code if isinstance(code, str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", code) else None
        suffix = f" ({safe_code})" if safe_code else ""
        print(f"Gateway returned HTTP {error.code}{suffix}.", file=sys.stderr)
        return 1
    except URLError as error:
        print(f"Connection failed: {error.reason}", file=sys.stderr)
        return 1

    choices = result.get("choices", [])
    content = choices[0].get("message", {}).get("content") if choices else None
    if not isinstance(content, str) or not content.strip():
        print("Gateway returned no text response.", file=sys.stderr)
        return 1
    print(f"Gateway responded using {result.get('model', model)}: {content.strip()[:160]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
