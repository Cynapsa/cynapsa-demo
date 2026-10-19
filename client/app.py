"""Interactive HTTP client for the demo orchestrator."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
import uuid

import requests


LOG = logging.getLogger("demo.http")
DEFAULT_ORIGIN = "http://demo-orchestrator.local"


def main() -> None:
    parser = argparse.ArgumentParser(description="Demo chat client")
    parser.add_argument("--quiet", action="store_true", help="Hide diagnostic logs and startup messages")
    parser.add_argument("--conversation-id", default=os.environ.get("DEMO_CONVERSATION_ID"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    if args.quiet:
        logging.disable(logging.CRITICAL)

    conversation_id = args.conversation_id or uuid.uuid4().hex
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", conversation_id):
        parser.error("conversation ID must contain 1–64 letters, numbers, underscores or hyphens")
    url = os.environ.get("DEMO_ORCHESTRATOR_ORIGIN", DEFAULT_ORIGIN).rstrip("/") + "/ask"
    transport = "cynapsa" if os.environ.get("DEMO_TRANSPORT") == "cynapsa" else "http"

    if not args.quiet:
        print("demo-client ready", flush=True)
        print(f"Conversation: {conversation_id} (type 'new' for a fresh chat)", flush=True)
    while True:
        try:
            prompt = input('Ask a question (or "quit"): ').strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if prompt.lower() == "quit":
            return
        if prompt.lower() == "new":
            conversation_id = uuid.uuid4().hex
            print(f"Conversation: {conversation_id}", flush=True)
            continue
        if not prompt:
            continue

        payload = {"prompt": prompt, "conversation_id": conversation_id}
        request_id = uuid.uuid4().hex[:8]
        started = time.monotonic()
        LOG.info('[%s] request started to "orchestrator" (%s) id=%s: %s',
                 transport, os.environ.get("DEMO_ORCHESTRATOR_AGENT_ID", url), request_id,
                 json.dumps(payload, ensure_ascii=False))
        try:
            headers = {"x-demo-request-id": request_id}
            if transport == "http":
                key = os.environ.get("DEMO_HTTP_ACCESS_KEY", "")
                if not key:
                    raise RuntimeError("DEMO_HTTP_ACCESS_KEY is required for HTTP transport")
                headers["x-demo-access-key"] = key
            response = requests.post(url, json=payload, headers=headers, timeout=140)
            LOG.info('[%s] response received from "orchestrator" (%s) id=%s status=%d duration_ms=%d: %s',
                     transport, os.environ.get("DEMO_ORCHESTRATOR_AGENT_ID", url),
                     request_id, response.status_code, round((time.monotonic() - started) * 1000), response.text)
            response.raise_for_status()
            data = response.json()
            print(data["answer"], flush=True)
            for source in (data.get("maps") or {}).get("sources", []):
                print(f"  {source.get('name') or 'Place'}: {source['google_maps_url']}")
            for source in (data.get("files") or {}).get("sources", []):
                print(f"  File: {source['path']}:{source.get('line', source.get('start_line', 1))}", flush=True)
        except Exception as exc:
            LOG.warning('[%s] request to "orchestrator" failed id=%s duration_ms=%d: %r',
                        transport, request_id, round((time.monotonic() - started) * 1000), exc)
            print("Request failed: The demo could not answer", flush=True)


if __name__ == "__main__":
    # To use Cynapsa without the CLI, uncomment this block and comment out
    # the plain main() below. Set CYNAPSA_TOKEN for first enrollment only.
    # import cynapsa
    # origin = "https://de-4f1d206529514541bb8cb316c5b0b51e.ecs.us-east-1.on.aws"
    # with cynapsa.login(
    #     mesh_id="cynapsa_demo_cynapsa_demo",
    #     enrollment_token=os.environ.get("CYNAPSA_TOKEN") or None,
    #     profile_id="demo-client",
    #     address_map={origin: {
    #         "recipient": "cynapsa-demo.agent-4dcbded5-8c32-435a-9e43-eeb37d903e98@connect.cynapsa.com",
    #         "mode": "rpc",
    #     }},
    #     rpc_timeout_ms=140000,
    # ):
    #     os.environ["DEMO_ORCHESTRATOR_ORIGIN"] = origin
    #     os.environ["DEMO_TRANSPORT"] = "cynapsa"  # Skip demo's HTTP-only access key.
    #     main()
    main()
