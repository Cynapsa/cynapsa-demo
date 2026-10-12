"""Interactive HTTP client for the demo orchestrator."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import uuid

import requests


LOG = logging.getLogger("demo.http")
DEFAULT_ORIGIN = "http://demo-orchestrator.local"


def main() -> None:
    parser = argparse.ArgumentParser(description="Demo chat client")
    parser.add_argument("--quiet", action="store_true", help="Hide diagnostic logs and startup messages")
    parser.add_argument("--conversation-id", default=os.environ.get("DEMO_CONVERSATION_ID"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    if args.quiet:
        logging.disable(logging.CRITICAL)

    conversation_id = args.conversation_id or uuid.uuid4().hex
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", conversation_id):
        parser.error("conversation ID must contain 1–64 letters, numbers, underscores or hyphens")
    url = os.environ.get("DEMO_ORCHESTRATOR_ORIGIN", DEFAULT_ORIGIN).rstrip("/") + "/ask"

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
        LOG.info("[client] HTTP request sent to orchestrator %s: %s", url,
                 json.dumps(payload, ensure_ascii=False))
        try:
            response = requests.post(url, json=payload, timeout=140)
            LOG.info("[client] HTTP response received from orchestrator %s (%s): %s",
                     url, response.status_code, response.text)
            response.raise_for_status()
            data = response.json()
            print(data["answer"], flush=True)
            for source in (data.get("maps") or {}).get("sources", []):
                print(f"  {source.get('name') or 'Place'}: {source['google_maps_url']}")
            for source in (data.get("files") or {}).get("sources", []):
                print(f"  File: {source['path']}:{source.get('line', source.get('start_line', 1))}", flush=True)
        except Exception as exc:
            LOG.warning("[client] HTTP request to orchestrator failed: %r", exc)
            print("Request failed: The demo could not answer", flush=True)


if __name__ == "__main__":
    main()
