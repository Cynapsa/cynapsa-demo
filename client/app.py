from __future__ import annotations

import argparse
import os
import re
import uuid

from runtime import prepare_runtime

prepare_runtime()

import cynapsa

from runtime import connection_options, orchestrator_agent_id
from connection_alerts import watch_connection


def main() -> None:
    parser = argparse.ArgumentParser(description="Cynapsa demo client")
    parser.add_argument("--enroll", action="store_true")
    parser.add_argument("--force-enroll", action="store_true")
    parser.add_argument("--conversation-id", default=os.environ.get("DEMO_CONVERSATION_ID"))
    args = parser.parse_args()
    conversation_id = args.conversation_id or uuid.uuid4().hex
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", conversation_id):
        parser.error("conversation ID must contain 1–64 letters, numbers, underscores or hyphens")
    target = orchestrator_agent_id()

    with cynapsa.connect(**connection_options(enroll=args.enroll, force_enroll=args.force_enroll)) as session, watch_connection(session, "demo-client"):
        print(f"demo-client ready as {session.agent_id}", flush=True)
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
            try:
                response = session.request(
                    target,
                    {"prompt": prompt, "conversation_id": conversation_id},
                    path="/ask",
                    ttl_ms=140_000,
                )
                if response.status_code >= 400:
                    error = response.error
                    code = error.code if error is not None else "remote_error"
                    detail = error.detail if error is not None else response.reason
                    print(f"Request failed ({response.status_code} {code}): {detail}")
                    continue
                data = response.json()
                print(data["answer"], flush=True)
                maps = data.get("maps") or {}
                for source in maps.get("sources", []):
                    print(f"  {source.get('name') or 'Place'}: {source['google_maps_url']}")
            except (cynapsa.NativeError, cynapsa.SdkSafetyTimeout, ValueError, KeyError) as exc:
                print(f"Request failed: {exc}", flush=True)


if __name__ == "__main__":
    main()
