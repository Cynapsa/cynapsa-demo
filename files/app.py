from __future__ import annotations

import argparse
import logging
import os
import threading

from runtime import prepare_runtime

prepare_runtime()

import cynapsa
from connection_alerts import watch_connection
from llm import ModelClient, ModelError
from runtime import connection_options, litellm_key
from tools import FileDatabase
from workflow import answer_question
from rpc_logging import configure_logging, log_handler

LOG = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only Cynapsa files agent")
    parser.add_argument("--enroll", action="store_true")
    parser.add_argument("--force-enroll", action="store_true")
    args = parser.parse_args()
    configure_logging()
    database = FileDatabase(os.environ["DEMO_FILES_DIRECTORY"])
    client = ModelClient(litellm_key(),
                         base_url=os.environ.get("LITELLM_BASE_URL", "https://litellm.eladrave.com"),
                         model=os.environ.get("LITELLM_MODEL", "gpt-5.6-terra-high"))
    try:
        with cynapsa.connect(**connection_options(enroll=args.enroll, force_enroll=args.force_enroll)) as session, watch_connection(session, "demo-files"):
            @session.on("*")
            @log_handler("demo-files")
            def receive(request: cynapsa.CynapsaRequest) -> dict:
                try:
                    data = request.json()
                    question = data.get("question") if isinstance(data, dict) else None
                    if not isinstance(question, str) or not 1 <= len(question.strip()) <= 2_000:
                        raise ValueError
                except (ValueError, UnicodeError):
                    raise cynapsa.RPCException(400, code="bad_request", detail="Expected a question of 1–2000 characters") from None
                try:
                    return answer_question(question.strip(), client, database)
                except ModelError as exc:
                    LOG.warning("Files model request failed with status %s", exc.status_code)
                    raise cynapsa.RPCException(503, code="model_unavailable", detail="The model gateway is unavailable") from None
                except Exception:
                    LOG.exception("Files request failed")
                    raise cynapsa.RPCException(502, code="files_failed", detail="The files agent could not answer") from None

            print(f"demo-files ready as {session.agent_id}", flush=True)
            threading.Event().wait()
    except KeyboardInterrupt:
        pass
    finally:
        client.close()


if __name__ == "__main__":
    main()
