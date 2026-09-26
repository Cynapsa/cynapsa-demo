from __future__ import annotations

import argparse
import asyncio
import logging
import os
import uuid
from contextlib import AsyncExitStack
from typing import Any

from runtime import prepare_runtime

prepare_runtime()

import cynapsa
from pydantic import BaseModel, Field, ValidationError
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from llm import ModelClient, ModelError
from runtime import STATE, connection_options, litellm_key, maps_agent_id
from workflow import ConversationAgent, thread_key
from connection_alerts import watch_connection_async


LOG = logging.getLogger(__name__)
MODEL = os.environ.get("LITELLM_MODEL", "gpt-5.6-terra-high")
BASE_URL = os.environ.get("LITELLM_BASE_URL", "https://litellm.eladrave.com")


class AskBody(BaseModel):
    prompt: str = Field(min_length=1, max_length=2_000)
    conversation_id: str | None = Field(default=None, pattern=r"^[A-Za-z0-9_-]{1,64}$")


async def serve(*, enroll: bool, force_enroll: bool = False) -> None:
    target = maps_agent_id()
    model_client = ModelClient(litellm_key(), base_url=BASE_URL, model=MODEL)
    try:
        memory_path = STATE / "conversations.sqlite"
        fd = os.open(memory_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            os.fchmod(fd, 0o600)
        finally:
            os.close(fd)
        # Open storage before connecting. Reverse cleanup drains the session
        # while the saver remains available, even if later startup fails.
        async with AsyncExitStack() as resources:
            checkpointer = await resources.enter_async_context(
                AsyncSqliteSaver.from_conn_string(str(memory_path))
            )
            session = await resources.enter_async_context(
                await cynapsa.connect_async(**connection_options(enroll=enroll, force_enroll=force_enroll))
            )
            await resources.enter_async_context(watch_connection_async(session, "demo-orchestrator"))
            async def ask_maps(question: str) -> dict[str, Any]:
                remote = await session.request(
                    target, {"question": question}, path="/maps", ttl_ms=100_000
                )
                try:
                    return remote.json()
                except (ValueError, UnicodeError):
                    raise RuntimeError("maps agent returned an invalid response") from None

            agent = ConversationAgent(model_client, ask_maps, checkpointer)

            @session.on("/ask")
            async def ask(request: cynapsa.CynapsaRequest) -> dict[str, Any]:
                try:
                    body = AskBody.model_validate(request.json())
                    if (not body.prompt.strip() or not request.from_agent_id
                            or request.mesh_id != os.environ["DEMO_MESH_ID"].strip()):
                        raise ValueError("invalid authenticated conversation")
                    conversation_id = body.conversation_id or uuid.uuid4().hex
                    key = thread_key(
                        session.agent_id, request.mesh_id, request.from_agent_id, conversation_id
                    )
                except (ValueError, UnicodeError, ValidationError):
                    raise cynapsa.RPCException(
                        400, code="bad_request", detail="Expected a prompt and a valid conversation ID"
                    ) from None

                try:
                    answer = await agent.answer(body.prompt.strip(), key)
                    return {**answer, "conversation_id": conversation_id}
                except (TimeoutError, cynapsa.SdkSafetyTimeout) as exc:
                    LOG.warning("Maps RPC timed out: %r", exc, exc_info=True)
                    raise cynapsa.RPCException(
                        504, code="maps_timeout", detail="The maps agent timed out"
                    ) from None
                except ModelError as exc:
                    LOG.warning(
                        "Model gateway request failed with status %s: %r",
                        exc.status_code, exc, exc_info=True,
                    )
                    raise cynapsa.RPCException(
                        503, code="model_unavailable", detail="The model gateway is unavailable"
                    ) from None
                except cynapsa.RemoteNativeError as exc:
                    LOG.warning(
                        "Maps RPC returned %r; canonical response status=%s error=%r",
                        exc, exc.response.status_code, exc.response.error,
                        exc_info=True,
                    )
                    raise cynapsa.RPCException(
                        502, code="orchestrator_failed", detail="The demo could not answer"
                    ) from None
                except cynapsa.NativeError as exc:
                    LOG.warning("Maps RPC failed: %r", exc, exc_info=True)
                    raise cynapsa.RPCException(
                        502, code="orchestrator_failed", detail="The demo could not answer"
                    ) from None
                except cynapsa.RPCException:
                    raise
                except Exception as exc:
                    LOG.exception("Orchestrator request failed: %r", exc)
                    raise cynapsa.RPCException(
                        502, code="orchestrator_failed", detail="The demo could not answer"
                    ) from None

            print(f"demo-orchestrator ready as {session.agent_id} using model {MODEL}", flush=True)
            await asyncio.Event().wait()
    finally:
        model_client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Cynapsa demo orchestrator")
    parser.add_argument("--enroll", action="store_true")
    parser.add_argument("--force-enroll", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(serve(enroll=args.enroll, force_enroll=args.force_enroll))
    except cynapsa.NativeError as exc:
        LOG.error("Orchestrator connection failed: %r", exc)
        raise
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
