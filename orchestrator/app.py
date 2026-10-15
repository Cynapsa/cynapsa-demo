"""Ordinary HTTP orchestrator; Cynapsa is supplied by the process launcher."""

from __future__ import annotations

import json
import logging
import os
import stat
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from pydantic import BaseModel, Field, ValidationError

from llm import ModelClient, ModelError
from runtime import litellm_key
from workflow import ConversationAgent, thread_key


LOG = logging.getLogger(__name__)
MODEL = os.environ.get("LITELLM_MODEL", "chatgpt/gpt-5.6-terra-high")
BASE_URL = os.environ.get("LITELLM_BASE_URL", "https://litellm.eladrave.com")
MAPS_ORIGIN = os.environ.get("DEMO_MAPS_ORIGIN", "http://demo-maps.local").rstrip("/")
FILES_ORIGIN = os.environ.get("DEMO_FILES_ORIGIN", "").rstrip("/")


class AskBody(BaseModel):
    prompt: str = Field(min_length=1, max_length=2_000)
    conversation_id: str | None = Field(default=None, pattern=r"^[A-Za-z0-9_-]{1,64}$")


def _memory_path() -> Path:
    directory = Path(os.environ.get(
        "DEMO_MEMORY_DIRECTORY", str(Path(__file__).resolve().parent / ".memory")
    ))
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = directory.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) & 0o077):
        raise RuntimeError("memory directory must be owned by the current user with mode 0700")
    path = directory / "conversations.sqlite"
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        os.fchmod(fd, 0o600)
    finally:
        os.close(fd)
    return path


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=logging.INFO)
    for name in ("httpx", "httpcore", "urllib3", "requests"):
        logging.getLogger(name).setLevel(logging.WARNING)
    model = ModelClient(litellm_key(), base_url=BASE_URL, model=MODEL)
    try:
        async with AsyncSqliteSaver.from_conn_string(str(_memory_path())) as checkpointer:
            async with httpx.AsyncClient(timeout=100.0) as downstream:
                async def ask_remote(origin: str, path: str, question: str) -> dict[str, Any]:
                    name = "maps" if path == "/maps" else "files"
                    identity = os.environ.get(f"DEMO_{name.upper()}_AGENT_ID", origin)
                    LOG.info('[cynapsa] message sent to "%s" (%s): %s',
                             name, identity, json.dumps({"question": question}, ensure_ascii=False))
                    response = await downstream.post(origin + path, json={"question": question})
                    LOG.info('[cynapsa] response received from "%s" (%s): %s',
                             name, identity, response.text)
                    response.raise_for_status()
                    try:
                        result = response.json()
                    except (ValueError, UnicodeError):
                        raise RuntimeError("downstream agent returned invalid JSON") from None
                    if not isinstance(result, dict):
                        raise RuntimeError("downstream agent returned an invalid response")
                    return result

                async def ask_maps(question: str) -> dict[str, Any]:
                    return await ask_remote(MAPS_ORIGIN, "/maps", question)

                async def ask_files(question: str) -> dict[str, Any]:
                    return await ask_remote(FILES_ORIGIN, "/files", question)

                app.state.agent = ConversationAgent(
                    model, ask_maps, checkpointer,
                    ask_files=ask_files if FILES_ORIGIN else None,
                )
                LOG.info("demo-orchestrator HTTP app ready using model %s", MODEL)
                yield
    finally:
        model.close()


app = FastAPI(lifespan=lifespan)


@app.post("/ask")
async def ask(request: Request) -> dict[str, Any]:
    try:
        body = AskBody.model_validate(await request.json())
        if not body.prompt.strip():
            raise ValueError("empty prompt")
        conversation_id = body.conversation_id or uuid.uuid4().hex
        # The socketless HTTP bridge supplies the authenticated peer in ASGI
        # state. Ordinary HTTP retains its client address; production HTTP
        # deployments would authenticate callers rather than trust an IP.
        client = getattr(request.state, "cynapsa_sender", None)
        if not client:
            client = request.client.host if request.client else "http-client"
        key = thread_key("demo-orchestrator", os.environ.get("DEMO_MESH_ID", "http").strip(),
                         client, conversation_id)
    except (ValueError, UnicodeError, ValidationError):
        raise HTTPException(
            400, detail={"code": "bad_request", "message": "Expected a prompt and a valid conversation ID"}
        ) from None

    sender = getattr(request.state, "cynapsa_sender", None)
    label = "client" if sender and sender == os.environ.get("DEMO_CLIENT_AGENT_ID") else sender
    LOG.info('[%s] message received from "%s": %s',
             "cynapsa" if sender else "http", label or (request.client.host if request.client else "unknown"),
             json.dumps({"prompt": body.prompt, "conversation_id": conversation_id}, ensure_ascii=False))
    try:
        answer = await request.app.state.agent.answer(body.prompt.strip(), key)
    except (TimeoutError, httpx.TimeoutException) as exc:
        LOG.warning("Downstream request timed out: %r", exc, exc_info=True)
        raise HTTPException(504, detail={"code": "agent_timeout", "message": "The downstream agent timed out"}) from None
    except ModelError as exc:
        LOG.warning("Model gateway request failed with status %s: %r",
                    exc.status_code, exc, exc_info=True)
        raise HTTPException(503, detail={"code": "model_unavailable", "message": "The model gateway is unavailable"}) from None
    except Exception as exc:
        LOG.exception("Orchestrator request failed: %r", exc)
        raise HTTPException(502, detail={"code": "orchestrator_failed", "message": "The demo could not answer"}) from None
    result = {**answer, "conversation_id": conversation_id}
    LOG.info('[%s] response returned to "%s": %s', "cynapsa" if sender else "http",
             label or (request.client.host if request.client else "unknown"), json.dumps(result, ensure_ascii=False))
    return result
