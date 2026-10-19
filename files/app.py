"""Ordinary HTTP files agent; Cynapsa is supplied by the process launcher."""

from __future__ import annotations

import json
import logging
import os
import re
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from starlette.concurrency import run_in_threadpool

from llm import ModelClient, ModelError
from runtime import litellm_key
from tools import FileDatabase
from workflow import answer_question


LOG = logging.getLogger("demo.files")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    for name in ("httpx", "httpcore", "urllib3", "requests"):
        logging.getLogger(name).setLevel(logging.WARNING)
    database = FileDatabase(os.environ["DEMO_FILES_DIRECTORY"])
    model = ModelClient(
        litellm_key(),
        base_url=os.environ.get("LITELLM_BASE_URL", "https://litellm.eladrave.com"),
        model=os.environ.get("LITELLM_MODEL", "chatgpt/gpt-5.6-terra-high"),
    )
    app.state.database = database
    app.state.model = model
    LOG.info("demo-files ready on /files")
    try:
        yield
    finally:
        model.close()


app = FastAPI(lifespan=lifespan)


@app.post("/files")
async def files(request: Request) -> dict:
    try:
        data = await request.json()
        question = data.get("question") if isinstance(data, dict) else None
        if not isinstance(question, str) or not 1 <= len(question.strip()) <= 2_000:
            raise ValueError("invalid question")
    except (ValueError, UnicodeError):
        raise HTTPException(
            status_code=400, detail="Expected a question of 1–2000 characters"
        ) from None

    question = question.strip()
    sender = getattr(request.state, "cynapsa_sender", None)
    supplied_id = request.headers.get("x-demo-request-id", "")
    request_id = supplied_id if re.fullmatch(r"[0-9a-f]{8}", supplied_id) else uuid.uuid4().hex[:8]
    transport = "cynapsa" if sender or os.environ.get("DEMO_TRANSPORT") == "cynapsa" else "http"
    label = "orchestrator" if sender and sender in {
        os.environ.get("DEMO_ORCHESTRATOR_AGENT_ID"), "http://demo-orchestrator.local"
    } else sender
    peer = f"{label} ({sender})" if label == "orchestrator" else (label or (request.client.host if request.client else "unknown"))
    LOG.info('[%s] request received from "%s" id=%s: %s', transport, peer, request_id,
             json.dumps({"question": question}, ensure_ascii=False))
    started = time.monotonic()
    try:
        result = await run_in_threadpool(
            answer_question, question, request.app.state.model, request.app.state.database
        )
    except ModelError as exc:
        LOG.warning("Files model request failed with status %s", exc.status_code)
        raise HTTPException(status_code=503, detail="The model gateway is unavailable") from None
    except Exception:
        LOG.exception("Files request failed")
        raise HTTPException(status_code=502, detail="The files agent could not answer") from None
    LOG.info('[%s] response sent to "%s" id=%s duration_ms=%d: %s',
             transport, peer, request_id, round((time.monotonic() - started) * 1000),
             json.dumps(result, ensure_ascii=False))
    return result
