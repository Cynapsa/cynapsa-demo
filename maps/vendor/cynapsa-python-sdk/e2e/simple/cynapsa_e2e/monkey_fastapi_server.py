from __future__ import annotations

import json

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()


@app.post("/reverse")
async def reverse(request: Request) -> JSONResponse:
    body = await request.body()
    evidence = {
        "event": "asgi-dispatch",
        "path": request.url.path,
        "body_bytes": len(body),
    }
    print("E2E_DIAGNOSTIC=" + json.dumps(evidence, sort_keys=True), flush=True)
    text = body.decode("utf-8", errors="strict")
    return JSONResponse([text[::-1], "monkey"])


@app.post("/rate-limit")
async def rate_limit(request: Request) -> JSONResponse:
    body = await request.body()
    print(
        "E2E_DIAGNOSTIC="
        + json.dumps({"event": "hop-upstream", "body": body.decode("utf-8")}),
        flush=True,
    )
    return JSONResponse(
        {"error": "quota exceeded"},
        status_code=429,
        headers={"retry-after": "7", "x-upstream": "maps"},
    )
