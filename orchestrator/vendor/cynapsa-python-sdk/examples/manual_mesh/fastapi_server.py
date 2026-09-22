from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


app = FastAPI()


@app.post("/reverse")
async def reverse(request: Request) -> JSONResponse:
    text = (await request.body()).decode("utf-8")
    print(f"fastapi server received: {text!r}", flush=True)
    return JSONResponse([text[::-1], "fastapi"])
