"""Route inbound canonical HTTP requests to one explicit ASGI app."""

import os

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

import cynapsa


async def health(_: Request) -> JSONResponse:
    return JSONResponse({"ok": True})


app = Starlette(routes=[Route("/health", health, methods=["GET"])])

with cynapsa.login(
    mesh_id=os.environ.get("CYNAPSA_MESH_ID", "mesh-one"),
    profile_id=os.environ.get("CYNAPSA_PROFILE_ID", "default"),
    enrollment_token=os.environ.get("CYNAPSA_ENROLLMENT_TOKEN"),
    address_map={},
    asgi_app=app,
) as bridge:
    print(bridge.status())
