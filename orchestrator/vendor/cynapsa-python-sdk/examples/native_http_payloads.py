"""Native Session with universal request and response models."""

import os

import cynapsa


with cynapsa.connect(
    mesh_id=os.environ.get("CYNAPSA_MESH_ID", "mesh-one"),
    profile_id=os.environ.get("CYNAPSA_PROFILE_ID", "default"),
    enrollment_token=os.environ.get("CYNAPSA_ENROLLMENT_TOKEN"),
) as session:
    @session.on("/orders")
    def orders(request: cynapsa.CynapsaRequest) -> object:
        return cynapsa.CynapsaResponse(
            201,
            "Created",
            (("content-type", "application/json"),),
            b'{"accepted":true}',
        )

    payload = cynapsa.CynapsaRequest(
        "POST",
        "/orders",
        "",
        (("content-type", "application/json"),),
        b'{"sku":"A1"}',
    )
    response = session.request("orders@example.test", payload, ttl_ms=5_000)
    print(response.status_code, response.json())
