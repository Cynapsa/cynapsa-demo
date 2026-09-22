"""Synchronous native messaging."""

import os

import cynapsa


with cynapsa.connect(
    mesh_id=os.environ.get("CYNAPSA_MESH_ID", "mesh-one"),
    profile_id=os.environ.get("CYNAPSA_PROFILE_ID", "default"),
    enrollment_token=os.environ.get("CYNAPSA_ENROLLMENT_TOKEN"),
) as session:
    @session.on("/math/add")
    def add(request: cynapsa.CynapsaRequest) -> object:
        values = request.json()
        return {"sum": values["a"] + values["b"]}

    @session.on("/audit/ready")
    def ready(request: cynapsa.CynapsaRequest) -> None:
        print(request.json())

    accepted = session.send("worker@example.test", {"ready": True}, path="/audit/ready")
    print(accepted.message_id)
    response = session.request(
        "worker@example.test", {"a": 2, "b": 3}, path="/math/add", ttl_ms=5_000
    )
    print(response.json())
