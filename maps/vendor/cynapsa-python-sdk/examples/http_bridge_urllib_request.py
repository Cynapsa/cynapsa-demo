"""Selective synchronous HTTP Bridge use with urllib.request."""

import os
import urllib.request

import cynapsa


with cynapsa.login(
    mesh_id=os.environ.get("CYNAPSA_MESH_ID", "mesh-one"),
    profile_id=os.environ.get("CYNAPSA_PROFILE_ID", "default"),
    enrollment_token=os.environ.get("CYNAPSA_ENROLLMENT_TOKEN"),
    address_map={
        "https://orders.example.test": {
            "recipient": "orders@example.test",
            "mode": "rpc",
        }
    },
):
    request = urllib.request.Request(
        "https://orders.example.test/orders?notify=true",
        data=b'{"sku":"A1"}',
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        print(response.status, response.reason, response.read())
