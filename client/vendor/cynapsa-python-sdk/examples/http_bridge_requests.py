"""Selective synchronous HTTP Bridge use with Requests."""

import os

import requests

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
    response = requests.post("https://orders.example.test/orders", json={"sku": "A1"})
    response.raise_for_status()
    print(response.json())
