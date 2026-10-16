"""The socketless Cloud Run ingress must never expose the Maps application."""

import http.client
import importlib.util
from pathlib import Path
import threading


MODULE = Path(__file__).resolve().parents[1] / "maps/cloud_run_listener.py"
spec = importlib.util.spec_from_file_location("cloud_run_listener", MODULE)
listener = importlib.util.module_from_spec(spec)
spec.loader.exec_module(listener)


def test_listener_serves_only_health():
    server = listener.ThreadingHTTPServer(("127.0.0.1", 0), listener.Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        connection = http.client.HTTPConnection("127.0.0.1", server.server_port)
        connection.request("GET", "/health")
        response = connection.getresponse()
        assert response.status == 200
        response.read()
        connection.request("POST", "/maps", body=b'{}')
        response = connection.getresponse()
        assert response.status == 404
        response.read()
        connection.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
