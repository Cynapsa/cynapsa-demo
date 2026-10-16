"""Cloud Run ingress while the Maps app is running socketless via Cynapsa.

This deliberately does not expose the FastAPI routes over HTTP. The same
container image serves those routes when DEMO_RUN_COMMAND selects Uvicorn.
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok\n")
            return
        self.send_error(404)

    def do_POST(self) -> None:
        self.send_error(404)

    def log_message(self, _format: str, *_args: object) -> None:
        pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
