"""Minimal HTTP server exposing a /ping health-check endpoint.

Usage::

    python -m starfix.server          # listens on 0.0.0.0:8080 by default
    python -m starfix.server 9000     # listens on port 9000

The server responds to GET /ping with::

    HTTP 200  Content-Type: application/json
    {"status": "pong"}

All other paths return HTTP 404.
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer

PONG_RESPONSE = json.dumps({"status": "pong"}).encode()


class _PingHandler(BaseHTTPRequestHandler):
    """Request handler that serves GET /ping."""

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/ping":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(PONG_RESPONSE)))
            self.end_headers()
            self.wfile.write(PONG_RESPONSE)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        # Suppress default request logging; callers can add their own.
        pass


def make_server(host: str = "0.0.0.0", port: int = 8080) -> HTTPServer:
    """Return a configured (but not yet started) :class:`HTTPServer`."""
    return HTTPServer((host, port), _PingHandler)


def serve_forever(host: str = "0.0.0.0", port: int = 8080) -> None:
    """Start the server and block until interrupted."""
    server = make_server(host, port)
    server.serve_forever()


if __name__ == "__main__":
    import sys

    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    serve_forever(port=port)
