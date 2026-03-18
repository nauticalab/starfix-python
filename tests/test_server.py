"""Tests for the starfix ping server."""

from __future__ import annotations

import json
import threading
import urllib.request

import pytest

from starfix.server import make_server


@pytest.fixture()
def ping_server():
    """Start a temporary ping server on a free port and yield its base URL."""
    server = make_server(host="127.0.0.1", port=0)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()


def test_ping_returns_200(ping_server: str) -> None:
    with urllib.request.urlopen(f"{ping_server}/ping") as resp:
        assert resp.status == 200


def test_ping_content_type_is_json(ping_server: str) -> None:
    with urllib.request.urlopen(f"{ping_server}/ping") as resp:
        content_type = resp.headers.get("Content-Type", "")
        assert "application/json" in content_type


def test_ping_body_is_pong(ping_server: str) -> None:
    with urllib.request.urlopen(f"{ping_server}/ping") as resp:
        body = json.loads(resp.read())
    assert body == {"status": "pong"}


def test_unknown_path_returns_404(ping_server: str) -> None:
    try:
        urllib.request.urlopen(f"{ping_server}/unknown")
        pytest.fail("Expected HTTPError for 404")
    except urllib.error.HTTPError as exc:
        assert exc.code == 404
