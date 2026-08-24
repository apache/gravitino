# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Process-level IT: live MCP + mock IdP + mock Gravitino.

Helm does not ship an MCP chart in this repo. This is the integration layer
that exists today: real HTTP, real MCP process, no frozen ``--token``.

Proves the service hop:
  MCP --client_credentials--> mock IdP
  MCP --Authorization Bearer--> mock Gravitino
without a hop-1 user header.
"""

# do_GET/do_POST are BaseHTTPRequestHandler names. pytest fixtures share
# names with test parameters.
# pylint: disable=invalid-name,redefined-outer-name

import asyncio
import json
import os
import socket
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import List, Optional
from urllib.parse import parse_qs

import pytest
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport

METALAKE = "oauth_it"
CATALOG = "it_catalog"


class _IdPState:
    def __init__(self, expires_in: int):
        self.expires_in = expires_in
        self.hits = 0
        self.bodies: List[str] = []
        self.lock = threading.Lock()


class _GravitinoState:
    def __init__(self):
        self.authorizations: List[str] = []
        self.fail_first = False
        self.lock = threading.Lock()


def _start_server(handler_cls, state) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    server.state = state
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def _idp_handler(state: _IdPState):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            return

        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length).decode("utf-8")
            with state.lock:
                state.hits += 1
                state.bodies.append(body)
                token = f"tok-{state.hits}"
                expires_in = state.expires_in
            payload = json.dumps(
                {
                    "access_token": token,
                    "token_type": "bearer",
                    "expires_in": expires_in,
                }
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    return Handler


def _gravitino_handler(state: _GravitinoState):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            return

        def do_GET(self):
            auth = self.headers.get("Authorization", "")
            with state.lock:
                state.authorizations.append(auth)
                fail = state.fail_first and auth == "Bearer tok-1"
            if fail:
                payload = json.dumps(
                    {
                        "code": 1,
                        "type": "UnauthorizedException",
                        "message": "expired",
                    }
                ).encode("utf-8")
                self.send_response(401)
            else:
                payload = json.dumps(
                    {
                        "code": 0,
                        "catalogs": [{"name": CATALOG}],
                    }
                ).encode("utf-8")
                self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    return Handler


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_for_port(host: str, port: int, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"{host}:{port} did not open")


def _start_mcp(
    gravitino_uri: str,
    token_endpoint: str,
    mcp_port: int,
    extra_args: Optional[List[str]] = None,
) -> subprocess.Popen:
    mcp_url = f"http://127.0.0.1:{mcp_port}/mcp"
    env = dict(os.environ)
    # A leftover GRAVITINO_TOKEN would win over client-credentials.
    env.pop("GRAVITINO_TOKEN", None)
    env["NO_PROXY"] = "127.0.0.1,localhost"
    env["no_proxy"] = "127.0.0.1,localhost"
    command = [
        sys.executable,
        "-m",
        "mcp_server",
        "--metalake",
        METALAKE,
        "--gravitino-uri",
        gravitino_uri,
        "--transport",
        "http",
        "--mcp-url",
        mcp_url,
        "--oauth-token-endpoint",
        token_endpoint,
        "--oauth-client-id",
        "mcp-it",
        "--oauth-client-secret",
        "it-secret",
        "--oauth-scope",
        "gravitino",
    ]
    if extra_args:
        command.extend(extra_args)
    return subprocess.Popen(
        command,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


@pytest.fixture
def oauth_stack():
    """Mock IdP + mock Gravitino + live MCP process (no --token)."""
    idp_state = _IdPState(expires_in=3600)
    gravitino_state = _GravitinoState()
    idp = _start_server(_idp_handler(idp_state), idp_state)
    gravitino = _start_server(
        _gravitino_handler(gravitino_state), gravitino_state
    )
    mcp_port = _free_port()
    token_endpoint = f"http://127.0.0.1:{idp.server_address[1]}/token"
    gravitino_uri = f"http://127.0.0.1:{gravitino.server_address[1]}"
    proc = _start_mcp(gravitino_uri, token_endpoint, mcp_port)
    try:
        _wait_for_port("127.0.0.1", mcp_port)
        yield {
            "mcp_url": f"http://127.0.0.1:{mcp_port}/mcp",
            "idp": idp_state,
            "gravitino": gravitino_state,
        }
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        idp.shutdown()
        gravitino.shutdown()


def _list_catalogs(mcp_url: str) -> list:
    async def _run():
        transport = StreamableHttpTransport(url=mcp_url)
        async with Client(transport) as client:
            result = await client.call_tool("get_list_of_catalogs")
        return json.loads(result.content[0].text)

    return asyncio.run(_run())


def test_mcp_fetches_token_and_forwards_bearer(oauth_stack):
    """No hop-1 header: MCP must mint a Bearer and send it to Gravitino."""
    catalogs = _list_catalogs(oauth_stack["mcp_url"])
    assert catalogs[0]["name"] == CATALOG

    idp = oauth_stack["idp"]
    gravitino = oauth_stack["gravitino"]
    assert idp.hits == 1
    form = parse_qs(idp.bodies[0])
    assert form["grant_type"] == ["client_credentials"]
    assert form["client_id"] == ["mcp-it"]
    assert form["client_secret"] == ["it-secret"]
    assert form["scope"] == ["gravitino"]
    assert gravitino.authorizations == ["Bearer tok-1"]


def test_mcp_reuses_cached_token_on_second_call(oauth_stack):
    """A long-lived token is fetched once for two tool calls."""
    _list_catalogs(oauth_stack["mcp_url"])
    _list_catalogs(oauth_stack["mcp_url"])
    assert oauth_stack["idp"].hits == 1
    assert oauth_stack["gravitino"].authorizations == [
        "Bearer tok-1",
        "Bearer tok-1",
    ]


def test_mcp_refetches_when_token_is_immediately_stale():
    """expires_in shorter than refresh skew forces a fetch per call."""
    idp_state = _IdPState(expires_in=1)
    gravitino_state = _GravitinoState()
    idp = _start_server(_idp_handler(idp_state), idp_state)
    gravitino = _start_server(
        _gravitino_handler(gravitino_state), gravitino_state
    )
    mcp_port = _free_port()
    proc = _start_mcp(
        f"http://127.0.0.1:{gravitino.server_address[1]}",
        f"http://127.0.0.1:{idp.server_address[1]}/token",
        mcp_port,
    )
    try:
        _wait_for_port("127.0.0.1", mcp_port)
        mcp_url = f"http://127.0.0.1:{mcp_port}/mcp"
        _list_catalogs(mcp_url)
        _list_catalogs(mcp_url)
        assert idp_state.hits == 2
        assert gravitino_state.authorizations == [
            "Bearer tok-1",
            "Bearer tok-2",
        ]
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        idp.shutdown()
        gravitino.shutdown()


def test_mcp_retries_once_after_gravitino_401(oauth_stack):
    """A 401 from Gravitino invalidates the cache and retries with a new token."""
    _list_catalogs(oauth_stack["mcp_url"])
    oauth_stack["gravitino"].fail_first = True
    catalogs = _list_catalogs(oauth_stack["mcp_url"])
    assert catalogs[0]["name"] == CATALOG
    assert oauth_stack["idp"].hits == 2
    assert oauth_stack["gravitino"].authorizations[-2:] == [
        "Bearer tok-1",
        "Bearer tok-2",
    ]


def test_static_token_overrides_oauth_and_skips_idp():
    """``--token`` wins: MCP must not call the IdP."""
    idp_state = _IdPState(expires_in=3600)
    gravitino_state = _GravitinoState()
    idp = _start_server(_idp_handler(idp_state), idp_state)
    gravitino = _start_server(
        _gravitino_handler(gravitino_state), gravitino_state
    )
    mcp_port = _free_port()
    proc = _start_mcp(
        f"http://127.0.0.1:{gravitino.server_address[1]}",
        f"http://127.0.0.1:{idp.server_address[1]}/token",
        mcp_port,
        extra_args=["--token", "frozen"],
    )
    try:
        _wait_for_port("127.0.0.1", mcp_port)
        catalogs = _list_catalogs(f"http://127.0.0.1:{mcp_port}/mcp")
        assert catalogs[0]["name"] == CATALOG
        assert idp_state.hits == 0
        assert gravitino_state.authorizations == ["Bearer frozen"]
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        idp.shutdown()
        gravitino.shutdown()
