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

"""Tests for HTTP transport URL parsing, the streamable-http alias, and TLS wiring."""

import unittest
from unittest.mock import patch

from mcp_server.client.factory import RESTClientFactory
from mcp_server.client.plain.plain_rest_client_operation import (
    PlainRESTClientOperation,
)
from mcp_server.core.setting import Setting
from mcp_server.server import GravitinoMCPServer, _parse_mcp_url
from tests.unit.tools import MockOperation


class TestParseMcpUrl(unittest.TestCase):
    """_parse_mcp_url accepts http and https and rejects other schemes."""

    def test_http_url(self):
        self.assertEqual(
            _parse_mcp_url("http://127.0.0.1:8000/mcp"),
            ("127.0.0.1", 8000, "/mcp"),
        )

    def test_https_url(self):
        self.assertEqual(
            _parse_mcp_url("https://mcphost:9443/mcp"),
            ("mcphost", 9443, "/mcp"),
        )

    def test_https_default_port(self):
        _, port, _ = _parse_mcp_url("https://mcphost/mcp")
        self.assertEqual(port, 443)

    def test_http_default_port(self):
        _, port, _ = _parse_mcp_url("http://mcphost/mcp")
        self.assertEqual(port, 80)

    def test_unsupported_scheme_rejected(self):
        with self.assertRaises(ValueError):
            _parse_mcp_url("ftp://mcphost/mcp")


class TestRunHttpTransport(unittest.TestCase):
    """GravitinoMCPServer.run() wires transport name and TLS config correctly."""

    def setUp(self):
        RESTClientFactory.set_rest_client(MockOperation)

    def tearDown(self):
        # Restore the default so this global mutation can't leak into other tests.
        RESTClientFactory.set_rest_client(PlainRESTClientOperation)

    def _run_and_capture(self, setting: Setting) -> dict:
        """Run the server with run_async patched; return the kwargs it was called with."""
        server = GravitinoMCPServer(setting)
        captured = {}

        async def fake_run_async(**kwargs):
            captured.update(kwargs)

        with patch.object(server.mcp, "run_async", side_effect=fake_run_async):
            server.run()
        return captured

    def test_http_transport(self):
        setting = Setting(
            metalake="ml",
            transport="http",
            mcp_url="http://127.0.0.1:8000/mcp",
        )
        kwargs = self._run_and_capture(setting)
        self.assertEqual(kwargs["transport"], "http")
        self.assertEqual(kwargs["host"], "127.0.0.1")
        self.assertEqual(kwargs["port"], 8000)
        self.assertEqual(kwargs["path"], "/mcp")
        self.assertNotIn("uvicorn_config", kwargs)

    def test_streamable_http_alias(self):
        setting = Setting(
            metalake="ml",
            transport="streamable-http",
            mcp_url="http://127.0.0.1:8000/mcp",
        )
        kwargs = self._run_and_capture(setting)
        self.assertEqual(kwargs["transport"], "streamable-http")

    def test_tls_config_wired_when_cert_and_key_set(self):
        setting = Setting(
            metalake="ml",
            transport="streamable-http",
            mcp_url="https://127.0.0.1:8443/mcp",
            tls_cert="/path/to/cert.pem",
            tls_key="/path/to/key.pem",
        )
        kwargs = self._run_and_capture(setting)
        self.assertEqual(
            kwargs["uvicorn_config"],
            {
                "ssl_certfile": "/path/to/cert.pem",
                "ssl_keyfile": "/path/to/key.pem",
            },
        )

    def test_lone_cert_rejected(self):
        """TLS requires both cert and key; a lone cert is rejected."""
        setting = Setting(
            metalake="ml",
            transport="http",
            mcp_url="https://127.0.0.1:8443/mcp",
            tls_cert="/path/to/cert.pem",
            tls_key="",
        )
        with self.assertRaises(ValueError):
            self._run_and_capture(setting)

    def test_https_url_without_tls_rejected(self):
        """An https URL without cert/key must not silently serve plain HTTP."""
        setting = Setting(
            metalake="ml",
            transport="http",
            mcp_url="https://127.0.0.1:8443/mcp",
        )
        with self.assertRaises(ValueError):
            self._run_and_capture(setting)

    def test_http_url_with_tls_rejected(self):
        """TLS configured behind an http URL is a misconfiguration and rejected."""
        setting = Setting(
            metalake="ml",
            transport="http",
            mcp_url="http://127.0.0.1:8000/mcp",
            tls_cert="/path/to/cert.pem",
            tls_key="/path/to/key.pem",
        )
        with self.assertRaises(ValueError):
            self._run_and_capture(setting)
