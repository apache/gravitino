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

"""Tests for per-request token isolation (Task 6).

GravitinoContext.rest_client() must return a client carrying the token from
the current HTTP request, not the shared startup token, when an Authorization
header is present.  This ensures concurrent multi-principal sessions are fully
isolated in HTTP transport mode.
"""

import unittest
from unittest.mock import MagicMock, patch

from mcp_server.core.context import (
    GravitinoContext,
    _extract_bearer_token,
    _get_request_token,
)
from mcp_server.core.setting import Setting


class TestExtractBearerToken(unittest.TestCase):
    """Unit tests for the token extraction helper."""

    def test_well_formed_bearer_header(self):
        self.assertEqual(_extract_bearer_token("Bearer mytoken123"), "mytoken123")

    def test_case_insensitive_bearer(self):
        self.assertEqual(_extract_bearer_token("bearer MYTOKEN"), "MYTOKEN")

    def test_empty_header_returns_empty(self):
        self.assertEqual(_extract_bearer_token(""), "")

    def test_non_bearer_scheme_returns_empty(self):
        self.assertEqual(_extract_bearer_token("Basic dXNlcjpwYXNz"), "")

    def test_only_scheme_no_token_returns_empty(self):
        self.assertEqual(_extract_bearer_token("Bearer"), "")


class TestGetRequestToken(unittest.TestCase):
    """Unit tests for _get_request_token() (HTTP context extraction)."""

    def test_returns_token_when_http_request_available(self):
        mock_request = MagicMock()
        mock_request.headers.get.return_value = "Bearer request-token-xyz"

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=mock_request,
        ):
            token = _get_request_token()

        self.assertEqual(token, "request-token-xyz")

    def test_returns_empty_when_no_http_context(self):
        """Simulates stdio mode where get_http_request raises LookupError."""
        with patch(
            "fastmcp.server.dependencies.get_http_request",
            side_effect=LookupError("no request context"),
        ):
            token = _get_request_token()

        self.assertEqual(token, "")

    def test_returns_empty_when_no_authorization_header(self):
        mock_request = MagicMock()
        mock_request.headers.get.return_value = ""

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=mock_request,
        ):
            token = _get_request_token()

        self.assertEqual(token, "")


class TestGravitinoContextPerRequestToken(unittest.TestCase):
    """GravitinoContext.rest_client() isolates per-request tokens."""

    def _make_context(self, startup_token: str = "") -> GravitinoContext:
        return GravitinoContext(
            Setting(
                metalake="ml",
                gravitino_uri="http://localhost:8090",
                token=startup_token,
            )
        )

    def test_per_request_token_overrides_startup_token(self):
        """When an HTTP request carries a token, it takes priority over the startup token."""
        ctx = self._make_context(startup_token="startup-token")

        mock_request = MagicMock()
        mock_request.headers.get.return_value = "Bearer request-token"

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=mock_request,
        ):
            client = ctx.rest_client()

        headers = dict(client._catalog_operation.rest_client.headers)
        self.assertEqual(headers.get("authorization"), "Bearer request-token")

    def test_falls_back_to_default_client_when_no_request_token(self):
        """When no per-request token exists, the shared default client (startup token) is used."""
        ctx = self._make_context(startup_token="startup-token")

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            side_effect=LookupError,
        ):
            client = ctx.rest_client()

        # Must be the exact same object as the cached default client.
        self.assertIs(client, ctx._default_client)

    def test_two_concurrent_requests_get_different_clients(self):
        """Different request tokens must produce different client instances."""
        ctx = self._make_context()

        def make_mock(token: str) -> MagicMock:
            m = MagicMock()
            m.headers.get.return_value = f"Bearer {token}"
            return m

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=make_mock("alice-token"),
        ):
            client_alice = ctx.rest_client()

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=make_mock("bob-token"),
        ):
            client_bob = ctx.rest_client()

        alice_headers = dict(client_alice._catalog_operation.rest_client.headers)
        bob_headers = dict(client_bob._catalog_operation.rest_client.headers)
        self.assertEqual(alice_headers.get("authorization"), "Bearer alice-token")
        self.assertEqual(bob_headers.get("authorization"), "Bearer bob-token")
        self.assertIsNot(client_alice, client_bob)
