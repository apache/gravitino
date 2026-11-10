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

"""Tests for per-request identity isolation (Task 6).

GravitinoContext.rest_client() must forward the current HTTP request's raw
Authorization header (any scheme) to Gravitino, not the shared startup token,
so concurrent multi-principal sessions stay fully isolated in HTTP mode.
"""

import asyncio
import unittest
from unittest.mock import MagicMock, patch

from mcp_server.client.factory import RESTClientFactory
from mcp_server.client.plain.plain_rest_client_operation import (
    PlainRESTClientOperation,
)
from mcp_server.core import context as context_module
from mcp_server.core.context import (
    GravitinoContext,
    _get_request_authorization,
)
from mcp_server.core.setting import Setting

# Tests intentionally exercise context/client internals (e.g. _default_client,
# _catalog_operation) to assert per-request isolation; protected access is expected.
# pylint: disable=protected-access


class TestGetRequestAuthorization(unittest.TestCase):
    """Unit tests for _get_request_authorization() (HTTP context extraction)."""

    def test_returns_raw_bearer_header(self):
        mock_request = MagicMock()
        mock_request.headers.get.return_value = "Bearer request-token-xyz"

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=mock_request,
        ):
            authorization = _get_request_authorization()

        self.assertEqual(authorization, "Bearer request-token-xyz")

    def test_returns_raw_basic_header_verbatim(self):
        """Basic (simple auth) headers must pass through unchanged, not be dropped."""
        mock_request = MagicMock()
        mock_request.headers.get.return_value = "Basic YWxpY2U6ZHVtbXk="

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=mock_request,
        ):
            authorization = _get_request_authorization()

        self.assertEqual(authorization, "Basic YWxpY2U6ZHVtbXk=")

    def test_returns_empty_when_no_http_context(self):
        """Simulates stdio mode where get_http_request raises LookupError."""
        with patch(
            "fastmcp.server.dependencies.get_http_request",
            side_effect=LookupError("no request context"),
        ):
            authorization = _get_request_authorization()

        self.assertEqual(authorization, "")

    def test_returns_empty_when_no_authorization_header(self):
        mock_request = MagicMock()
        mock_request.headers.get.return_value = ""

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=mock_request,
        ):
            authorization = _get_request_authorization()

        self.assertEqual(authorization, "")


class TestGravitinoContextPerRequestAuthorization(unittest.TestCase):
    """GravitinoContext.rest_client() isolates per-request identities."""

    def setUp(self):
        # These tests inspect the real PlainRESTClientOperation; other test
        # modules swap the factory to MockOperation without restoring it, so pin
        # the real client here to stay order-independent.
        RESTClientFactory.set_rest_client(PlainRESTClientOperation)

    def _make_context(self, startup_token: str = "") -> GravitinoContext:
        return GravitinoContext(
            Setting(
                metalake="ml",
                gravitino_uri="http://localhost:8090",
                token=startup_token,
            )
        )

    def test_per_request_header_overrides_startup_token(self):
        """An HTTP request's Authorization header takes priority over the startup token."""
        ctx = self._make_context(startup_token="startup-token")

        mock_request = MagicMock()
        mock_request.headers.get.return_value = "Basic YWxpY2U6ZHVtbXk="

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=mock_request,
        ):
            client = ctx.rest_client()

        headers = dict(client._catalog_operation.rest_client.headers)
        self.assertEqual(headers.get("authorization"), "Basic YWxpY2U6ZHVtbXk=")

    def test_falls_back_to_default_client_when_no_request_header(self):
        """With no per-request header, the shared default client (startup token) is used."""
        ctx = self._make_context(startup_token="startup-token")

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            side_effect=LookupError,
        ):
            client = ctx.rest_client()

        # Must be the exact same object as the cached default client.
        self.assertIs(client, ctx._default_client)

    def test_two_concurrent_requests_get_different_clients(self):
        """Different request identities must produce different client instances."""
        ctx = self._make_context()

        def make_mock(authorization: str) -> MagicMock:
            m = MagicMock()
            m.headers.get.return_value = authorization
            return m

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=make_mock("Basic YWxpY2U6ZHVtbXk="),
        ):
            client_alice = ctx.rest_client()

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=make_mock("Basic Ym9iOmR1bW15"),
        ):
            client_bob = ctx.rest_client()

        alice_headers = dict(
            client_alice._catalog_operation.rest_client.headers
        )
        bob_headers = dict(client_bob._catalog_operation.rest_client.headers)
        self.assertEqual(
            alice_headers.get("authorization"), "Basic YWxpY2U6ZHVtbXk="
        )
        self.assertEqual(bob_headers.get("authorization"), "Basic Ym9iOmR1bW15")
        self.assertIsNot(client_alice, client_bob)

    def test_same_principal_reuses_cached_client(self):
        """Repeated calls from the same principal reuse one cached client."""
        ctx = self._make_context()

        mock_request = MagicMock()
        mock_request.headers.get.return_value = "Basic YWxpY2U6ZHVtbXk="

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=mock_request,
        ):
            first = ctx.rest_client()
            second = ctx.rest_client()

        # Same Authorization header -> same client object (connection pool reused).
        self.assertIs(first, second)

    def test_client_cache_is_bounded(self):
        """The per-principal client cache evicts the oldest entries past its cap."""
        ctx = self._make_context()
        cap = context_module._MAX_CACHED_CLIENTS

        for i in range(cap + 5):
            mock_request = MagicMock()
            mock_request.headers.get.return_value = f"Bearer token-{i}"
            with patch(
                "fastmcp.server.dependencies.get_http_request",
                return_value=mock_request,
            ):
                ctx.rest_client()

        self.assertLessEqual(len(ctx._clients_by_auth), cap)

    def test_evicted_client_is_closed(self):
        """On eviction (with a running loop) the evicted client's pool is closed."""
        closed = []

        class _ClosableClient:
            def __init__(self, *_args, **_kwargs):
                pass

            async def close(self):
                closed.append(self)

        RESTClientFactory.set_rest_client(_ClosableClient)
        try:
            ctx = self._make_context()
            cap = context_module._MAX_CACHED_CLIENTS

            async def _drive():
                for i in range(cap + 1):
                    mock_request = MagicMock()
                    mock_request.headers.get.return_value = f"Bearer t-{i}"
                    with patch(
                        "fastmcp.server.dependencies.get_http_request",
                        return_value=mock_request,
                    ):
                        ctx.rest_client()
                # Let the scheduled close task run to completion.
                await asyncio.sleep(0)
                await asyncio.gather(*ctx._pending_closes)

            asyncio.run(_drive())

            # Exactly one client (the oldest) was evicted and closed.
            self.assertEqual(len(closed), 1)
            self.assertEqual(len(ctx._pending_closes), 0)
        finally:
            RESTClientFactory.set_rest_client(PlainRESTClientOperation)
