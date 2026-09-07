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

"""Tests for per-request metalake resolution.

GravitinoContext.rest_client() must resolve the metalake from the current HTTP
request's X-Gravitino-Metalake header, falling back to the configured startup
default, so a single server instance can serve more than one metalake without
any per-connection/session state (multi-node safe by construction).
"""

import asyncio
import sys
import unittest
from unittest import mock
from unittest.mock import MagicMock, patch

from mcp_server.client.factory import RESTClientFactory
from mcp_server.client.plain.plain_rest_client_operation import (
    PlainRESTClientOperation,
)
from mcp_server.core import context as context_module
from mcp_server.core.context import (
    METALAKE_HEADER,
    GravitinoContext,
    ServiceIdentityFallbackDisabled,
    _get_request_metalake,
)
from mcp_server.core.setting import Setting
from mcp_server.main import _parse_args, do_main

# Tests intentionally exercise context/client internals (e.g. _default_client,
# _service_clients, _catalog_operation) to assert per-request isolation;
# protected access is expected.
# pylint: disable=protected-access


def _mock_request(headers: dict) -> MagicMock:
    """A fake HTTP request whose headers.get() only knows the given keys."""
    mock_request = MagicMock()
    mock_request.headers.get.side_effect = lambda key, default="": headers.get(
        key, default
    )
    return mock_request


class TestGetRequestMetalake(unittest.TestCase):
    """Unit tests for _get_request_metalake() (HTTP context extraction)."""

    def test_returns_header_value(self):
        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=_mock_request({METALAKE_HEADER: "ml_a"}),
        ):
            self.assertEqual(_get_request_metalake(), "ml_a")

    def test_returns_empty_when_no_http_context(self):
        """Simulates stdio mode where get_http_request raises RuntimeError."""
        with patch(
            "fastmcp.server.dependencies.get_http_request",
            side_effect=RuntimeError("no request context"),
        ):
            self.assertEqual(_get_request_metalake(), "")

    def test_returns_empty_when_header_absent(self):
        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=_mock_request({}),
        ):
            self.assertEqual(_get_request_metalake(), "")

    def test_whitespace_only_header_is_treated_as_absent(self):
        """A whitespace-only header must not be mistaken for a real metalake name."""
        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=_mock_request({METALAKE_HEADER: "   "}),
        ):
            self.assertEqual(_get_request_metalake(), "")


class TestGravitinoContextPerRequestMetalake(unittest.TestCase):
    """GravitinoContext.rest_client() isolates per-request metalakes."""

    def setUp(self):
        RESTClientFactory.set_rest_client(PlainRESTClientOperation)

    def _make_context(self, metalake: str = "ml_default") -> GravitinoContext:
        return GravitinoContext(
            Setting(
                metalake=metalake,
                gravitino_uri="http://localhost:8090",
                transport="http",
            )
        )

    def test_header_overrides_startup_default(self):
        ctx = self._make_context()

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=_mock_request(
                {
                    "authorization": "Bearer t",
                    METALAKE_HEADER: "ml_other",
                }
            ),
        ):
            client = ctx.rest_client()

        self.assertEqual(client._catalog_operation.metalake_name, "ml_other")

    def test_falls_back_to_startup_default_when_header_absent(self):
        ctx = self._make_context()

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            side_effect=LookupError,
        ):
            client = ctx.rest_client()

        self.assertIs(client, ctx._default_client)
        self.assertEqual(client._catalog_operation.metalake_name, "ml_default")

    def test_missing_metalake_raises(self):
        """No startup default and no header -> explicit error, not a silent guess."""
        ctx = self._make_context(metalake="")

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            side_effect=LookupError,
        ):
            with self.assertRaises(ValueError):
                ctx.rest_client()

    def test_whitespace_only_header_falls_back_to_startup_default(self):
        """A whitespace-only header must not be used as a literal metalake name."""
        ctx = self._make_context()

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=_mock_request(
                {"authorization": "Bearer t", METALAKE_HEADER: "   "}
            ),
        ):
            client = ctx.rest_client()

        self.assertEqual(client._catalog_operation.metalake_name, "ml_default")

    def test_missing_metalake_error_takes_priority_over_fallback_disabled(self):
        """When both a missing metalake and a disabled service-identity fallback
        apply, the caller must see the fixable "no metalake" error (ValueError),
        not ServiceIdentityFallbackDisabled - metalake resolution runs first."""
        ctx = GravitinoContext(
            Setting(
                metalake="",
                gravitino_uri="http://localhost:8090",
                transport="http",
                token="static-token",
                no_service_identity_fallback=True,
            )
        )

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=_mock_request({}),
        ):
            with self.assertRaises(ValueError) as raised:
                ctx.rest_client()

        self.assertNotIsInstance(
            raised.exception, ServiceIdentityFallbackDisabled
        )

    def test_two_concurrent_requests_get_different_metalake_clients(self):
        """Different requested metalakes must produce different client instances,
        even for the same caller identity. This is the concurrency guarantee the
        header-based design relies on instead of any per-connection session state.
        """
        ctx = self._make_context()

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=_mock_request(
                {"authorization": "Bearer t", METALAKE_HEADER: "ml_a"}
            ),
        ):
            client_a = ctx.rest_client()

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=_mock_request(
                {"authorization": "Bearer t", METALAKE_HEADER: "ml_b"}
            ),
        ):
            client_b = ctx.rest_client()

        self.assertEqual(client_a._catalog_operation.metalake_name, "ml_a")
        self.assertEqual(client_b._catalog_operation.metalake_name, "ml_b")
        self.assertIsNot(client_a, client_b)

    def test_same_identity_and_metalake_reuses_cached_client(self):
        ctx = self._make_context()

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=_mock_request(
                {"authorization": "Bearer t", METALAKE_HEADER: "ml_a"}
            ),
        ):
            first = ctx.rest_client()
            second = ctx.rest_client()

        self.assertIs(first, second)

    def test_non_default_metalake_without_authorization_uses_service_identity(
        self,
    ):
        """No per-request Authorization but a non-default metalake: falls back to
        the startup service identity (static token / OAuth), scoped to that
        metalake, not the shared _default_client bound to the startup default.
        """
        ctx = self._make_context()

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=_mock_request({METALAKE_HEADER: "ml_other"}),
        ):
            client = ctx.rest_client()

        self.assertIsNot(client, ctx._default_client)
        self.assertEqual(client._catalog_operation.metalake_name, "ml_other")

    def test_service_identity_client_is_cached_per_metalake(self):
        ctx = self._make_context()

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=_mock_request({METALAKE_HEADER: "ml_other"}),
        ):
            first = ctx.rest_client()
            second = ctx.rest_client()

        self.assertIs(first, second)
        self.assertEqual(len(ctx._service_clients), 1)

    def test_service_client_cache_is_bounded(self):
        """The metalake-keyed service-identity cache evicts past its cap too,
        mirroring _clients_by_auth's bound (test_client_cache_is_bounded in
        test_per_request_token.py) - it must not be exempt from the same LRU
        limit just because it's the newer of the two caches."""
        ctx = self._make_context()
        cap = context_module._MAX_CACHED_CLIENTS

        for i in range(cap + 5):
            with patch(
                "fastmcp.server.dependencies.get_http_request",
                return_value=_mock_request({METALAKE_HEADER: f"ml_{i}"}),
            ):
                ctx.rest_client()

        self.assertLessEqual(len(ctx._service_clients), cap)

    def test_evicted_service_client_is_closed(self):
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
                    with patch(
                        "fastmcp.server.dependencies.get_http_request",
                        return_value=_mock_request(
                            {METALAKE_HEADER: f"ml_{i}"}
                        ),
                    ):
                        ctx.rest_client()
                await asyncio.sleep(0)
                await asyncio.gather(*ctx._pending_closes)

            asyncio.run(_drive())

            self.assertEqual(len(closed), 1)
            self.assertEqual(len(ctx._pending_closes), 0)
        finally:
            RESTClientFactory.set_rest_client(PlainRESTClientOperation)

    def test_stdio_mode_always_uses_startup_default(self):
        """No HTTP request context at all (stdio): the header path never fires."""
        ctx = self._make_context()

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            side_effect=RuntimeError,
        ):
            client = ctx.rest_client()

        self.assertIs(client, ctx._default_client)


class TestSettingValidateMetalake(unittest.TestCase):
    def test_stdio_without_metalake_is_rejected(self):
        setting = Setting(metalake="", transport="stdio")
        with self.assertRaises(ValueError):
            setting.validate_metalake()

    def test_stdio_with_metalake_is_accepted(self):
        Setting(metalake="ml", transport="stdio").validate_metalake()

    def test_http_without_metalake_is_accepted(self):
        Setting(metalake="", transport="http").validate_metalake()

    def test_whitespace_only_metalake_is_stripped_to_empty(self):
        """A shell-quoting mistake like --metalake "  " must not be treated as
        a configured default - it collapses to the same "unconfigured" state
        as an empty string."""
        self.assertEqual(Setting(metalake="  ").metalake, "")

    def test_whitespace_only_metalake_is_rejected_for_stdio(self):
        setting = Setting(metalake="   ", transport="stdio")
        with self.assertRaises(ValueError):
            setting.validate_metalake()


class TestGravitinoContextValidatesSettingAtConstruction(unittest.TestCase):
    """GravitinoContext.__init__ must fail fast, independent of do_main()."""

    def setUp(self):
        RESTClientFactory.set_rest_client(PlainRESTClientOperation)

    def test_stdio_without_metalake_raises_at_construction(self):
        with self.assertRaises(ValueError):
            GravitinoContext(Setting(metalake="", transport="stdio"))

    def test_partial_oauth_raises_at_construction(self):
        with self.assertRaises(ValueError):
            GravitinoContext(
                Setting(
                    metalake="ml",
                    oauth_client_id="mcp",
                    oauth_client_secret="s",
                )
            )


class TestMetalakeArgParsing(unittest.TestCase):
    def test_metalake_is_optional_and_defaults_to_empty(self):
        with mock.patch.object(sys, "argv", ["mcp_server"]):
            args = _parse_args()
        self.assertEqual(args.metalake, "")


class TestMainMetalakeValidation(unittest.TestCase):
    def test_stdio_without_metalake_inits_logging_before_exit(self):
        with mock.patch(
            "mcp_server.main._init_logging"
        ) as init_log, mock.patch(
            "mcp_server.main.GravitinoMCPServer"
        ), mock.patch.object(
            sys, "argv", ["mcp_server"]
        ):
            with self.assertRaises(SystemExit) as raised:
                do_main()
            self.assertEqual(raised.exception.code, 1)
        init_log.assert_called_once()


if __name__ == "__main__":
    unittest.main()
