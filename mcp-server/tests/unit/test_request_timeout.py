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

# Tests inspect client configuration and identity caches deliberately.
# pylint: disable=protected-access

import asyncio
import math
import os
import sys
import unittest
from unittest.mock import patch

import httpx
from fastmcp import Client, FastMCP

from mcp_server.client.factory import RESTClientFactory
from mcp_server.client.plain.exception import GravitinoException
from mcp_server.client.plain.plain_rest_client_operation import (
    _GravitinoAsyncClient,
)
from mcp_server.core.context import GravitinoContext
from mcp_server.core.setting import Setting
from mcp_server.main import _parse_args
from mcp_server.server import GravitinoMCPServer


class TestRequestTimeout(unittest.TestCase):
    def test_configuration(self):
        self.assertEqual(60.0, Setting().gravitino_read_timeout)
        for value in (0, -1, math.inf, math.nan):
            with self.assertRaises(ValueError):
                Setting(gravitino_read_timeout=value)
        with patch.dict(os.environ, {"GRAVITINO_READ_TIMEOUT": "120"}):
            with patch.object(sys, "argv", ["mcp_server"]):
                self.assertEqual(120, _parse_args().gravitino_read_timeout)
            with patch.object(
                sys, "argv", ["mcp_server", "--gravitino-read-timeout", "90"]
            ):
                self.assertEqual(90, _parse_args().gravitino_read_timeout)

    def test_invalid_cli_and_environment_timeout(self):
        with patch.dict(os.environ, {"GRAVITINO_READ_TIMEOUT": "nan"}):
            with patch.object(sys, "argv", ["mcp_server"]):
                with self.assertRaises(SystemExit) as failure:
                    _parse_args()
                self.assertEqual(2, failure.exception.code)
        with patch.object(
            sys, "argv", ["mcp_server", "--gravitino-read-timeout", "0"]
        ):
            with self.assertRaises(SystemExit):
                _parse_args()

    def test_client_timeout(self):
        async def check():
            client = RESTClientFactory.create_rest_client(
                "m", "http://localhost", read_timeout=90
            )
            try:
                self.assertEqual(90, client._rest_client.timeout.read)
                self.assertEqual(5, client._rest_client.timeout.connect)
                self.assertEqual(5, client._rest_client.timeout.pool)
            finally:
                await client._rest_client.aclose()

        asyncio.run(check())

    def test_all_identity_paths_use_configured_timeout(self):
        async def check():
            context = GravitinoContext(
                Setting(metalake="m", gravitino_read_timeout=120)
            )
            clients = [
                context._default_client,
                context._service_client("other"),
            ]
            with patch(
                "mcp_server.core.context._get_request_authorization",
                return_value="Bearer caller",
            ):
                clients.append(context.rest_client())
            try:
                for client in clients:
                    self.assertEqual(120, client._rest_client.timeout.read)
            finally:
                for client in clients:
                    await client.close()

        asyncio.run(check())

    def test_read_timeout_is_actionable_and_not_retried(self):
        async def check():
            calls = []

            def handler(request):
                calls.append(request)
                raise httpx.ReadTimeout(
                    "secret backend detail", request=request
                )

            async with _GravitinoAsyncClient(
                transport=httpx.MockTransport(handler),
                timeout=httpx.Timeout(60),
            ) as client:
                with self.assertRaises(GravitinoException) as failure:
                    await client.post(
                        "http://user:password@localhost/tables?token=query_secret",
                        headers={"Authorization": "Bearer auth_secret"},
                        json={},
                    )
                self.assertIn("POST /tables", str(failure.exception))
                self.assertNotIn("password", str(failure.exception))
                self.assertNotIn("query_secret", str(failure.exception))
                self.assertIn("60 seconds", str(failure.exception))
                self.assertIn("may still complete", str(failure.exception))
                self.assertNotIn("secret", str(failure.exception))
                self.assertIsInstance(
                    failure.exception.__cause__, httpx.ReadTimeout
                )
                self.assertEqual(1, len(calls))

        asyncio.run(check())

    def test_mcp_tool_preserves_timeout_details(self):
        async def check():
            def handler(request):
                raise httpx.ReadTimeout("slow", request=request)

            server = FastMCP("timeout-test")
            async with _GravitinoAsyncClient(
                transport=httpx.MockTransport(handler),
                timeout=httpx.Timeout(90),
            ) as rest:

                @server.tool
                async def load_table() -> str:
                    """Load metadata from Gravitino."""
                    await rest.get("http://localhost/table")
                    return "loaded"

                async with Client(server) as client:
                    result = await client.call_tool(
                        "load_table", {}, raise_on_error=False
                    )
                    self.assertTrue(result.is_error)
                    message = " ".join(item.text for item in result.content)
                    self.assertIn("90 seconds", message)
                    self.assertIn("may still complete", message)

        asyncio.run(check())

    def test_registered_tool_preserves_timeout_through_production_server(self):
        async def check():
            calls = []

            def handler(request):
                calls.append(request)
                raise httpx.ReadTimeout("slow", request=request)

            original_init = _GravitinoAsyncClient.__init__

            def init_with_transport(client, *args, **kwargs):
                kwargs["transport"] = httpx.MockTransport(handler)
                original_init(client, *args, **kwargs)

            with patch.object(
                _GravitinoAsyncClient, "__init__", init_with_transport
            ):
                server = GravitinoMCPServer(
                    Setting(metalake="m", gravitino_read_timeout=75)
                )
                async with Client(server.mcp) as client:
                    result = await client.call_tool(
                        "get_table_metadata_details",
                        {
                            "catalog_name": "c",
                            "schema_name": "s",
                            "table_name": "t",
                        },
                        raise_on_error=False,
                    )
                    self.assertTrue(result.is_error)
                    message = " ".join(item.text for item in result.content)
                    self.assertIn("75 seconds", message)
                    self.assertIn(
                        "GET /api/metalakes/m/catalogs/c/schemas/s/tables/t",
                        message,
                    )
                    self.assertIn("may still complete", message)
                    self.assertEqual(1, len(calls))

        asyncio.run(check())

    def test_authentication_timeout_is_not_reported_as_gravitino(self):
        async def check():
            calls = []

            def token_handler(request):
                raise httpx.ReadTimeout("IdP timeout", request=request)

            async with httpx.AsyncClient(
                transport=httpx.MockTransport(token_handler)
            ) as token_client:

                class TokenAuth(httpx.Auth):
                    async def async_auth_flow(self, request):
                        await token_client.get("http://identity/token")
                        yield request

                def gravitino_handler(request):
                    calls.append(request)
                    return httpx.Response(200)

                async with _GravitinoAsyncClient(
                    auth=TokenAuth(),
                    transport=httpx.MockTransport(gravitino_handler),
                    timeout=httpx.Timeout(120),
                ) as client:
                    with self.assertRaises(httpx.ReadTimeout) as failure:
                        await client.get("http://gravitino/table")
                    self.assertEqual("IdP timeout", str(failure.exception))
                    self.assertEqual([], calls)

        asyncio.run(check())

    def test_other_results_are_preserved(self):
        async def check():
            def handler(request):
                return httpx.Response(403, json={"message": "denied"})

            async with _GravitinoAsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                response = await client.get("http://localhost/tables")
                self.assertEqual(403, response.status_code)
                self.assertEqual("denied", response.json()["message"])

        asyncio.run(check())
