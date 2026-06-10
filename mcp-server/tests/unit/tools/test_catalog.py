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

import asyncio
import unittest

from fastmcp import Client

from mcp_server.client.factory import RESTClientFactory
from mcp_server.core import Setting
from mcp_server.server import GravitinoMCPServer
from tests.unit.tools import MockOperation


class TestCatalogTool(unittest.TestCase):
    """Test the catalog tool functionality."""

    def setUp(self):
        """Set up the test environment."""

        RESTClientFactory.set_rest_client(MockOperation)
        server = GravitinoMCPServer(Setting("mock_metalake"))
        self.mcp = server.mcp

    def test_list_catalogs(self):
        """Test listing catalogs."""

        async def _test_list_catalogs(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool("get_list_of_catalogs")
                self.assertEqual("mock_catalogs", result.content[0].text)

        asyncio.run(_test_list_catalogs(self.mcp))

    def test_create_catalog(self):
        async def _test(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "create_catalog",
                    {
                        "name": "cat1",
                        "catalog_type": "relational",
                        "provider": "hive",
                        "comment": "c",
                        "properties": {"k": "v"},
                    },
                )
                self.assertEqual(
                    "mock_catalog_created: cat1, relational, hive",
                    result.content[0].text,
                )

        asyncio.run(_test(self.mcp))

    def test_alter_catalog(self):
        async def _test(mcp_server):
            updates = [{"@type": "rename", "newName": "cat2"}]
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "alter_catalog",
                    {"catalog_name": "cat1", "updates": updates},
                )
                self.assertEqual(
                    f"mock_catalog_altered: cat1 with updates {updates}",
                    result.content[0].text,
                )

        asyncio.run(_test(self.mcp))

    def test_drop_catalog(self):
        async def _test(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "drop_catalog", {"catalog_name": "cat1"}
                )
                self.assertEqual(
                    "mock_catalog_dropped: cat1", result.content[0].text
                )

        asyncio.run(_test(self.mcp))

    def test_write_tools_exposed(self):
        """Write tools must be present (authorization enforced by Gravitino)."""

        async def _test(mcp_server):
            names = {t.name for t in await mcp_server.list_tools()}
            self.assertIn("create_catalog", names)
            self.assertIn("alter_catalog", names)
            self.assertIn("drop_catalog", names)

        asyncio.run(_test(self.mcp))
