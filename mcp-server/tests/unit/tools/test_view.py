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


class TestViewTool(unittest.TestCase):
    def setUp(self):
        RESTClientFactory.set_rest_client(MockOperation)
        server = GravitinoMCPServer(Setting("mock_view"))
        self.mcp = server.mcp

    def test_list_of_views(self):
        async def _test_list_of_views(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "list_of_views",
                    {
                        "catalog_name": "mock_catalog",
                        "schema_name": "mock_schema",
                    },
                )
                self.assertEqual(
                    "mock_views: mock_catalog, mock_schema",
                    result.content[0].text,
                )

        asyncio.run(_test_list_of_views(self.mcp))

    def test_load_view(self):
        async def _test_load_view(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "load_view",
                    {
                        "catalog_name": "mock_catalog",
                        "schema_name": "mock_schema",
                        "view_name": "mock_view",
                    },
                )
                self.assertEqual(
                    "mock_view: mock_catalog, mock_schema, mock_view",
                    result.content[0].text,
                )

        asyncio.run(_test_load_view(self.mcp))

    def test_create_view(self):
        async def _test(mcp_server):
            representations = [
                {"type": "sql", "dialect": "spark", "sql": "SELECT 1"}
            ]
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "create_view",
                    {
                        "catalog_name": "cat",
                        "schema_name": "sch",
                        "name": "v",
                        "comment": "c",
                        "columns": [{"name": "id", "type": "integer"}],
                        "representations": representations,
                        "properties": {"k": "v"},
                    },
                )
                self.assertEqual(
                    f"mock_view_created: cat.sch.v "
                    f"with representations {representations}, "
                    "default_catalog=None",
                    result.content[0].text,
                )

        asyncio.run(_test(self.mcp))

    def test_create_view_with_defaults(self):
        async def _test(mcp_server):
            representations = [
                {"type": "sql", "dialect": "spark", "sql": "SELECT 1"}
            ]
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "create_view",
                    {
                        "catalog_name": "cat",
                        "schema_name": "sch",
                        "name": "v",
                        "comment": "c",
                        "columns": [],
                        "representations": representations,
                        "properties": {},
                        "default_catalog": "dc",
                        "default_schema": "ds",
                    },
                )
                self.assertEqual(
                    f"mock_view_created: cat.sch.v "
                    f"with representations {representations}, "
                    "default_catalog=dc",
                    result.content[0].text,
                )

        asyncio.run(_test(self.mcp))

    def test_alter_view(self):
        async def _test(mcp_server):
            updates = [{"@type": "rename", "newName": "renamed"}]
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "alter_view",
                    {
                        "catalog_name": "cat",
                        "schema_name": "sch",
                        "view_name": "v",
                        "updates": updates,
                    },
                )
                self.assertEqual(
                    f"mock_view_altered: cat.sch.v with updates {updates}",
                    result.content[0].text,
                )

        asyncio.run(_test(self.mcp))

    def test_drop_view(self):
        async def _test(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "drop_view",
                    {
                        "catalog_name": "cat",
                        "schema_name": "sch",
                        "view_name": "v",
                    },
                )
                self.assertEqual(
                    "mock_view_dropped: cat.sch.v", result.content[0].text
                )

        asyncio.run(_test(self.mcp))
