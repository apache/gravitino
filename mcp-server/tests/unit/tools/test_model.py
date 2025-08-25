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


class TestModelTool(unittest.TestCase):
    def setUp(self):
        RESTClientFactory.set_rest_client(MockOperation)
        server = GravitinoMCPServer(Setting("mock_metalake"))
        self.mcp = server.mcp

    def test_list_models(self):
        async def _test_list_models(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "list_of_models",
                    {"catalog_name": "mock", "schema_name": "mock"},
                )
                self.assertEqual("mock_models", result.content[0].text)

        asyncio.run(_test_list_models(self.mcp))

    def test_load_model(self):
        async def _test_load_model(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "load_model",
                    {
                        "catalog_name": "mock",
                        "schema_name": "mock",
                        "model_name": "mock",
                    },
                )
                self.assertEqual("mock_model", result.content[0].text)

        asyncio.run(_test_load_model(self.mcp))

    def test_list_model_versions(self):
        async def _test_list_model_versions(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "list_model_versions",
                    {
                        "catalog_name": "mock",
                        "schema_name": "mock",
                        "model_name": "mock",
                    },
                )
                self.assertEqual("mock_model_versions", result.content[0].text)

        asyncio.run(_test_list_model_versions(self.mcp))

    def test_load_model_version(self):
        async def _test_load_model_version(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "load_model_version",
                    {
                        "catalog_name": "mock",
                        "schema_name": "mock",
                        "model_name": "mock",
                        "version": 1,
                    },
                )
                self.assertEqual("mock_model_version", result.content[0].text)

        asyncio.run(_test_load_model_version(self.mcp))

    def test_load_model_version_by_alias(self):
        async def _test_load_model_version_by_alias(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "load_model_version_by_alias",
                    {
                        "catalog_name": "mock",
                        "schema_name": "mock",
                        "model_name": "mock",
                        "alias": "latest",
                    },
                )
                self.assertEqual(
                    "mock_model_version_by_alias", result.content[0].text
                )

        asyncio.run(_test_load_model_version_by_alias(self.mcp))
