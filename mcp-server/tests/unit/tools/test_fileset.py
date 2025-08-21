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


class TestFilesetTool(unittest.TestCase):
    def setUp(self):
        RESTClientFactory.set_rest_client(MockOperation)
        server = GravitinoMCPServer(Setting("mock_metalake"))
        self.mcp = server.mcp

    def test_list_filesets(self):
        async def _test_list_filesets(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "list_of_filesets",
                    {"catalog_name": "mock", "schema_name": "mock"},
                )
                self.assertEqual("mock_filesets", result.content[0].text)

        asyncio.run(_test_list_filesets(self.mcp))

    def test_load_fileset(self):
        async def _test_load_fileset(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "load_fileset",
                    {
                        "catalog_name": "mock",
                        "schema_name": "mock",
                        "fileset_name": "mock",
                    },
                )
                self.assertEqual("mock_fileset", result.content[0].text)

        asyncio.run(_test_load_fileset(self.mcp))

    def test_list_files_in_fileset(self):
        async def _test_list_files_in_fileset(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "list_files_in_fileset",
                    {
                        "catalog_name": "mock",
                        "schema_name": "mock",
                        "fileset_name": "mock",
                        "location_name": "mock_location",
                    },
                )
                self.assertEqual(
                    "mock_files_in_fileset", result.content[0].text
                )

        asyncio.run(_test_list_files_in_fileset(self.mcp))
