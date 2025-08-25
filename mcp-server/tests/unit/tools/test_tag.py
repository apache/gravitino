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


class TestTagTool(unittest.TestCase):
    def setUp(self):
        RESTClientFactory.set_rest_client(MockOperation)
        server = GravitinoMCPServer(Setting("mock_metalake"))
        self.mcp = server.mcp

    def test_list_tags(self):
        async def _test_list_tags(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool("list_of_tags")
                self.assertEqual("mock_tags", result.content[0].text)

        asyncio.run(_test_list_tags(self.mcp))

    def test_get_tag_by_name(self):
        async def _test_get_tag_by_name(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "get_tag_by_name",
                    {
                        "tag_name": "mock",
                    },
                )
                self.assertEqual("mock_tag: mock", result.content[0].text)

        asyncio.run(_test_get_tag_by_name(self.mcp))

    def test_associate_tag_with_metadata(self):
        async def _test_associate_tag_with_metadata(mcp_server):

            tags_to_associate = ["tag1", "tag2"]
            metadata_full_name = "catalog.schema.table"
            metadata_type = "table"
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "associate_tag_with_metadata",
                    {
                        "metadata_full_name": metadata_full_name,
                        "metadata_type": metadata_type,
                        "tags_to_associate": tags_to_associate,
                    },
                )
                self.assertEqual(
                    f"mock_associated_tags: {tags_to_associate} with metadata"
                    f" {metadata_full_name} of type {metadata_type}",
                    result.content[0].text,
                )

        asyncio.run(_test_associate_tag_with_metadata(self.mcp))

    def test_disassociate_tag_from_metadata(self):
        async def _test_disassociate_tag_from_metadata(mcp_server):

            tags_to_disassociate = ["tag1", "tag2"]
            metadata_full_name = "catalog.schema.table"
            metadata_type = "table"
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "disassociate_tag_from_metadata",
                    {
                        "metadata_full_name": metadata_full_name,
                        "metadata_type": metadata_type,
                        "tags_to_disassociate": tags_to_disassociate,
                    },
                )
                self.assertEqual(
                    f"mock_associated_tags: [] with metadata"
                    f" {metadata_full_name} of type {metadata_type}",
                    result.content[0].text,
                )

        asyncio.run(_test_disassociate_tag_from_metadata(self.mcp))
