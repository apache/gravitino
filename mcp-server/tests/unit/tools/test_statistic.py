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
from fastmcp.exceptions import ToolError

from mcp_server.client.factory import RESTClientFactory
from mcp_server.core import Setting
from mcp_server.server import GravitinoMCPServer
from tests.unit.tools import MockOperation


class TestStatisticTool(unittest.TestCase):
    def setUp(self):
        RESTClientFactory.set_rest_client(MockOperation)
        server = GravitinoMCPServer(Setting("mock_job"))
        self.mcp = server.mcp

    def test_list_of_statistics(self):
        async def _test_list_of_statistics(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "list_statistics_for_metadata",
                    {
                        "metadata_type": "mock_type",
                        "metadata_full_name": "mock_fullname",
                    },
                )
                self.assertEqual(
                    "mock_statistics: mock_type, mock_fullname",
                    result.content[0].text,
                )

        asyncio.run(_test_list_of_statistics(self.mcp))

    def test_list_statistics_for_partition(self):
        async def _test_list_statistics_for_partition(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "list_statistics_for_partition",
                    {
                        "metadata_type": "mock_type",
                        "metadata_full_name": "mock_fullname",
                        "from_partition_name": "from_partition",
                        "to_partition_name": "to_partition",
                    },
                )
                self.assertEqual(
                    "mock_statistics_for_partition: mock_type, mock_fullname, "
                    "from_partition, to_partition, True, False",
                    result.content[0].text,
                )

        asyncio.run(_test_list_statistics_for_partition(self.mcp))

    def test_metadata_full_name_schema_is_consistent(self):
        async def _test():
            async with Client(self.mcp) as client:
                tools = {tool.name: tool for tool in await client.list_tools()}
                for name in (
                    "list_statistics_for_metadata",
                    "list_statistics_for_partition",
                    "associate_tag_with_metadata",
                    "disassociate_tag_from_metadata",
                    "list_tags_for_metadata",
                    "list_policies_for_metadata",
                ):
                    with self.subTest(tool=name):
                        schema = tools[name].inputSchema
                        self.assertIn(
                            "metadata_full_name", schema["properties"]
                        )
                        self.assertIn("metadata_full_name", schema["required"])
                        self.assertNotIn(
                            "metadata_fullname", schema["properties"]
                        )

        asyncio.run(_test())

    def test_legacy_metadata_fullname_is_still_accepted(self):
        async def _test():
            async with Client(self.mcp) as client:
                for name, extra, expected in (
                    (
                        "list_statistics_for_metadata",
                        {},
                        "mock_statistics: table, catalog.schema.table",
                    ),
                    (
                        "list_statistics_for_partition",
                        {
                            "from_partition_name": "p1",
                            "to_partition_name": "p2",
                            "from_inclusive": False,
                            "to_inclusive": True,
                        },
                        "mock_statistics_for_partition: table, "
                        "catalog.schema.table, p1, p2, False, True",
                    ),
                ):
                    with self.subTest(tool=name):
                        result = await client.call_tool(
                            name,
                            {
                                "metadata_type": "table",
                                "metadata_fullname": "catalog.schema.table",
                                **extra,
                            },
                        )
                        self.assertEqual(result.content[0].text, expected)

        asyncio.run(_test())

    def test_missing_or_duplicate_full_name_is_rejected(self):
        async def _test():
            async with Client(self.mcp) as client:
                for name, extra in (
                    ("list_statistics_for_metadata", {}),
                    (
                        "list_statistics_for_partition",
                        {
                            "from_partition_name": "p1",
                            "to_partition_name": "p2",
                        },
                    ),
                ):
                    for arguments, error in (
                        ({}, "Missing required argument"),
                        (
                            {
                                "metadata_full_name": "catalog.schema.table",
                                "metadata_fullname": "catalog.schema.other",
                            },
                            "Unexpected keyword argument",
                        ),
                    ):
                        with self.subTest(tool=name, arguments=arguments):
                            with self.assertRaisesRegex(ToolError, error):
                                await client.call_tool(
                                    name,
                                    {
                                        "metadata_type": "table",
                                        **extra,
                                        **arguments,
                                    },
                                )

        asyncio.run(_test())
