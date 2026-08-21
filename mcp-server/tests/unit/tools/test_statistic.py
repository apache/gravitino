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
                        "metalake_name": "mock_metalake",
                        "metadata_type": "mock_type",
                        "metadata_fullname": "mock_fullname",
                    },
                )
                self.assertEqual(
                    "mock_statistics: mock_metalake, mock_type, mock_fullname",
                    result.content[0].text,
                )

        asyncio.run(_test_list_of_statistics(self.mcp))

    def test_list_statistics_for_partition(self):
        async def _test_list_statistics_for_partition(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "list_statistics_for_partition",
                    {
                        "metalake_name": "mock_metalake",
                        "metadata_type": "mock_type",
                        "metadata_fullname": "mock_fullname",
                        "from_partition_name": "from_partition",
                        "to_partition_name": "to_partition",
                    },
                )
                self.assertEqual(
                    "mock_statistics_for_partition: mock_metalake, mock_type, mock_fullname, "
                    "from_partition, to_partition, True, False",
                    result.content[0].text,
                )

        asyncio.run(_test_list_statistics_for_partition(self.mcp))

    def test_write_statistic_tools_enabled_by_default(self):
        async def _test_write_statistic_tools_enabled_by_default(mcp_server):
            tool_names = {tool.name for tool in await mcp_server.list_tools()}

            self.assertIn("list_statistics_for_metadata", tool_names)
            self.assertIn("update_statistics", tool_names)
            self.assertIn("drop_statistics", tool_names)
            self.assertIn("update_partition_statistics", tool_names)
            self.assertIn("drop_partition_statistics", tool_names)

        asyncio.run(_test_write_statistic_tools_enabled_by_default(self.mcp))

    def test_update_statistics(self):
        async def _test_update_statistics(mcp_server):
            self.mcp.enable(names={"update_statistics"}, components={"tool"})
            statistics = {"custom-key1": "value1"}
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "update_statistics",
                    {
                        "metadata_type": "table",
                        "metadata_fullname": "c.s.t",
                        "statistics": statistics,
                    },
                )
                self.assertEqual(
                    f"mock_statistics_updated: table, c.s.t, {statistics}",
                    result.content[0].text,
                )

        asyncio.run(_test_update_statistics(self.mcp))

    def test_drop_statistics(self):
        async def _test_drop_statistics(mcp_server):
            self.mcp.enable(names={"drop_statistics"}, components={"tool"})
            names = ["custom-key1", "custom-key2"]
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "drop_statistics",
                    {
                        "metadata_type": "table",
                        "metadata_fullname": "c.s.t",
                        "statistic_names": names,
                    },
                )
                self.assertEqual(
                    f"mock_statistics_dropped: table, c.s.t, {names}",
                    result.content[0].text,
                )

        asyncio.run(_test_drop_statistics(self.mcp))

    def test_update_partition_statistics(self):
        async def _test_update_partition_statistics(mcp_server):
            self.mcp.enable(
                names={"update_partition_statistics"}, components={"tool"}
            )
            updates = [
                {"partitionName": "p1", "statistics": {"custom-key1": "value1"}}
            ]
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "update_partition_statistics",
                    {
                        "metadata_type": "table",
                        "metadata_fullname": "c.s.t",
                        "partition_updates": updates,
                    },
                )
                self.assertEqual(
                    f"mock_partition_statistics_updated: table, c.s.t, {updates}",
                    result.content[0].text,
                )

        asyncio.run(_test_update_partition_statistics(self.mcp))

    def test_drop_partition_statistics(self):
        async def _test_drop_partition_statistics(mcp_server):
            self.mcp.enable(
                names={"drop_partition_statistics"}, components={"tool"}
            )
            drops = [{"partitionName": "p1", "statisticNames": ["custom-key1"]}]
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "drop_partition_statistics",
                    {
                        "metadata_type": "table",
                        "metadata_fullname": "c.s.t",
                        "partition_drops": drops,
                    },
                )
                self.assertEqual(
                    f"mock_partition_statistics_dropped: table, c.s.t, {drops}",
                    result.content[0].text,
                )

        asyncio.run(_test_drop_partition_statistics(self.mcp))
