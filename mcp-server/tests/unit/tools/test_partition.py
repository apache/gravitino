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


class TestPartitionTool(unittest.TestCase):
    def setUp(self):
        RESTClientFactory.set_rest_client(MockOperation)
        server = GravitinoMCPServer(Setting("mock_partition"))
        self.mcp = server.mcp

    def test_list_of_partitions(self):
        async def _test_list_of_partitions(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "list_of_partitions",
                    {
                        "catalog_name": "mock_catalog",
                        "schema_name": "mock_schema",
                        "table_name": "mock_table",
                    },
                )
                self.assertEqual(
                    "mock_partitions: mock_catalog, mock_schema, mock_table, "
                    "False",
                    result.content[0].text,
                )

        asyncio.run(_test_list_of_partitions(self.mcp))

    def test_list_of_partitions_with_details(self):
        async def _test_list_of_partitions_with_details(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "list_of_partitions",
                    {
                        "catalog_name": "mock_catalog",
                        "schema_name": "mock_schema",
                        "table_name": "mock_table",
                        "details": True,
                    },
                )
                self.assertEqual(
                    "mock_partitions: mock_catalog, mock_schema, mock_table, "
                    "True",
                    result.content[0].text,
                )

        asyncio.run(_test_list_of_partitions_with_details(self.mcp))

    def test_get_partition(self):
        async def _test_get_partition(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "get_partition",
                    {
                        "catalog_name": "mock_catalog",
                        "schema_name": "mock_schema",
                        "table_name": "mock_table",
                        "partition_name": "mock_partition",
                    },
                )
                self.assertEqual(
                    "mock_partition: mock_catalog, mock_schema, mock_table, "
                    "mock_partition",
                    result.content[0].text,
                )

        asyncio.run(_test_get_partition(self.mcp))
