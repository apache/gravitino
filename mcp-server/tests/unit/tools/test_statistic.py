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
