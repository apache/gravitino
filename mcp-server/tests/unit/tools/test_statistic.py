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

    def test_get_list_of_statistics(self):
        async def _test_get_list_of_statistics(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "list_statistics_for_metadata",
                    {
                        "metalake_name": "mock_metalake",
                        "metadata_type": "mock_type",
                        "metadata_fullname": "mock_fullname",
                    },
                )
                self.assertEqual("mock_statistics", result.content[0].text)

        asyncio.run(_test_get_list_of_statistics(self.mcp))

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
                    "mock_statistics_for_partition", result.content[0].text
                )

        asyncio.run(_test_list_statistics_for_partition(self.mcp))
