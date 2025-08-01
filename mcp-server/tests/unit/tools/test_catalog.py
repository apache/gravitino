import unittest
import asyncio
from unittest.mock import patch

from mcp_server.connector import ConnectorFactory
from mcp_server.core import Setting
from mcp_server.server import GravitinoMCPServer
from fastmcp import Client

from tests.unit.tools import MockOperation


class TestCatalogTool(unittest.TestCase):

  def setUp(self):
    ConnectorFactory.set_test_connector(MockOperation)
    server = GravitinoMCPServer(Setting("", "", ""))
    server.run()
    self.mcp = server.mcp

  def test_list_catalogs(self):
    async def _test_list_catalogs(mcp_server):
      async with Client(mcp_server) as client:
        result = await client.call_tool("get_list_of_catalogs")
        self.assertEqual(result.data, "mock_catalogs")

    asyncio.run(_test_list_catalogs(self.mcp))


if __name__ == '__main__':
  unittest.main()
