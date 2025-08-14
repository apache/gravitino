import asyncio
import unittest

from fastmcp import Client

from mcp_server.client.factory import RESTClientFactory
from mcp_server.core import Setting
from mcp_server.server import GravitinoMCPServer
from tests.unit.tools.mock_operation import MockOperation


class TestJobTool(unittest.TestCase):
    def setUp(self):
        RESTClientFactory.set_rest_client(MockOperation)
        server = GravitinoMCPServer(Setting("mock_job"))
        self.mcp = server.mcp

    def test_list_job_templates(self):
        async def _test_list_job_templates(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool("get_list_of_job_templates")
                self.assertEqual("mock_job_templates", result.content[0].text)

        asyncio.run(_test_list_job_templates(self.mcp))

    def test_get_job_template_by_name(self):
        async def _test_get_job_template_by_name(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "get_job_template_by_name", {"name": "mock_job"}
                )
                self.assertEqual(
                    "mock_job_template: mock_job", result.content[0].text
                )

        asyncio.run(_test_get_job_template_by_name(self.mcp))

    def test_get_job_by_id(self):
        async def _test_get_job_by_id(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "get_job_by_id", {"job_id": "mock_job_id"}
                )
                self.assertEqual(
                    "mock_job: mock_job_id", result.content[0].text
                )

        asyncio.run(_test_get_job_by_id(self.mcp))

    def test_get_list_of_jobs(self):
        async def _test_get_list_of_jobs(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool("get_list_of_jobs")
                self.assertEqual("mock_jobs", result.content[0].text)

        asyncio.run(_test_get_list_of_jobs(self.mcp))
