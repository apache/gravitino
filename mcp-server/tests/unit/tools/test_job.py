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
from tests.unit.tools.mock_operation import MockOperation


class TestJobTool(unittest.TestCase):
    def setUp(self):
        RESTClientFactory.set_rest_client(MockOperation)
        server = GravitinoMCPServer(Setting("mock_job"))
        self.mcp = server.mcp

    def test_list_job_templates(self):
        async def _test_list_job_templates(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool("list_of_job_templates")
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

    def test_list_of_jobs(self):
        async def _test_list_of_jobs(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool("list_of_jobs")
                self.assertEqual("mock_jobs", result.content[0].text)

        asyncio.run(_test_list_of_jobs(self.mcp))

    def test_run_job(self):
        async def _test_run_job(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "run_job",
                    {
                        "job_template_name": "mock_job_template",
                        "job_config": {},
                    },
                )
                self.assertEqual(
                    "mock_job_run: mock_job_template with parameters {}",
                    result.content[0].text,
                )

        asyncio.run(_test_run_job(self.mcp))

    def test_cancel_job(self):
        async def _test_cancel_job(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "cancel_job", {"job_id": "mock_job_id"}
                )
                self.assertEqual(
                    "mock_job_cancelled: mock_job_id", result.content[0].text
                )

        asyncio.run(_test_cancel_job(self.mcp))
