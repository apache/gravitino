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

    def test_write_job_template_tools_disabled_by_default(self):
        async def _test_write_job_template_tools_disabled_by_default(
            mcp_server,
        ):
            tool_names = {tool.name for tool in await mcp_server.list_tools()}

            self.assertIn("list_of_job_templates", tool_names)
            self.assertNotIn("register_job_template", tool_names)
            self.assertNotIn("delete_job_template", tool_names)

        asyncio.run(
            _test_write_job_template_tools_disabled_by_default(self.mcp)
        )

    def test_register_job_template(self):
        async def _test_register_job_template(mcp_server):
            self.mcp.enable(
                names={"register_job_template"}, components={"tool"}
            )
            job_template = {
                "jobType": "shell",
                "name": "shell_test",
                "executable": "/path/to/script.sh",
            }
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "register_job_template",
                    {"job_template": job_template},
                )
                self.assertEqual(
                    f"mock_job_template_registered: {job_template}",
                    result.content[0].text,
                )

        asyncio.run(_test_register_job_template(self.mcp))

    def test_delete_job_template(self):
        async def _test_delete_job_template(mcp_server):
            self.mcp.enable(names={"delete_job_template"}, components={"tool"})
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "delete_job_template", {"name": "shell_test"}
                )
                self.assertEqual(
                    "mock_job_template_deleted: shell_test",
                    result.content[0].text,
                )

        asyncio.run(_test_delete_job_template(self.mcp))
