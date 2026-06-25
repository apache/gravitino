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


class TestPolicyTool(unittest.TestCase):

    def setUp(self):
        RESTClientFactory.set_rest_client(MockOperation)
        server = GravitinoMCPServer(Setting("mock_metalake"))
        self.mcp = server.mcp

    def test_list_policies(self):
        async def _test_list_policies(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool("get_list_of_policies", {})
                self.assertEqual("mock_policies", result.content[0].text)

        asyncio.run(_test_list_policies(self.mcp))

    def test_get_policy_detail_information(self):
        async def _test_get_policy_detail_information(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "get_policy_detail_information",
                    {"policy_name": "mock_name"},
                )
                self.assertEqual(
                    "mock_policy: mock_name", result.content[0].text
                )

        asyncio.run(_test_get_policy_detail_information(self.mcp))

    def test_list_policies_for_metadata(self):
        async def _test_list_policies_for_metadata(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "list_policies_for_metadata",
                    {
                        "metadata_full_name": "catalog.db.table",
                        "metadata_type": "table",
                    },
                )
                self.assertEqual(
                    "list_policies_for_metadata: catalog.db.table, table",
                    result.content[0].text,
                )

        asyncio.run(_test_list_policies_for_metadata(self.mcp))

    def test_list_metadata_for_policy(self):
        async def _test_list_metadata_for_policy(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "list_metadata_by_policy",
                    {"policy_name": "mock_policy"},
                )
                self.assertEqual(
                    "list_metadata_by_policy: mock_policy",
                    result.content[0].text,
                )

        asyncio.run(_test_list_metadata_for_policy(self.mcp))

    def test_disassociate_policy_from_metadata(self):
        async def _test_disassociate_policy_from_metadata(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "disassociate_policy_from_metadata",
                    {
                        "metadata_full_name": "catalog.db.table",
                        "metadata_type": "table",
                        "policies_to_remove": ["mock_policy"],
                    },
                )
                self.assertEqual(
                    """associate_policy_with_metadata: catalog.db.table, table, [], ['mock_policy']""",
                    result.content[0].text,
                )

        asyncio.run(_test_disassociate_policy_from_metadata(self.mcp))

    def test_associate_policy_with_metadata(self):
        async def _test_associate_policy_with_metadata(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "associate_policy_with_metadata",
                    {
                        "metadata_full_name": "catalog.db.table",
                        "metadata_type": "table",
                        "policies_to_add": ["mock_policy"],
                    },
                )
                self.assertEqual(
                    """associate_policy_with_metadata: catalog.db.table, table, ['mock_policy'], []""",
                    result.content[0].text,
                )

        asyncio.run(_test_associate_policy_with_metadata(self.mcp))

    def test_get_policy_for_metadata(self):
        async def _test_get_policy_for_metadata(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "get_policy_for_metadata",
                    {
                        "metadata_full_name": "catalog.db.table",
                        "metadata_type": "table",
                        "policy_name": "mock_policy",
                    },
                )
                self.assertEqual(
                    """get_policy_for_metadata: catalog.db.table, table, mock_policy""",
                    result.content[0].text,
                )

        asyncio.run(_test_get_policy_for_metadata(self.mcp))

    def test_write_policy_tools_disabled_by_default(self):
        async def _test_write_policy_tools_disabled_by_default(mcp_server):
            tool_names = {tool.name for tool in await mcp_server.list_tools()}

            self.assertIn("get_list_of_policies", tool_names)
            self.assertNotIn("create_policy", tool_names)
            self.assertNotIn("alter_policy", tool_names)
            self.assertNotIn("delete_policy", tool_names)

        asyncio.run(_test_write_policy_tools_disabled_by_default(self.mcp))

    def test_create_policy(self):
        async def _test_create_policy(mcp_server):
            self.mcp.enable(names={"create_policy"}, components={"tool"})
            content = {
                "customRules": {"rule1": 123},
                "properties": {"key1": "value1"},
                "supportedObjectTypes": ["table"],
            }
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "create_policy",
                    {
                        "name": "mock_policy",
                        "policy_type": "custom",
                        "comment": "mock_comment",
                        "content": content,
                        "enabled": True,
                    },
                )
                self.assertEqual(
                    f"mock_policy_created: mock_policy, custom, mock_comment, "
                    f"True, {content}",
                    result.content[0].text,
                )

        asyncio.run(_test_create_policy(self.mcp))

    def test_alter_policy(self):
        async def _test_alter_policy(mcp_server):
            self.mcp.enable(names={"alter_policy"}, components={"tool"})
            updates = [{"@type": "rename", "newName": "policy2"}]
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "alter_policy",
                    {"name": "mock_policy", "updates": updates},
                )
                self.assertEqual(
                    f"mock_policy_altered: mock_policy with updates {updates}",
                    result.content[0].text,
                )

        asyncio.run(_test_alter_policy(self.mcp))

    def test_delete_policy(self):
        async def _test_delete_policy(mcp_server):
            self.mcp.enable(names={"delete_policy"}, components={"tool"})
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "delete_policy",
                    {"name": "mock_policy"},
                )
                self.assertEqual(
                    "mock_policy_deleted: mock_policy",
                    result.content[0].text,
                )

        asyncio.run(_test_delete_policy(self.mcp))
