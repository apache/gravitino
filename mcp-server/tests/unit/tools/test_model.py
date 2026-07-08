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


class TestModelTool(unittest.TestCase):
    def setUp(self):
        RESTClientFactory.set_rest_client(MockOperation)
        server = GravitinoMCPServer(Setting("mock_metalake"))
        self.mcp = server.mcp

    def test_list_models(self):
        async def _test_list_models(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "list_of_models",
                    {"catalog_name": "mock", "schema_name": "mock"},
                )
                self.assertEqual("mock_models", result.content[0].text)

        asyncio.run(_test_list_models(self.mcp))

    def test_load_model(self):
        async def _test_load_model(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "load_model",
                    {
                        "catalog_name": "mock",
                        "schema_name": "mock",
                        "model_name": "mock",
                    },
                )
                self.assertEqual("mock_model", result.content[0].text)

        asyncio.run(_test_load_model(self.mcp))

    def test_list_model_versions(self):
        async def _test_list_model_versions(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "list_model_versions",
                    {
                        "catalog_name": "mock",
                        "schema_name": "mock",
                        "model_name": "mock",
                    },
                )
                self.assertEqual("mock_model_versions", result.content[0].text)

        asyncio.run(_test_list_model_versions(self.mcp))

    def test_load_model_version(self):
        async def _test_load_model_version(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "load_model_version",
                    {
                        "catalog_name": "mock",
                        "schema_name": "mock",
                        "model_name": "mock",
                        "version": 1,
                    },
                )
                self.assertEqual("mock_model_version", result.content[0].text)

        asyncio.run(_test_load_model_version(self.mcp))

    def test_load_model_version_by_alias(self):
        async def _test_load_model_version_by_alias(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "load_model_version_by_alias",
                    {
                        "catalog_name": "mock",
                        "schema_name": "mock",
                        "model_name": "mock",
                        "alias": "latest",
                    },
                )
                self.assertEqual(
                    "mock_model_version_by_alias", result.content[0].text
                )

        asyncio.run(_test_load_model_version_by_alias(self.mcp))

    def test_register_model(self):
        async def _test(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "register_model",
                    {
                        "catalog_name": "cat",
                        "schema_name": "sch",
                        "name": "m",
                        "comment": "c",
                        "properties": {"k": "v"},
                    },
                )
                self.assertEqual(
                    "mock_model_registered: cat.sch.m", result.content[0].text
                )

        asyncio.run(_test(self.mcp))

    def test_delete_model(self):
        async def _test(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "delete_model",
                    {
                        "catalog_name": "cat",
                        "schema_name": "sch",
                        "model_name": "m",
                    },
                )
                self.assertEqual(
                    "mock_model_deleted: cat.sch.m", result.content[0].text
                )

        asyncio.run(_test(self.mcp))

    def test_link_model_version(self):
        async def _test(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "link_model_version",
                    {
                        "catalog_name": "cat",
                        "schema_name": "sch",
                        "model_name": "m",
                        "uri": "s3://bucket/model",
                        "aliases": ["latest"],
                        "comment": "c",
                        "properties": {"k": "v"},
                    },
                )
                self.assertEqual(
                    "mock_model_version_linked: cat.sch.m "
                    "uri=s3://bucket/model aliases=['latest']",
                    result.content[0].text,
                )

        asyncio.run(_test(self.mcp))

    def test_delete_model_version(self):
        async def _test(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "delete_model_version",
                    {
                        "catalog_name": "cat",
                        "schema_name": "sch",
                        "model_name": "m",
                        "version": 1,
                    },
                )
                self.assertEqual(
                    "mock_model_version_deleted: cat.sch.m version=1",
                    result.content[0].text,
                )

        asyncio.run(_test(self.mcp))

    def test_delete_model_version_by_alias(self):
        async def _test(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "delete_model_version_by_alias",
                    {
                        "catalog_name": "cat",
                        "schema_name": "sch",
                        "model_name": "m",
                        "alias": "latest",
                    },
                )
                self.assertEqual(
                    "mock_model_version_deleted_by_alias: cat.sch.m "
                    "alias=latest",
                    result.content[0].text,
                )

        asyncio.run(_test(self.mcp))

    def test_alter_model(self):
        async def _test(mcp_server):
            updates = [{"@type": "rename", "newName": "m2"}]
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "alter_model",
                    {
                        "catalog_name": "cat",
                        "schema_name": "sch",
                        "model_name": "m",
                        "updates": updates,
                    },
                )
                self.assertEqual(
                    f"mock_model_altered: cat.sch.m with updates {updates}",
                    result.content[0].text,
                )

        asyncio.run(_test(self.mcp))

    def test_alter_model_version(self):
        async def _test(mcp_server):
            updates = [{"@type": "updateComment", "newComment": "c2"}]
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "alter_model_version",
                    {
                        "catalog_name": "cat",
                        "schema_name": "sch",
                        "model_name": "m",
                        "version": 1,
                        "updates": updates,
                    },
                )
                self.assertEqual(
                    "mock_model_version_altered: cat.sch.m version=1 "
                    f"with updates {updates}",
                    result.content[0].text,
                )

        asyncio.run(_test(self.mcp))

    def test_alter_model_version_by_alias(self):
        async def _test(mcp_server):
            updates = [{"@type": "updateUri", "newUri": "s3://bucket/m2"}]
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "alter_model_version_by_alias",
                    {
                        "catalog_name": "cat",
                        "schema_name": "sch",
                        "model_name": "m",
                        "alias": "latest",
                        "updates": updates,
                    },
                )
                self.assertEqual(
                    "mock_model_version_altered_by_alias: cat.sch.m "
                    f"alias=latest with updates {updates}",
                    result.content[0].text,
                )

        asyncio.run(_test(self.mcp))
