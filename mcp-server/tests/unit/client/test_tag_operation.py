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

"""Regression tests for the metadata tag list REST response contract."""

import json
import unittest
from unittest.mock import patch

import httpx
from fastmcp import Client

from mcp_server.client.factory import RESTClientFactory
from mcp_server.client.plain.exception import GravitinoException
from mcp_server.client.plain.plain_rest_client_tag_operation import (
    PlainRESTClientTagOperation,
)
from mcp_server.core import Setting
from mcp_server.server import GravitinoMCPServer
from tests.unit.tools import MockOperation


class TestTagOperation(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tags = [
            {
                "name": "firstTag",
                "comment": "Inherited governance tag",
                "properties": {"classification": "internal"},
                "audit": {
                    "creator": "admin",
                    "createTime": "2026-09-13T10:00:00Z",
                },
                "inherited": True,
            },
            {
                "name": "tableTag",
                "comment": "Direct table tag",
                "properties": {},
                "audit": {
                    "creator": "admin",
                    "createTime": "2026-09-13T11:00:00Z",
                },
                "inherited": False,
            },
        ]

    def _client(self, body):
        def respond(request):
            self.assertEqual(request.method, "GET")
            self.assertEqual(
                request.url.path,
                "/api/metalakes/acme/objects/table/iceberg_s3.sales.orders/tags",
            )
            self.assertEqual(dict(request.url.params), {"details": "true"})
            return httpx.Response(200, json=body)

        return httpx.AsyncClient(
            base_url="http://localhost:8090",
            transport=httpx.MockTransport(respond),
        )

    async def test_list_tags_preserves_complete_tag_details(self):
        async with self._client({"code": 0, "tags": self.tags}) as client:
            operation = PlainRESTClientTagOperation("acme", client)
            result = await operation.list_tags_for_metadata(
                "iceberg_s3.sales.orders", "table"
            )
            self.assertEqual(json.loads(result), self.tags)

    async def test_list_tags_without_associations(self):
        async with self._client({"code": 0, "tags": []}) as client:
            operation = PlainRESTClientTagOperation("acme", client)
            result = await operation.list_tags_for_metadata(
                "iceberg_s3.sales.orders", "table"
            )
            self.assertEqual(json.loads(result), [])

    async def test_list_tags_propagates_server_error(self):
        body = {
            "code": 1003,
            "type": "NoSuchMetadataObjectException",
            "message": "Metadata object does not exist",
        }
        async with self._client(body) as client:
            operation = PlainRESTClientTagOperation("acme", client)
            with self.assertRaisesRegex(
                GravitinoException, "Metadata object does not exist"
            ):
                await operation.list_tags_for_metadata(
                    "iceberg_s3.sales.orders", "table"
                )

    async def test_mcp_tool_returns_rest_tag_details(self):
        async with self._client({"code": 0, "tags": self.tags}) as rest_client:
            operation = PlainRESTClientTagOperation("acme", rest_client)
            with (
                patch.object(
                    RESTClientFactory, "_rest_client_class", MockOperation
                ),
                patch.object(
                    MockOperation, "as_tag_operation", return_value=operation
                ),
            ):
                server = GravitinoMCPServer(Setting("acme"))
                async with Client(server.mcp) as client:
                    result = await client.call_tool(
                        "list_tags_for_metadata",
                        {
                            "metadata_full_name": "iceberg_s3.sales.orders",
                            "metadata_type": "table",
                        },
                    )
                    self.assertEqual(
                        json.loads(result.content[0].text), self.tags
                    )
