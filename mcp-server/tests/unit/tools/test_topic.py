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
import json
import unittest
from unittest.mock import AsyncMock, patch

from fastmcp import Client

from mcp_server.client.factory import RESTClientFactory
from mcp_server.core import Setting
from mcp_server.server import GravitinoMCPServer
from tests.unit.tools.mock_operation import MockOperation, MockTopicOperation


class TestTopicTool(unittest.TestCase):
    def setUp(self):
        RESTClientFactory.set_rest_client(MockOperation)
        server = GravitinoMCPServer(Setting("mock_metalake"))
        self.mcp = server.mcp

    def test_list_topics(self):
        async def _test_list_topics(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "list_of_topics",
                    {"catalog_name": "mock", "schema_name": "mock"},
                )
                self.assertEqual("mock_topics", result.content[0].text)

        asyncio.run(_test_list_topics(self.mcp))

    def test_load_topic(self):
        async def _test_load_topic(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "load_topic",
                    {
                        "catalog_name": "mock",
                        "schema_name": "mock",
                        "topic_name": "mock",
                    },
                )
                self.assertEqual("mock_topic", result.content[0].text)

        asyncio.run(_test_load_topic(self.mcp))

    def test_create_topic(self):
        async def _test(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "create_topic",
                    {
                        "catalog_name": "cat",
                        "schema_name": "sch",
                        "name": "tp",
                        "comment": "c",
                        "properties": {"k": "v"},
                    },
                )
                self.assertEqual(
                    "mock_topic_created: cat.sch.tp", result.content[0].text
                )

        asyncio.run(_test(self.mcp))

    def test_alter_topic(self):
        async def _test(mcp_server):
            updates = [{"@type": "setProperty", "property": "k", "value": "v"}]
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "alter_topic",
                    {
                        "catalog_name": "cat",
                        "schema_name": "sch",
                        "topic_name": "tp",
                        "updates": updates,
                    },
                )
                self.assertEqual(
                    f"mock_topic_altered: cat.sch.tp with updates {updates}",
                    result.content[0].text,
                )

        asyncio.run(_test(self.mcp))

    def test_delete_topic(self):
        async def _test(mcp_server):
            async with Client(mcp_server) as client:
                result = await client.call_tool(
                    "delete_topic",
                    {
                        "catalog_name": "cat",
                        "schema_name": "sch",
                        "topic_name": "tp",
                    },
                )
                self.assertEqual(
                    "mock_topic_deleted: cat.sch.tp", result.content[0].text
                )

        asyncio.run(_test(self.mcp))

    def test_get_topic_iceberg_linkage_enabled(self):
        topic_payload = json.dumps(
            {
                "name": "orders_events",
                "properties": {
                    "automq.table.topic.enable": "true",
                    "automq.table.topic.schema.type": "schema",
                    "automq.table.topic.commit.interval.ms": "5000",
                    "automq.table.topic.upsert.enable": "true",
                    "automq.table.topic.id.columns": "order_id",
                    "automq.table.topic.partition.by": "day(event_ts)",
                    "automq.table.topic.cdc.field": "op",
                    "automq.table.topic.errors.tolerance": "none",
                    "table-topic.enabled": "true",
                    "table-topic.iceberg-catalog": "iceberg_lakehouse",
                    "table-topic.iceberg-namespace": "analytics",
                    "table-topic.iceberg-table": "orders_events",
                    "table-topic.iceberg-ident": (
                        "mock_metalake.iceberg_lakehouse."
                        "analytics.orders_events"
                    ),
                },
            }
        )

        async def _test(mcp_server):
            with patch.object(
                MockTopicOperation,
                "load_topic",
                new=AsyncMock(return_value=topic_payload),
            ):
                async with Client(mcp_server) as client:
                    result = await client.call_tool(
                        "get_topic_iceberg_linkage",
                        {
                            "catalog_name": "kafka_cat",
                            "schema_name": "default",
                            "topic_name": "orders_events",
                        },
                    )
                    payload = json.loads(result.content[0].text)
                    self.assertEqual("orders_events", payload["topic_name"])
                    self.assertTrue(payload["table_topic_enabled"])
                    self.assertEqual(
                        "iceberg_lakehouse", payload["iceberg_catalog"]
                    )
                    self.assertEqual("analytics", payload["iceberg_namespace"])
                    self.assertEqual("orders_events", payload["iceberg_table"])
                    self.assertEqual(
                        "mock_metalake.iceberg_lakehouse."
                        "analytics.orders_events",
                        payload["iceberg_ident"],
                    )
                    self.assertEqual("schema", payload["schema_type"])
                    self.assertEqual("5000", payload["commit_interval_ms"])
                    self.assertTrue(payload["upsert_enabled"])
                    self.assertEqual("order_id", payload["id_columns"])
                    self.assertEqual("day(event_ts)", payload["partition_by"])
                    self.assertEqual("op", payload["cdc_field"])
                    self.assertEqual("none", payload["errors_tolerance"])

        asyncio.run(_test(self.mcp))

    def test_topic_tool_annotations_and_whitespace_normalization(self):
        topic_payload = json.dumps(
            {
                "name": " orders_whitespace ",
                "properties": {
                    "automq.table.topic.enable": " true ",
                    "automq.table.topic.namespace": "   ",
                    "table-topic.iceberg-catalog": "  iceberg_lakehouse  ",
                    "automq.table.topic.schema.type": " by_schema ",
                    "automq.table.topic.id.columns": "   ",
                    "automq.table.topic.errors.tolerance": " invalid_data ",
                    "automq.table.topic.convert.key.type": " string ",
                    "automq.table.topic.convert.value.type": " by_schema_id ",
                    "automq.table.topic.transform.value.type": " debezium ",
                },
            }
        )

        async def _test(mcp_server):
            with patch.object(
                MockTopicOperation,
                "load_topic",
                new=AsyncMock(return_value=topic_payload),
            ):
                async with Client(mcp_server) as client:
                    tools = {t.name: t for t in await client.list_tools()}
                    for readonly_name in (
                        "list_of_topics",
                        "load_topic",
                        "get_topic_iceberg_linkage",
                    ):
                        ann = tools[readonly_name].annotations
                        self.assertIsNotNone(ann)
                        self.assertTrue(ann.readOnlyHint)
                        self.assertFalse(ann.destructiveHint)
                        self.assertTrue(ann.idempotentHint)

                    result = await client.call_tool(
                        "get_topic_iceberg_linkage",
                        {
                            "catalog_name": "kafka_cat",
                            "schema_name": "default",
                            "topic_name": "orders_whitespace",
                        },
                    )
                    payload = json.loads(result.content[0].text)
                    self.assertEqual("orders_whitespace", payload["topic_name"])
                    self.assertTrue(payload["table_topic_enabled"])
                    self.assertEqual(
                        "iceberg_lakehouse", payload["iceberg_catalog"]
                    )
                    self.assertEqual("default", payload["iceberg_namespace"])
                    self.assertEqual(
                        "orders_whitespace", payload["iceberg_table"]
                    )
                    self.assertEqual(
                        "mock_metalake.iceberg_lakehouse.default.orders_whitespace",
                        payload["iceberg_ident"],
                    )
                    self.assertIsNone(payload["id_columns"])
                    self.assertEqual(
                        "invalid_data", payload["errors_tolerance"]
                    )
                    self.assertEqual("string", payload["convert_key_type"])
                    self.assertEqual(
                        "by_schema_id", payload["convert_value_type"]
                    )
                    self.assertEqual(
                        "debezium", payload["transform_value_type"]
                    )

        asyncio.run(_test(self.mcp))

    def test_get_topic_iceberg_linkage_enabled_fallbacks(self):
        topic_payload = json.dumps(
            {
                "name": "orders_raw",
                "properties": {
                    "automq.table.topic.enable": "true",
                },
            }
        )

        async def _test(mcp_server):
            with patch.object(
                MockTopicOperation,
                "load_topic",
                new=AsyncMock(return_value=topic_payload),
            ):
                async with Client(mcp_server) as client:
                    result = await client.call_tool(
                        "get_topic_iceberg_linkage",
                        {
                            "catalog_name": "kafka_cat",
                            "schema_name": "default",
                            "topic_name": "orders_raw",
                        },
                    )
                    payload = json.loads(result.content[0].text)
                    self.assertEqual("orders_raw", payload["topic_name"])
                    self.assertTrue(payload["table_topic_enabled"])
                    self.assertIsNone(payload["iceberg_catalog"])
                    self.assertEqual("default", payload["iceberg_namespace"])
                    self.assertEqual("orders_raw", payload["iceberg_table"])
                    self.assertEqual(
                        "default.orders_raw", payload["iceberg_ident"]
                    )
                    self.assertFalse(payload["upsert_enabled"])

        asyncio.run(_test(self.mcp))

    def test_get_topic_iceberg_linkage_catalog_fallback_includes_metalake(self):
        topic_payload = json.dumps(
            {
                "name": "orders_raw",
                "properties": {
                    "automq.table.topic.enable": "true",
                    "table-topic.iceberg-catalog": "iceberg_lakehouse",
                },
            }
        )

        async def _test(mcp_server):
            with patch.object(
                MockTopicOperation,
                "load_topic",
                new=AsyncMock(return_value=topic_payload),
            ):
                async with Client(mcp_server) as client:
                    result = await client.call_tool(
                        "get_topic_iceberg_linkage",
                        {
                            "catalog_name": "kafka_cat",
                            "schema_name": "default",
                            "topic_name": "orders_raw",
                        },
                    )
                    payload = json.loads(result.content[0].text)
                    self.assertEqual(
                        "iceberg_lakehouse", payload["iceberg_catalog"]
                    )
                    self.assertEqual("default", payload["iceberg_namespace"])
                    self.assertEqual("orders_raw", payload["iceberg_table"])
                    self.assertEqual(
                        "mock_metalake.iceberg_lakehouse.default.orders_raw",
                        payload["iceberg_ident"],
                    )

        asyncio.run(_test(self.mcp))

    def test_get_topic_iceberg_linkage_disabled(self):
        topic_payload = json.dumps(
            {
                "name": "raw_clicks",
                "properties": {
                    "compression.type": "producer",
                    "automq.table.topic.enable": "false",
                    "automq.table.topic.namespace": "stale_ns",
                    "table-topic.iceberg-catalog": "stale_cat",
                },
            }
        )

        async def _test(mcp_server):
            with patch.object(
                MockTopicOperation,
                "load_topic",
                new=AsyncMock(return_value=topic_payload),
            ):
                async with Client(mcp_server) as client:
                    result = await client.call_tool(
                        "get_topic_iceberg_linkage",
                        {
                            "catalog_name": "kafka_cat",
                            "schema_name": "default",
                            "topic_name": "raw_clicks",
                        },
                    )
                    payload = json.loads(result.content[0].text)
                    self.assertEqual("raw_clicks", payload["topic_name"])
                    self.assertFalse(payload["table_topic_enabled"])
                    self.assertIsNone(payload["iceberg_catalog"])
                    self.assertIsNone(payload["iceberg_namespace"])
                    self.assertIsNone(payload["iceberg_table"])
                    self.assertIsNone(payload["iceberg_ident"])
                    self.assertFalse(payload["upsert_enabled"])

        asyncio.run(_test(self.mcp))
