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

"""Discover metrics through the live MCP HTTP server and Gravitino REST API."""

import asyncio
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest
import simplejson
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
from fastmcp.exceptions import ToolError

from tests.integration.gravitino_setup import basic_auth_header

# Pytest injects fixtures by argument name.
# pylint: disable=redefined-outer-name


class MetricDiscoveryFixture:
    """Isolated Semantic Models and role grants for each live-server test."""

    def __init__(self, env, rest_client):
        self.env = env
        self.rest_client = rest_client
        self.metalake = "metric_mcp_" + uuid4().hex
        self.base = f"/api/metalakes/{self.metalake}"
        self.models_path = (
            self.base
            + "/catalogs/metrics_catalog/schemas/semantic/semantic-models"
        )
        self.revenue = self.metric("Revenue")
        self.lower_revenue = self.metric("revenue")

    def request(self, method, path, body=None):
        response = self.rest_client.request(
            method,
            path,
            content=(
                None
                if body is None
                else simplejson.dumps(body, use_decimal=True)
            ),
        )
        response.raise_for_status()
        return simplejson.loads(response.content, use_decimal=True)

    def provision(self):
        self.request(
            "POST",
            self.base + "/catalogs",
            {
                "name": "metrics_catalog",
                "type": "RELATIONAL",
                "provider": "lakehouse-generic",
                "comment": "",
                "properties": {},
            },
        )
        schemas_path = self.base + "/catalogs/metrics_catalog/schemas"
        for schema in ("source", "semantic", "other", "empty"):
            self.request(
                "POST",
                schemas_path,
                {"name": schema, "comment": "", "properties": {}},
            )
        self.request(
            "POST",
            schemas_path + "/source/tables",
            {
                "name": "orders",
                "columns": [{"name": "amount", "type": "long"}],
                "properties": {
                    "format": "delta",
                    "external": "true",
                    "location": f"file:///tmp/{self.metalake}/orders",
                },
            },
        )
        for model, metrics in (
            ("sales", [self.lower_revenue, self.revenue]),
            ("hidden", [self.revenue]),
            ("no_metrics", []),
        ):
            self.request(
                "POST",
                self.models_path,
                {
                    "name": model,
                    "definition": self.definition(metrics),
                    "properties": {},
                },
            )
        self.request(
            "POST",
            schemas_path + "/other/semantic-models",
            {
                "name": "sales",
                "definition": self.definition([self.metric("other_schema")]),
            },
        )
        self.request("POST", self.base + "/users", {"name": "metric_reader"})
        self.request(
            "POST",
            self.base + "/roles",
            {
                "name": "metric_reader",
                "properties": {},
                "securableObjects": [
                    {
                        "fullName": "metrics_catalog",
                        "type": "CATALOG",
                        "privileges": [
                            {"name": "USE_CATALOG", "condition": "ALLOW"}
                        ],
                    },
                    {
                        "fullName": "metrics_catalog.semantic",
                        "type": "SCHEMA",
                        "privileges": [
                            {"name": "USE_SCHEMA", "condition": "ALLOW"}
                        ],
                    },
                ],
            },
        )
        self.request(
            "PUT",
            self.base + "/permissions/users/metric_reader/grant",
            {"roleNames": ["metric_reader"]},
        )

    def call(self, tool, principal="admin", schema="semantic"):
        async def invoke():
            transport = StreamableHttpTransport(
                url=self.env["mcp_url"],
                headers={"Authorization": basic_auth_header(principal)},
            )
            async with Client(transport) as client:
                result = await client.call_tool(
                    tool,
                    {
                        "metalake": self.metalake,
                        "catalog_name": "metrics_catalog",
                        "schema_name": schema,
                    },
                )
                return simplejson.loads(
                    result.content[0].text, use_decimal=True
                )

        return asyncio.run(invoke())

    def assert_discovery(self, expected, principal="admin"):
        assert self.call("list_metric_infos", principal) == expected
        assert self.call("list_metrics", principal) == [
            {"namespace": info["namespace"], "name": info["metric"]["name"]}
            for info in expected
        ]

    def info(self, model, metric):
        return {
            "namespace": [self.metalake, "metrics_catalog", "semantic", model],
            "metric": metric,
        }

    @staticmethod
    def definition(metrics):
        return {
            "datasets": [
                {
                    "name": "orders",
                    "source": {
                        "namespace": ["metrics_catalog", "source"],
                        "name": "orders",
                    },
                }
            ],
            "metrics": metrics,
        }

    @staticmethod
    def metric(name):
        return {
            "name": name,
            "expression": {
                "dialects": [
                    {"dialect": "ANSI_SQL", "expression": "SUM(orders.amount)"}
                ]
            },
            "description": "Revenue - café 雪",
            "datatype": "Decimal",
            "aiContext": {
                "instructions": "Use certified values",
                "nested": {
                    "precise": Decimal("0.12345678901234567890123456789"),
                    "large": 1234567890123456789012345678901234567890,
                    "values": [True, None, "extra"],
                },
            },
            "customExtensions": [
                {"vendorName": "vendor", "data": '{"opaque": "雪"}'}
            ],
        }


@pytest.fixture
def metric_fixture(integration_env):
    with httpx.Client(
        base_url=integration_env["gravitino_uri"],
        timeout=30.0,
        headers={
            "Authorization": basic_auth_header("admin"),
            "Content-Type": "application/json",
        },
    ) as client:
        fixture = MetricDiscoveryFixture(integration_env, client)
        fixture.request(
            "POST",
            "/api/metalakes",
            {"name": fixture.metalake, "properties": {}},
        )
        try:
            fixture.provision()
            yield fixture
        finally:
            fixture.request("DELETE", fixture.base + "?force=true")


class TestSemanticModelMetricDiscovery:
    """Verify both metric tools across real MCP and REST transports."""

    def test_complete_metrics_order_and_schema_isolation(self, metric_fixture):
        fixture = metric_fixture
        fixture.assert_discovery(
            [
                fixture.info("hidden", fixture.revenue),
                fixture.info("sales", fixture.revenue),
                fixture.info("sales", fixture.lower_revenue),
            ]
        )
        for tool in ("list_metrics", "list_metric_infos"):
            assert fixture.call(tool, schema="empty") == []
        actual = fixture.call("list_metric_infos", schema="other")
        assert len(actual) == 1
        assert actual[0]["namespace"] == [
            fixture.metalake,
            "metrics_catalog",
            "other",
            "sales",
        ]
        assert actual[0]["metric"]["name"] == "other_schema"

    def test_model_visibility_grant_revoke_and_parent_access(
        self, metric_fixture
    ):
        fixture = metric_fixture
        fixture.assert_discovery([], "metric_reader")
        expected = [
            fixture.info("sales", fixture.revenue),
            fixture.info("sales", fixture.lower_revenue),
        ]
        permission_path = (
            fixture.base
            + "/permissions/roles/metric_reader/semantic_model/metrics_catalog.semantic.sales"
        )
        for privilege in ("USE_SEMANTIC_MODEL", "MODIFY_SEMANTIC_MODEL"):
            body = {"privileges": [{"name": privilege, "condition": "ALLOW"}]}
            fixture.request("PUT", permission_path + "/grant", body)
            fixture.assert_discovery(expected, "metric_reader")
            fixture.request("PUT", permission_path + "/revoke", body)
            fixture.assert_discovery([], "metric_reader")
        fixture.request(
            "PUT",
            fixture.base
            + "/owners/semantic_model/metrics_catalog.semantic.sales",
            {"name": "metric_reader", "type": "USER"},
        )
        fixture.assert_discovery(expected, "metric_reader")
        for object_type, object_name, privilege in (
            ("schema", "metrics_catalog.semantic", "USE_SCHEMA"),
            ("catalog", "metrics_catalog", "USE_CATALOG"),
        ):
            path = (
                fixture.base
                + f"/permissions/roles/metric_reader/{object_type}/{object_name}"
            )
            body = {"privileges": [{"name": privilege, "condition": "ALLOW"}]}
            fixture.request("PUT", path + "/revoke", body)
            for tool in ("list_metrics", "list_metric_infos"):
                with pytest.raises(ToolError, match="ForbiddenException"):
                    fixture.call(tool, "metric_reader")
            fixture.request("PUT", path + "/grant", body)
            fixture.assert_discovery(expected, "metric_reader")

    def test_definition_replacement_rename_and_drop(self, metric_fixture):
        fixture = metric_fixture
        fixture.request("DELETE", fixture.models_path + "/hidden")
        changed = {
            "name": "Revenue",
            "expression": {
                "dialects": [{"dialect": "ANSI_SQL", "expression": "COUNT(*)"}]
            },
            "aiContext": "Updated",
        }
        fixture.request(
            "PUT",
            fixture.models_path + "/sales",
            {
                "updates": [
                    {
                        "@type": "replaceDefinition",
                        "definition": fixture.definition([changed]),
                    },
                    {"@type": "rename", "newName": "renamed"},
                ]
            },
        )
        fixture.assert_discovery([fixture.info("renamed", changed)])
        fixture.request("DELETE", fixture.models_path + "/renamed")
        fixture.assert_discovery([])

    def test_missing_schema_is_a_tool_error(self, metric_fixture):
        for tool in ("list_metrics", "list_metric_infos"):
            with pytest.raises(ToolError, match="NoSuchSchemaException"):
                metric_fixture.call(tool, schema="missing")
