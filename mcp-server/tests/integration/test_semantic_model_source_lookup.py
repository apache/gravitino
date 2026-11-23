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

"""Discover source-associated datasets through real MCP and Gravitino servers."""

import asyncio
import json
from uuid import uuid4

import httpx
import pytest
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
from fastmcp.exceptions import ToolError

from tests.integration.gravitino_setup import basic_auth_header

# Pytest injects fixtures by argument name.
# pylint: disable=redefined-outer-name

_ALIASES = ("Orders.raw", "orders.raw", "\U00010000", "\ue000")


class SourceLookupFixture:
    """Own source objects, cross-catalog models, and grants for one test."""

    def __init__(self, env, rest_client):
        self.env = env
        self.rest_client = rest_client
        self.metalake = "source_mcp_" + uuid4().hex
        self.base = f"/api/metalakes/{self.metalake}"

    def request(self, method, path, body=None):
        response = self.rest_client.request(method, path, json=body)
        response.raise_for_status()
        return response.json()

    def provision(self, warehouse):
        for name, provider, properties in (
            (
                "sources",
                "lakehouse-iceberg",
                {
                    "catalog-backend": "jdbc",
                    "warehouse": warehouse.as_uri(),
                    "uri": f"jdbc:h2:mem:{self.metalake};DB_CLOSE_DELAY=-1;MODE=MYSQL",
                    "jdbc-driver": "org.h2.Driver",
                    "jdbc-initialize": "true",
                },
            ),
            ("models", "lakehouse-generic", {}),
        ):
            self.request(
                "POST",
                self.base + "/catalogs",
                {
                    "name": name,
                    "type": "RELATIONAL",
                    "provider": provider,
                    "properties": properties,
                },
            )
        for catalog, schema in (
            ("sources", "raw"),
            ("models", "semantic"),
            ("models", "other"),
        ):
            self.request(
                "POST",
                self.base + f"/catalogs/{catalog}/schemas",
                {"name": schema, "properties": {}},
            )
        source_path = self.base + "/catalogs/sources/schemas/raw"
        for name in ("orders", "unused"):
            self.request(
                "POST",
                source_path + "/tables",
                {
                    "name": name,
                    "columns": [{"name": "amount", "type": "long"}],
                    "properties": {},
                },
            )
        self.request(
            "POST",
            source_path + "/views",
            {
                "name": "order_view",
                "columns": [{"name": "amount", "type": "long"}],
                "representations": [
                    {
                        "type": "sql",
                        "dialect": "trino",
                        "sql": "SELECT amount FROM sources.raw.orders",
                    }
                ],
                "defaultCatalog": "sources",
                "defaultSchema": "raw",
                "properties": {},
            },
        )
        for schema, model, source, aliases in (
            ("semantic", "a", "orders", tuple(reversed(_ALIASES))),
            ("other", "b", "orders", ("cross_schema",)),
            ("semantic", "hidden", "orders", ("hidden",)),
            ("semantic", "from_view", "order_view", ("view.raw",)),
        ):
            self.request(
                "POST",
                self.base
                + f"/catalogs/models/schemas/{schema}/semantic-models",
                {
                    "name": model,
                    "definition": {
                        "datasets": [
                            {
                                "name": alias,
                                "source": {
                                    "namespace": ["sources", "raw"],
                                    "name": source,
                                },
                            }
                            for alias in aliases
                        ]
                    },
                },
            )

    def call(
        self, source_type="table", source_name="orders", principal="admin"
    ):
        async def invoke():
            transport = StreamableHttpTransport(
                url=self.env["mcp_url"],
                headers={"Authorization": basic_auth_header(principal)},
            )
            async with Client(transport) as client:
                result = await client.call_tool(
                    "list_semantic_model_datasets",
                    {
                        "metalake": self.metalake,
                        "source_type": source_type,
                        "catalog_name": "sources",
                        "schema_name": "raw",
                        "source_name": source_name,
                    },
                )
                return json.loads(result.content[0].text)

        return asyncio.run(invoke())

    def identifier(self, schema, model, alias):
        return {
            "namespace": [self.metalake, "models", schema, model],
            "name": alias,
        }

    def change_privilege(self, action, object_type, name, privilege):
        self.request(
            "PUT",
            self.base
            + f"/permissions/roles/source_reader/{object_type}/{name}/{action}",
            {"privileges": [{"name": privilege, "condition": "ALLOW"}]},
        )


@pytest.fixture
def source_fixture(integration_env, tmp_path):
    with httpx.Client(
        base_url=integration_env["gravitino_uri"],
        timeout=30.0,
        headers={"Authorization": basic_auth_header("admin")},
    ) as client:
        fixture = SourceLookupFixture(integration_env, client)
        fixture.request(
            "POST",
            "/api/metalakes",
            {"name": fixture.metalake, "properties": {}},
        )
        try:
            fixture.provision(tmp_path)
            yield fixture
        finally:
            fixture.request("DELETE", fixture.base + "?force=true")


class TestSemanticModelSourceLookup:
    """Verify direct source discovery and per-caller visibility over HTTP."""

    @pytest.mark.parametrize("source_type", ("table", "view"))
    def test_identifiers_order_and_rest_parity(
        self, source_fixture, source_type
    ):
        fixture = source_fixture
        if source_type == "table":
            source_name = "orders"
            expected = [fixture.identifier("other", "b", "cross_schema")]
            expected += [
                fixture.identifier("semantic", "a", alias) for alias in _ALIASES
            ]
            expected += [fixture.identifier("semantic", "hidden", "hidden")]
        else:
            source_name = "order_view"
            expected = [fixture.identifier("semantic", "from_view", "view.raw")]
        actual = fixture.call(source_type, source_name)
        assert actual == expected
        response = fixture.request(
            "GET",
            fixture.base
            + f"/objects/{source_type}/sources.raw.{source_name}/semantic-model-datasets",
        )
        assert actual == response["identifiers"]

    def test_model_visibility_parent_access_and_source_denial(
        self, source_fixture
    ):
        fixture = source_fixture
        fixture.request(
            "POST", fixture.base + "/users", {"name": "source_reader"}
        )
        fixture.request(
            "POST",
            fixture.base + "/roles",
            {"name": "source_reader", "properties": {}, "securableObjects": []},
        )
        fixture.request(
            "PUT",
            fixture.base + "/permissions/users/source_reader/grant",
            {"roleNames": ["source_reader"]},
        )
        for object_type, name, privilege in (
            ("catalog", "sources", "USE_CATALOG"),
            ("schema", "sources.raw", "USE_SCHEMA"),
            ("table", "sources.raw.orders", "SELECT_TABLE"),
            ("catalog", "models", "USE_CATALOG"),
            ("schema", "models.semantic", "USE_SCHEMA"),
        ):
            fixture.change_privilege("grant", object_type, name, privilege)
        assert fixture.call(principal="source_reader") == []
        fixture.change_privilege(
            "grant", "semantic_model", "models.semantic.a", "USE_SEMANTIC_MODEL"
        )
        expected = [
            fixture.identifier("semantic", "a", alias) for alias in _ALIASES
        ]
        assert fixture.call(principal="source_reader") == expected
        fixture.change_privilege(
            "revoke", "schema", "models.semantic", "USE_SCHEMA"
        )
        assert fixture.call(principal="source_reader") == []
        fixture.change_privilege(
            "grant", "schema", "models.semantic", "USE_SCHEMA"
        )
        assert fixture.call(principal="source_reader") == expected
        fixture.change_privilege(
            "revoke", "table", "sources.raw.orders", "SELECT_TABLE"
        )
        with pytest.raises(ToolError, match="ForbiddenException"):
            fixture.call(principal="source_reader")

    def test_empty_result_and_missing_sources(self, source_fixture):
        assert source_fixture.call(source_name="unused") == []
        for source_type in ("table", "view"):
            with pytest.raises(
                ToolError, match="NoSuchMetadataObjectException"
            ):
                source_fixture.call(source_type, "missing")
