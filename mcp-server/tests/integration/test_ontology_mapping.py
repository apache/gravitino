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

"""Create and discover an ontology mapping through real MCP and Gravitino servers."""

import asyncio
import json
from uuid import uuid4

import httpx
import pytest
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport

from tests.integration.gravitino_setup import basic_auth_header

_CATALOG = "sales"
_SCHEMA = "reporting"
_MODEL = "customers_model"


@pytest.fixture(name="mapping_metalake")
def provision_mapping_model(integration_env, tmp_path):
    """Create a semantic model with a customer dataset in an isolated metalake."""
    metalake = "mapping_mcp_" + uuid4().hex
    base = f"/api/metalakes/{metalake}"
    schemas = f"{base}/catalogs/{_CATALOG}/schemas"
    with httpx.Client(
        base_url=integration_env["gravitino_uri"],
        headers={"Authorization": basic_auth_header("admin")},
        timeout=30.0,
    ) as rest:

        def create(path, **body):
            rest.post(path, json=body).raise_for_status()

        create("/api/metalakes", name=metalake, properties={})
        try:
            create(
                f"{base}/catalogs",
                name=_CATALOG,
                type="RELATIONAL",
                provider="lakehouse-generic",
                properties={},
            )
            create(schemas, name=_SCHEMA, properties={})
            create(
                f"{schemas}/{_SCHEMA}/tables",
                name="customers",
                columns=[{"name": "id", "type": "long"}],
                properties={
                    "format": "delta",
                    "external": "true",
                    "location": (tmp_path / "customers").as_uri(),
                },
            )
            source = {"namespace": [_CATALOG, _SCHEMA], "name": "customers"}
            create(
                f"{schemas}/{_SCHEMA}/semantic-models",
                name=_MODEL,
                definition={
                    "datasets": [{"name": "customer", "source": source}]
                },
            )
            yield metalake
        finally:
            rest.delete(base, params={"force": "true"}).raise_for_status()


def test_mapping_is_listed_by_model_and_ontology(
    integration_env, mapping_metalake
):
    """Find the mapping through both tool routes and verify its namespace."""
    transport = StreamableHttpTransport(
        url=integration_env["mcp_url"],
        headers={"Authorization": basic_auth_header("admin")},
    )

    async def exercise():
        async with Client(transport) as client:

            async def call(tool, **arguments):
                result = await client.call_tool(
                    tool, {"metalake": mapping_metalake, **arguments}
                )
                return json.loads(result.content[0].text)

            customer_id = {
                "concept": "CustomerId",
                "type": "ValueType",
                "extends": ["Integer"],
            }
            await call(
                "create_ontology",
                name="Sales",
                definition={"ontology": [customer_id]},
            )
            model = {
                "catalog_name": _CATALOG,
                "schema_name": _SCHEMA,
                "semantic_model_name": _MODEL,
            }
            concept_mapping = {
                "concept": "CustomerId",
                "objectMappings": [{"expression": "customer.id"}],
            }
            created = await call(
                "create_ontology_mapping",
                **model,
                name="customer_mapping",
                ontology="Sales",
                definition={"conceptMappings": [concept_mapping]},
            )
            assert created["name"] == "customer_mapping"
            assert created["ontology"] == "Sales"

            namespace = [_CATALOG, _SCHEMA, _MODEL]
            assert await call("list_ontology_mappings", **model) == [
                {
                    "namespace": [mapping_metalake, *namespace],
                    "name": created["name"],
                }
            ]
            assert await call("list_mappings_by_ontology", name="Sales") == [
                {"namespace": namespace, "name": created["name"]}
            ]

    asyncio.run(exercise())
