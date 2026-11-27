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


"""Checks MCP ontology tool registration and native REST calls."""

import json
from unittest import IsolatedAsyncioTestCase

import httpx
from fastmcp import FastMCP

from mcp_server.client.plain.exception import GravitinoException
from mcp_server.client.plain.plain_rest_client_ontology_operation import (
    PlainRESTClientOntologyOperation,
)
from mcp_server.tools.ontology import load_ontology_tools


class TestOntologyOperations(IsolatedAsyncioTestCase):
    """Exercises read and write operations through the MCP client."""

    async def test_model_mapping_paths_and_error(self):
        requests = []

        def handle(request):
            requests.append(request)
            if request.method == "POST":
                return httpx.Response(
                    200, json={"code": 0, "ontologyMapping": {"name": "map"}}
                )
            if request.method == "DELETE":
                return httpx.Response(
                    409,
                    json={
                        "code": 1010,
                        "type": "OntologyInUseException",
                        "message": "used by a mapping",
                    },
                )
            return httpx.Response(200, json={"code": 0, "identifiers": []})

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handle),
            base_url="http://example.test",
        ) as client:
            ops = PlainRESTClientOntologyOperation("m", client)
            result = await ops.create_ontology_mapping(
                "my catalog",
                "s",
                "model",
                "map",
                "Sales",
                {"conceptMappings": []},
            )
            self.assertEqual({"name": "map"}, json.loads(result))
            self.assertEqual(
                b"/api/metalakes/m/catalogs/my%20catalog/schemas/s/semantic-models/model/ontology-mappings",
                requests[0].url.raw_path,
            )
            self.assertEqual(
                "Sales", json.loads(requests[0].content)["ontology"]
            )
            self.assertEqual(
                [], json.loads(await ops.list_mappings_by_ontology("Sales"))
            )
            with self.assertRaisesRegex(
                GravitinoException, "OntologyInUseException"
            ):
                await ops.drop_ontology("Sales")

    async def test_reverse_mapping_identifiers_are_relative_to_the_metalake(
        self,
    ):
        relative = ("c", "s", "model")
        for namespace in (("m", *relative), relative):

            def handle(request, namespace=namespace):
                self.assertEqual(
                    "/api/metalakes/m/ontologies/Sales/mappings",
                    request.url.path,
                )
                return httpx.Response(
                    200,
                    json={
                        "code": 0,
                        "identifiers": [
                            {"namespace": namespace, "name": "map"}
                        ],
                    },
                )

            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handle),
                base_url="http://example.test",
            ) as client:
                ops = PlainRESTClientOntologyOperation("m", client)
                self.assertEqual(
                    [{"namespace": list(relative), "name": "map"}],
                    json.loads(await ops.list_mappings_by_ontology("Sales")),
                )

    async def test_registers_lifecycle_tools_with_ontology_tag(self):
        server = FastMCP("ontology test")
        load_ontology_tools(server)
        tools = await server.list_tools()
        self.assertEqual(
            {
                "list_ontologies",
                "load_ontology",
                "create_ontology",
                "alter_ontology",
                "drop_ontology",
                "list_ontology_mappings",
                "load_ontology_mapping",
                "create_ontology_mapping",
                "alter_ontology_mapping",
                "drop_ontology_mapping",
                "list_mappings_by_ontology",
            },
            {tool.name for tool in tools},
        )
        self.assertTrue(all(tool.tags == {"ontology"} for tool in tools))
