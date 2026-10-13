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

"""Semantic Model tools exercised through FastMCP with REST and stub clients."""

import json
import unittest
from unittest.mock import patch

import httpx
from fastmcp import Client
from fastmcp.exceptions import ToolError

from mcp_server.client.factory import RESTClientFactory
from mcp_server.client.plain.plain_rest_client_operation import (
    PlainRESTClientOperation,
)
from mcp_server.core import Setting
from mcp_server.server import GravitinoMCPServer
from tests.unit.tools import MockOperation

_NAMESPACE = {"catalog_name": "sales", "schema_name": "mart"}
_MODEL_ARGS = {**_NAMESPACE, "semantic_model_name": "sales_model"}
_MODELS_PATH = (
    "/api/metalakes/test_metalake/catalogs/sales/schemas/mart/semantic-models"
)


class TestSemanticModelTool(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.definition = {
            "aiContext": {"instructions": "Use certified sales definitions"},
            "datasets": [
                {
                    "name": "orders",
                    "source": {
                        "namespace": ["sales", "mart"],
                        "name": "orders",
                    },
                    "primaryKey": ["order_id"],
                    "fields": [],
                }
            ],
            "relationships": [],
            "metrics": [],
            "customExtensions": [{"vendorName": "example", "data": "{}"}],
        }
        self.model = {
            "name": "sales_model",
            "comment": "Sales definitions",
            "definition": self.definition,
            "properties": {"domain": "sales"},
            "audit": {"creator": "admin", "createTime": "2026-10-13T00:00:00Z"},
        }
        self.response = {"code": 0, "semanticModel": self.model}
        self.status = 200
        self.requests = []
        self.http_clients = []
        factory_patch = patch.object(
            RESTClientFactory, "_rest_client_class", self._create_client
        )
        factory_patch.start()
        self.addCleanup(factory_patch.stop)
        self.mcp = GravitinoMCPServer(
            Setting("test_metalake", token="semantic-test-token")
        ).mcp

    async def asyncTearDown(self):
        for client in self.http_clients:
            await client.aclose()

    def _respond(self, request):
        self.requests.append(request)
        return httpx.Response(self.status, json=self.response)

    def _create_client(self, *args, **kwargs):
        # Keep the production factory/client wiring and inject only transport.
        async_client_type = httpx.AsyncClient

        def create_http_client(*client_args, **client_kwargs):
            client = async_client_type(
                *client_args,
                transport=httpx.MockTransport(self._respond),
                **client_kwargs,
            )
            self.http_clients.append(client)
            return client

        with patch(
            "mcp_server.client.plain.plain_rest_client_operation.httpx.AsyncClient",
            side_effect=create_http_client,
        ):
            return PlainRESTClientOperation(*args, **kwargs)

    async def _call(self, name, arguments):
        async with Client(self.mcp) as client:
            result = await client.call_tool(name, arguments)
            return json.loads(result.content[0].text)

    def _assert_request(self, method, path, body=None):
        request = self.requests[-1]
        self.assertEqual(request.method, method)
        self.assertEqual(request.url.raw_path.decode(), path)
        self.assertEqual(
            request.headers["Authorization"], "Bearer semantic-test-token"
        )
        if body is None:
            self.assertEqual(request.content, b"")
        else:
            self.assertEqual(json.loads(request.content), body)

    async def test_list_semantic_models(self):
        for identifiers in (
            [
                {
                    "namespace": ["test_metalake", "sales", "mart"],
                    "name": "sales_model",
                }
            ],
            [],
        ):
            with self.subTest(identifiers=identifiers):
                self.response = {"code": 0, "identifiers": identifiers}
                result = await self._call("list_of_semantic_models", _NAMESPACE)
                self.assertEqual(result, identifiers)
                self._assert_request("GET", _MODELS_PATH)

    async def test_load_semantic_model_preserves_native_definition(self):
        result = await self._call("load_semantic_model", _MODEL_ARGS)
        self.assertEqual(result, self.model)
        self._assert_request("GET", f"{_MODELS_PATH}/sales_model")

    async def test_create_semantic_model_optional_fields(self):
        required = {"name": "sales_model", "definition": self.definition}
        for optional, expected in (
            ({}, {}),
            ({"comment": None, "properties": None}, {}),
            (
                {"comment": "", "properties": {}},
                {"comment": "", "properties": {}},
            ),
            (
                {
                    "comment": "Sales definitions",
                    "properties": {"domain": "sales"},
                },
                {
                    "comment": "Sales definitions",
                    "properties": {"domain": "sales"},
                },
            ),
        ):
            with self.subTest(optional=optional):
                result = await self._call(
                    "create_semantic_model",
                    {**_NAMESPACE, **required, **optional},
                )
                self.assertEqual(result, self.model)
                self._assert_request(
                    "POST",
                    _MODELS_PATH,
                    {**required, **expected},
                )

    async def test_alter_semantic_model_preserves_order_and_null_comment(self):
        updates = [
            {"@type": "rename", "newName": "sales_model_v2"},
            {"@type": "updateComment", "newComment": None},
            {"@type": "setProperty", "property": "domain", "value": "sales"},
            {"@type": "removeProperty", "property": "legacy"},
            {"@type": "replaceDefinition", "definition": self.definition},
        ]
        self.model["name"] = "sales_model_v2"
        self.model["comment"] = None
        result = await self._call(
            "alter_semantic_model", {**_MODEL_ARGS, "updates": updates}
        )
        self.assertEqual(result, self.model)
        self._assert_request(
            "PUT", f"{_MODELS_PATH}/sales_model", {"updates": updates}
        )

    async def test_drop_semantic_model(self):
        for dropped in (True, False):
            with self.subTest(dropped=dropped):
                self.response = {"code": 0, "dropped": dropped}
                result = await self._call("drop_semantic_model", _MODEL_ARGS)
                self.assertIs(result, dropped)
                self._assert_request("DELETE", f"{_MODELS_PATH}/sales_model")

    async def test_all_operations_encode_each_path_segment(self):
        namespace = {
            "metalake": "ml/a",
            "catalog_name": "cat?",
            "schema_name": "sch#",
        }
        model_name = "sales %"
        models_path = "/api/metalakes/ml%2Fa/catalogs/cat%3F/schemas/sch%23/semantic-models"
        for tool, method, extra, suffix in (
            ("list_of_semantic_models", "GET", {}, ""),
            (
                "create_semantic_model",
                "POST",
                {"name": model_name, "definition": self.definition},
                "",
            ),
            (
                "load_semantic_model",
                "GET",
                {"semantic_model_name": model_name},
                "/sales%20%25",
            ),
            (
                "alter_semantic_model",
                "PUT",
                {
                    "semantic_model_name": model_name,
                    "updates": [
                        {"@type": "updateComment", "newComment": "new"}
                    ],
                },
                "/sales%20%25",
            ),
            (
                "drop_semantic_model",
                "DELETE",
                {"semantic_model_name": model_name},
                "/sales%20%25",
            ),
        ):
            with self.subTest(tool=tool):
                await self._call(tool, {**namespace, **extra})
                request = self.requests[-1]
                self.assertEqual(request.method, method)
                self.assertEqual(
                    request.url.raw_path.decode(), models_path + suffix
                )
                if tool == "create_semantic_model":
                    self.assertEqual(
                        json.loads(request.content)["name"], model_name
                    )

    async def test_service_and_request_identity_and_metalake(self):
        async with Client(self.mcp) as client:
            with patch(
                "mcp_server.core.context._get_request_authorization",
                return_value="Bearer request-test-token",
            ):
                await client.call_tool(
                    "load_semantic_model",
                    {**_MODEL_ARGS, "metalake": "other_ml"},
                )
            await client.call_tool("load_semantic_model", _MODEL_ARGS)

        self.assertEqual(
            self.requests[0].url.path,
            _MODELS_PATH.replace("test_metalake", "other_ml") + "/sales_model",
        )
        self.assertEqual(
            self.requests[0].headers["Authorization"],
            "Bearer request-test-token",
        )
        self._assert_request("GET", f"{_MODELS_PATH}/sales_model")

    async def test_ambiguous_path_names_never_send_a_request(self):
        for argument in (
            "metalake",
            "catalog_name",
            "schema_name",
            "semantic_model_name",
        ):
            # An empty per-call metalake selects the configured default.
            invalid_names = (
                (".", "..") if argument == "metalake" else ("", ".", "..")
            )
            for name in invalid_names:
                with self.subTest(argument=argument, name=name):
                    with self.assertRaisesRegex(ToolError, "REST path names"):
                        await self._call(
                            "drop_semantic_model",
                            {**_MODEL_ARGS, argument: name},
                        )
        self.assertEqual(self.requests, [])

    async def test_dots_within_names_are_preserved(self):
        await self._call(
            "load_semantic_model",
            {**_MODEL_ARGS, "semantic_model_name": "sales.v2"},
        )
        self._assert_request("GET", f"{_MODELS_PATH}/sales.v2")

    async def test_rest_errors_reach_the_mcp_caller(self):
        for tool, arguments, status, error_type in (
            (
                "list_of_semantic_models",
                _NAMESPACE,
                403,
                "ForbiddenException",
            ),
            (
                "load_semantic_model",
                _MODEL_ARGS,
                404,
                "NoSuchSemanticModelException",
            ),
            (
                "create_semantic_model",
                {
                    **_NAMESPACE,
                    "name": "sales_model",
                    "definition": self.definition,
                },
                409,
                "SemanticModelAlreadyExistsException",
            ),
            (
                "alter_semantic_model",
                {**_MODEL_ARGS, "updates": []},
                400,
                "IllegalArgumentException",
            ),
            ("drop_semantic_model", _MODEL_ARGS, 409, "NotInUseException"),
            ("drop_semantic_model", _MODEL_ARGS, 403, "ForbiddenException"),
        ):
            with self.subTest(tool=tool, status=status):
                self.status = status
                self.response = {
                    "code": status,
                    "type": error_type,
                    "message": "Semantic Model operation rejected",
                }
                with self.assertRaisesRegex(ToolError, error_type):
                    await self._call(tool, arguments)

    async def test_create_requires_definition_before_sending_request(self):
        with self.assertRaisesRegex(ToolError, "definition"):
            await self._call(
                "create_semantic_model",
                {**_NAMESPACE, "name": "sales_model"},
            )
        self.assertEqual(self.requests, [])

    async def test_semantic_model_tag_exposes_all_five_tools(self):
        self.mcp = GravitinoMCPServer(
            Setting("test_metalake", tags={"semantic_model"})
        ).mcp
        async with Client(self.mcp) as client:
            tools = {tool.name: tool for tool in await client.list_tools()}
        self.assertEqual(
            set(tools),
            {
                "list_of_semantic_models",
                "load_semantic_model",
                "create_semantic_model",
                "alter_semantic_model",
                "drop_semantic_model",
            },
        )
        for tool in tools.values():
            self.assertIn("metalake", tool.inputSchema["properties"])
        self.assertEqual(
            set(tools["create_semantic_model"].inputSchema["required"]),
            {"catalog_name", "schema_name", "name", "definition"},
        )
        self.assertEqual(
            set(tools["drop_semantic_model"].inputSchema["required"]),
            {"catalog_name", "schema_name", "semantic_model_name"},
        )


class TestSemanticModelToolWithMockOperation(unittest.IsolatedAsyncioTestCase):
    async def test_all_tools_return_json_from_mock_operation(self):
        model_args = {
            **_NAMESPACE,
            "semantic_model_name": "mock_semantic_model",
        }
        model = {"name": "mock_semantic_model"}
        cases = (
            ("list_of_semantic_models", _NAMESPACE, []),
            ("load_semantic_model", model_args, model),
            (
                "create_semantic_model",
                {**_NAMESPACE, "name": "mock_semantic_model", "definition": {}},
                model,
            ),
            (
                "alter_semantic_model",
                {
                    **model_args,
                    "updates": [
                        {"@type": "updateComment", "newComment": "Updated"}
                    ],
                },
                model,
            ),
            ("drop_semantic_model", model_args, True),
        )
        with patch.object(
            RESTClientFactory, "_rest_client_class", MockOperation
        ):
            mcp = GravitinoMCPServer(
                Setting("mock_metalake", tags={"semantic_model"})
            ).mcp
            async with Client(mcp) as client:
                for tool, arguments, expected in cases:
                    with self.subTest(tool=tool):
                        result = await client.call_tool(tool, arguments)
                        self.assertEqual(
                            json.loads(result.content[0].text), expected
                        )
