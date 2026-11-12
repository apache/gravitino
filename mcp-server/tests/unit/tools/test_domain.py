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

"""Read-only Domain tools exercised through FastMCP and the REST client."""

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


class TestDomainTool(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.relations = [
            {"relation": "direct"},
            {
                "relation": "inherited",
                "source": {"fullName": "cat.schema", "type": "schema"},
            },
            {
                "relation": "via_subdomain",
                "source": {"fullName": "child", "type": "domain"},
            },
        ]
        self.domain = {
            "name": "sales",
            "status": "draft",
            "parent": "root",
            "backingTag": {"name": "sales_tag"},
            "relations": self.relations,
            "audit": {"creator": "admin"},
            "properties": {},
            "comment": "Sales",
        }
        self.response = {"code": 0}
        self.status = 200
        self.requests = []
        self.http_clients = []
        factory_patch = patch.object(
            RESTClientFactory, "_rest_client_class", self._create_client
        )
        factory_patch.start()
        self.addCleanup(factory_patch.stop)
        self.mcp = GravitinoMCPServer(
            Setting("test_metalake", token="domain-test-token", tags={"domain"})
        ).mcp

    async def asyncTearDown(self):
        for client in self.http_clients:
            await client.aclose()

    def _respond(self, request):
        self.requests.append(request)
        if isinstance(self.response, Exception):
            raise self.response
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

    async def test_registration_and_readonly_annotations(self):
        async with Client(self.mcp) as client:
            tools = await client.list_tools()
        self.assertEqual(
            {tool.name for tool in tools},
            {
                "list_of_domains",
                "get_domain_by_name",
                "list_subdomains",
                "list_domain_members",
                "list_domains_for_metadata",
            },
        )
        for tool in tools:
            self.assertTrue(tool.annotations.readOnlyHint)
            self.assertFalse(tool.annotations.destructiveHint)
            self.assertTrue(tool.annotations.idempotentHint)

    async def test_snapshots_and_names(self):
        for name, args in (
            ("list_of_domains", {}),
            ("list_subdomains", {"domain_name": "sales"}),
            (
                "list_domains_for_metadata",
                {
                    "metadata_full_name": "cat.schema.table",
                    "metadata_type": "table",
                },
            ),
        ):
            for details in (True, False):
                with self.subTest(name=name, details=details):
                    expected = [self.domain] if details else ["sales"]
                    field = "domains" if details else "names"
                    self.response = {"code": 0, field: expected}
                    self.assertEqual(
                        await self._call(name, {**args, "details": details}),
                        expected,
                    )
                    self.assertEqual(
                        self.requests[-1].url.params["details"],
                        str(details).lower(),
                    )
        self.response = {"code": 0, "domain": self.domain}
        self.assertEqual(
            await self._call("get_domain_by_name", {"domain_name": "sales"}),
            self.domain,
        )

    async def test_members_filters_and_relations(self):
        members = [
            {
                "object": {"fullName": "cat.sch.table", "type": "table"},
                "relations": self.relations,
            }
        ]
        self.response = {"code": 0, "members": members}
        self.assertEqual(
            await self._call(
                "list_domain_members",
                {
                    "domain_name": "sales",
                    "types": ["TABLE", "TOPIC"],
                    "domains": ["child,one", "child & 二"],
                    "name_contains": "a&b + %/?",
                },
            ),
            members,
        )
        query = self.requests[-1].url.params
        self.assertEqual(query["types"], "TABLE,TOPIC")
        self.assertEqual(query.get_list("domains"), ["child,one", "child & 二"])
        self.assertEqual(query["nameContains"], "a&b + %/?")

    async def test_encoded_paths_and_authorization(self):
        value = "a/b ?#%,二"
        for name, args, suffix in (
            ("list_of_domains", {}, "/api/metalakes/test_metalake/domains"),
            ("get_domain_by_name", {"domain_name": value}, "/domains/"),
            ("list_subdomains", {"domain_name": value}, "/domains/"),
            ("list_domain_members", {"domain_name": value}, "/domains/"),
            (
                "list_domains_for_metadata",
                {"metadata_type": "table", "metadata_full_name": value},
                "/objects/table/",
            ),
        ):
            await self._call(name, args)
            request = self.requests[-1]
            expected = (
                suffix
                if name == "list_of_domains"
                else (suffix + "a%2Fb%20%3F%23%25%2C%E4%BA%8C")
            )
            self.assertIn(expected.encode(), request.url.raw_path)
            self.assertEqual(
                request.headers["Authorization"], "Bearer domain-test-token"
            )
            self.assertEqual(request.method, "GET")
            self.assertEqual(request.content, b"")

    async def test_empty_results_and_errors(self):
        calls = [
            ("list_of_domains", {}),
            ("list_subdomains", {"domain_name": "sales"}),
            ("list_domain_members", {"domain_name": "sales"}),
            (
                "list_domains_for_metadata",
                {"metadata_type": "table", "metadata_full_name": "c.s.t"},
            ),
            ("get_domain_by_name", {"domain_name": "sales"}),
        ]
        for name, args in calls:
            self.response = {"code": 0}
            self.assertEqual(
                await self._call(name, args),
                {} if name == "get_domain_by_name" else [],
            )
            for code, error in (
                (401, "UnauthorizedException"),
                (403, "ForbiddenException"),
                (404, "NoSuchDomainException"),
                (400, "IllegalArgumentException"),
            ):
                self.response = {
                    "code": code,
                    "type": error,
                    "message": "denied",
                }
                self.status = code
                with self.assertRaisesRegex(ToolError, error):
                    await self._call(name, args)
            self.status = 200

    async def test_unfiltered_members(self):
        await self._call("list_domain_members", {"domain_name": "sales"})
        self.assertFalse(self.requests[-1].url.query)
        await self._call(
            "list_domain_members",
            {
                "domain_name": "sales",
                "types": [],
                "domains": [],
                "name_contains": "",
            },
        )
        self.assertEqual(self.requests[-1].url.params["nameContains"], "")

    async def test_leaf_object_types(self):
        self.response = {"code": 0, "domains": [self.domain]}
        for metadata_type in (
            "table",
            "topic",
            "fileset",
            "model",
            "view",
            "semantic_model",
        ):
            self.assertEqual(
                await self._call(
                    "list_domains_for_metadata",
                    {
                        "metadata_type": metadata_type,
                        "metadata_full_name": "c.s.leaf",
                    },
                ),
                [self.domain],
            )
            self.assertEqual(
                self.requests[-1].url.path,
                f"/api/metalakes/test_metalake/objects/{metadata_type}/c.s.leaf/domains",
            )

    async def test_invalid_path_segments(self):
        for value in ("", ".", ".."):
            for name, args in (
                ("get_domain_by_name", {"domain_name": value}),
                ("list_subdomains", {"domain_name": value}),
                ("list_domain_members", {"domain_name": value}),
                (
                    "list_domains_for_metadata",
                    {"metadata_type": "table", "metadata_full_name": value},
                ),
                (
                    "list_domains_for_metadata",
                    {"metadata_type": value, "metadata_full_name": "c.s.t"},
                ),
            ):
                with self.assertRaisesRegex(ToolError, "REST path names"):
                    await self._call(name, args)
        self.assertFalse(self.requests)

    async def test_network_failure(self):
        self.response = httpx.ConnectError("server unavailable")
        with self.assertRaisesRegex(ToolError, "server unavailable"):
            await self._call("get_domain_by_name", {"domain_name": "sales"})

    async def test_encoded_metalake(self):
        operation = self._create_client(
            "lake /#二", "http://localhost:8090", authorization="Bearer other"
        )
        try:
            await operation.as_domain_operation().list_of_domains()
            request = self.requests[-1]
            self.assertEqual(
                request.url.raw_path,
                b"/api/metalakes/lake%20%2F%23%E4%BA%8C/domains?details=true",
            )
            self.assertEqual(request.headers["Authorization"], "Bearer other")
        finally:
            await operation.close()

    async def test_http_errors_without_gravitino_envelope(self):
        for status in (401, 403, 500):
            self.status = status
            self.response = {"detail": "Gateway rejected the request"}
            for name, args in (
                ("list_of_domains", {}),
                ("get_domain_by_name", {"domain_name": "sales"}),
                ("list_subdomains", {"domain_name": "sales"}),
                ("list_domain_members", {"domain_name": "sales"}),
                (
                    "list_domains_for_metadata",
                    {
                        "metadata_type": "table",
                        "metadata_full_name": "c.s.t",
                    },
                ),
            ):
                with self.subTest(status=status, tool=name):
                    with self.assertRaisesRegex(ToolError, str(status)):
                        await self._call(name, args)

    async def test_identity_and_metalake_are_resolved_per_call(self):
        self.response = {"code": 0, "domains": [self.domain]}
        async with Client(self.mcp) as client:
            for authorization, metalake in (
                ("Basic YWxpY2U6ZHVtbXk=", "other/lake"),
                ("Bearer bob-token", "second"),
                ("", None),
            ):
                args = {} if metalake is None else {"metalake": metalake}
                with patch(
                    "mcp_server.core.context._get_request_authorization",
                    return_value=authorization,
                ):
                    await client.call_tool("list_of_domains", args)
                request = self.requests[-1]
                self.assertEqual(
                    request.headers["Authorization"],
                    authorization or "Bearer domain-test-token",
                )
                expected_path = {
                    "other/lake": b"/api/metalakes/other%2Flake/domains?details=true",
                    "second": b"/api/metalakes/second/domains?details=true",
                    None: b"/api/metalakes/test_metalake/domains?details=true",
                }[metalake]
                self.assertEqual(request.url.raw_path, expected_path)
                self.assertEqual(request.method, "GET")
                self.assertEqual(request.content, b"")

    async def test_disabled_service_identity_fallback(self):
        self.mcp = GravitinoMCPServer(
            Setting(
                "test_metalake",
                token="service-token",
                tags={"domain"},
                no_service_identity_fallback=True,
            )
        ).mcp
        with (
            patch(
                "mcp_server.core.context._get_request_authorization",
                return_value="",
            ),
            patch(
                "mcp_server.core.context._in_http_request", return_value=True
            ),
        ):
            with self.assertRaisesRegex(
                ToolError, "HTTP request omitted Authorization"
            ):
                await self._call("list_of_domains", {})
        self.assertFalse(self.requests)

    async def test_mock_client_implements_domain_facet(self):
        with patch.object(
            RESTClientFactory, "_rest_client_class", MockOperation
        ):
            self.mcp = GravitinoMCPServer(
                Setting("test_metalake", tags={"domain"})
            ).mcp
            for name, args in (
                ("list_of_domains", {}),
                ("get_domain_by_name", {"domain_name": "sales"}),
                ("list_subdomains", {"domain_name": "sales"}),
                ("list_domain_members", {"domain_name": "sales"}),
                (
                    "list_domains_for_metadata",
                    {
                        "metadata_type": "table",
                        "metadata_full_name": "c.s.t",
                    },
                ),
            ):
                self.assertEqual(
                    await self._call(name, args),
                    {} if name == "get_domain_by_name" else [],
                )
