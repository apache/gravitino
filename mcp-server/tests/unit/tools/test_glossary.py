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

"""Exercise glossary tools through FastMCP, capturing their outgoing HTTP requests."""

import json
import unittest
from functools import partial
from pathlib import Path
from unittest.mock import Mock, patch

import httpx
from fastmcp import Client
from fastmcp.exceptions import ToolError

from mcp_server.client.factory import RESTClientFactory
from mcp_server.client.plain.plain_rest_client_operation import (
    PlainRESTClientOperation,
)
from mcp_server.core import Setting
from mcp_server.server import GravitinoMCPServer


def _term(name, *matched_by):
    return {
        "name": name,
        "status": "APPROVED",
        "domains": ["Sales"],
        "broader": [],
        "related": [],
        "matchedBy": [
            {"word": word, "field": field} for word, field in matched_by
        ],
        "bindings": [],
        "bindingsTruncated": False,
    }


def _binding(kind, object_type, full_name, **fields):
    return {
        "kind": kind,
        "object": {"type": object_type, "fullName": full_name},
        **fields,
    }


async def _call(client, name, **arguments):
    result = await client.call_tool(name, arguments)
    return json.loads(result.content[0].text)


class TestGlossary(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.requests = []
        self.responses = []
        self.enterContext(
            patch.object(
                RESTClientFactory,
                "_rest_client_class",
                PlainRESTClientOperation,
            )
        )
        self.enterContext(
            patch(
                "mcp_server.client.plain.plain_rest_client_operation._GravitinoAsyncClient",
                partial(
                    httpx.AsyncClient,
                    transport=httpx.MockTransport(self._respond),
                ),
            )
        )
        self.mcp = GravitinoMCPServer(
            Setting("lake", token="service", tags={"glossary"})
        ).mcp

    def _respond(self, request):
        self.requests.append(request)
        self.assertEqual(request.method, "GET")
        self.assertEqual(request.url.params.get_list("status"), ["APPROVED"])
        return self.responses.pop(0)

    def _ok(self, **fields):
        self.responses.append(httpx.Response(200, json={"code": 0, **fields}))

    async def test_registration(self):
        async with Client(self.mcp) as client:
            tools = {tool.name: tool for tool in await client.list_tools()}
        self.assertEqual(
            set(tools),
            {
                "resolve_business_term",
                "get_term",
                "list_terms",
                "get_term_hierarchy",
                "list_metadata_by_term",
                "list_terms_for_metadata",
            },
        )
        for tool in tools.values():
            self.assertTrue(tool.annotations.readOnlyHint)
            self.assertTrue(tool.annotations.idempotentHint)
            self.assertFalse(tool.annotations.destructiveHint)
        description = tools["resolve_business_term"].description
        for rule in (
            "singular",
            "unmatched",
            "domains",
            "privileges",
            "include_bindings=false",
        ):
            self.assertIn(rule, description)

    async def test_resolve_design_example(self):
        net = _term("Net revenue", ("net revenue", "NAME"))
        net.update(
            definition="Revenue after refunds and discounts.",
            broader=["Revenue"],
            related=["Order"],
        )
        customer = _term("Active Customer", ("active user", "SYNONYM"))
        customer.update(
            definition="A customer with at least one order in the last 90 days.",
            broader=["Customer"],
        )
        net["bindings"] = [
            _binding(
                "used_by",
                "semantic_model",
                "dw.sales.sales_model",
                metric="net_revenue",
            )
        ]
        customer["bindings"] = [
            _binding(
                "implemented_by", "column", "dw.sales.customers.is_active"
            ),
            _binding(
                "used_by",
                "semantic_model",
                "dw.sales.sales_model",
                metric="active_customers",
            ),
        ]
        self._ok(terms=[customer, net])
        words = ["net revenue", "VIP", "active user"]
        async with Client(self.mcp) as client:
            result = await _call(
                client, "resolve_business_term", words=words, domain="Sales"
            )
        golden = Path(__file__).with_name("resolve_business_term.json")
        self.assertEqual(result, json.loads(golden.read_text(encoding="utf-8")))
        query = self.requests[0].url.params
        self.assertEqual(query.get_list("match"), words)
        self.assertEqual(query["details"], "true")
        self.assertEqual(query["bindings"], "true")
        self.assertEqual(query["domain"], "Sales")
        self.assertEqual(len(self.requests), 1)

    async def test_multiple_words_and_ambiguous_terms(self):
        first = _term(
            "Customer", ("Customer", "NAME"), (" Active  user ", "SYNONYM")
        )
        first["bindingsTruncated"] = True
        second = _term("Subscriber", (" Active  user ", "SYNONYM"))
        self._ok(terms=[first, second])
        words = [" Active  user ", "Customer", "miss", "Customer"]
        async with Client(self.mcp) as client:
            result = await _call(client, "resolve_business_term", words=words)
        self.assertEqual(
            [entry["word"] for entry in result["results"]],
            [words[0], "Customer", "Customer"],
        )
        ambiguity = result["results"][0]["matches"]
        self.assertEqual(
            [entry["term"] for entry in ambiguity], ["Customer", "Subscriber"]
        )
        self.assertTrue(ambiguity[0]["bindings_truncated"])
        self.assertEqual(
            result["results"][1]["matches"][0]["matched_by"], "NAME"
        )
        self.assertEqual(result["unmatched"], ["miss"])

    async def test_batches_preserve_order_and_fail_as_one_lookup(self):
        words = [f"phrase {i}" for i in range(25)]
        async with Client(self.mcp) as client:
            self._ok(terms=[_term("First", (words[0], "NAME"))])
            self.responses.append(
                httpx.Response(
                    400, json={"code": 1001, "type": "LookupTooBroadException"}
                )
            )
            with self.assertRaisesRegex(ToolError, "LookupTooBroadException"):
                await _call(
                    client, "resolve_business_term", words=words, domain="D"
                )

            self.assertEqual(len(self.requests), 2)
            for batch in [words[:20], words[20:]]:
                term = _term(batch[-1], (batch[-1], "NAME"))
                del term["bindings"], term["bindingsTruncated"]
                self._ok(terms=[term])
            result = await _call(
                client,
                "resolve_business_term",
                words=words,
                domain="D",
                include_bindings=False,
            )

        self.assertEqual(
            [r["word"] for r in result["results"]], [words[19], words[24]]
        )
        self.assertEqual(result["unmatched"], words[:19] + words[20:24])
        for row in result["results"]:
            self.assertNotIn("bindings", row["matches"][0])
            self.assertNotIn("bindings_truncated", row["matches"][0])
        for i, request in enumerate(self.requests):
            self.assertEqual(
                request.url.params.get_list("match"),
                words[:20] if i % 2 == 0 else words[20:],
            )
            self.assertEqual(
                request.url.params["bindings"], "true" if i < 2 else "false"
            )
            self.assertEqual(request.url.params["domain"], "D")
            self.assertEqual(request.headers["Authorization"], "Bearer service")

    async def test_filtered_reads(self):
        term = {
            "name": "Net/revenue",
            "status": "APPROVED",
            "broader": ["Revenue"],
        }
        async with Client(self.mcp) as client:
            self._ok(term=term)
            self.assertEqual(
                await _call(
                    client, "get_term", term=term["name"], metalake="sales/eu"
                ),
                term,
            )
            self.assertIn(
                b"/sales%2Feu/terms/Net%2Frevenue?",
                self.requests[-1].url.raw_path,
            )
            self._ok(terms=[term])
            self.assertEqual(
                await _call(
                    client, "list_terms", domain="A&B", name_contains="Net/"
                ),
                [term],
            )
            params = self.requests[-1].url.params
            self.assertEqual(
                (params["domain"], params["nameContains"], params["details"]),
                ("A&B", "Net/", "true"),
            )
            for direction in ("NARROWER", "BROADER"):
                self._ok(terms=[term])
                self.assertEqual(
                    await _call(
                        client,
                        "get_term_hierarchy",
                        term="Root",
                        direction=direction,
                    ),
                    [term],
                )
                self.assertEqual(
                    self.requests[-1].url.path,
                    "/api/metalakes/lake/terms/Root/hierarchy",
                )
                self.assertEqual(
                    self.requests[-1].url.params["direction"], direction
                )
            self._ok(
                bindings=[
                    _binding("implemented_by", "catalog", "my.catalog"),
                    _binding("implemented_by", "schema", "dw.sales"),
                    _binding("implemented_by", "table", "dw.sales.customers"),
                ]
            )
            bindings = await _call(
                client,
                "list_metadata_by_term",
                term="Customer",
                kind="IMPLEMENTED_BY",
                types=["CATALOG", "SCHEMA", "TABLE"],
            )
            self.assertEqual(
                self.requests[-1].url.path,
                "/api/metalakes/lake/terms/Customer/objects",
            )
            self.assertEqual(
                self.requests[-1].url.params["kind"], "IMPLEMENTED_BY"
            )
            self.assertEqual(
                self.requests[-1].url.params.get_list("types"),
                ["CATALOG", "SCHEMA", "TABLE"],
            )
            self.assertEqual(
                bindings,
                [
                    {
                        "type": "CATALOG",
                        "kind": "IMPLEMENTED_BY",
                        "catalog": "my.catalog",
                    },
                    {
                        "type": "SCHEMA",
                        "kind": "IMPLEMENTED_BY",
                        "catalog": "dw",
                        "schema": "sales",
                    },
                    {
                        "type": "TABLE",
                        "kind": "IMPLEMENTED_BY",
                        "catalog": "dw",
                        "schema": "sales",
                        "name": "customers",
                    },
                ],
            )

    async def test_reverse_lookup(self):
        async with Client(self.mcp) as client:
            for object_type in (
                "table",
                "SEMANTIC_MODEL",
                "view",
                "FILESET",
                "COLUMN",
            ):
                children = object_type in ("table", "SEMANTIC_MODEL")
                payload = {
                    "terms": [],
                    "children": (
                        [{"name": "a", "terms": [_term("A")]}]
                        if children
                        else []
                    ),
                }
                self._ok(**payload)
                result = await _call(
                    client,
                    "list_terms_for_metadata",
                    metadata_type=object_type,
                    metadata_full_name="cat.s.A/B",
                )
                self.assertEqual(result, payload)
                request = self.requests[-1]
                self.assertIn(
                    f"/objects/{object_type}/cat.s.A%2FB/terms?".encode(),
                    request.url.raw_path,
                )
                self.assertEqual(
                    request.url.params.get("children"),
                    "true" if children else None,
                )
                self.assertEqual(request.url.params["details"], "true")

    async def test_invalid_inputs_and_failed_responses(self):
        async with Client(self.mcp) as client:
            for words in ([], [" ", "ok"]):
                with self.assertRaisesRegex(ToolError, "phrase"):
                    await _call(client, "resolve_business_term", words=words)
            with self.assertRaisesRegex(ToolError, "REST path names"):
                await _call(client, "get_term", term="..")
            self.assertFalse(self.requests)
            for name in (
                "get_term",
                "list_metadata_by_term",
                "get_term_hierarchy",
            ):
                self.responses.append(
                    httpx.Response(
                        404, json={"code": 1003, "type": "NoSuchTermException"}
                    )
                )
                with self.assertRaisesRegex(ToolError, "NoSuchTermException"):
                    await _call(client, name, term="Draft")
            self.responses.append(httpx.Response(200, json={"terms": []}))
            with self.assertRaisesRegex(ToolError, "missing code"):
                await _call(client, "resolve_business_term", words=["Revenue"])
            self.assertEqual(len(self.requests), 4)

    async def test_identity_selection(self):
        for token, http, fallback_disabled, header, expected in (
            ("service", True, True, "Basic YWxpY2U6eA==", "Basic YWxpY2U6eA=="),
            ("service", True, True, "Bearer caller", "Bearer caller"),
            ("service", True, True, "Negotiate abc", "Negotiate abc"),
            ("service", True, False, "", "Bearer service"),
            ("", True, True, "", None),
            ("service", False, True, "", "Bearer service"),
            ("", False, True, "", None),
        ):
            with self.subTest(http=http, header=header, token=token):
                setting = Setting(
                    "lake",
                    token=token,
                    tags={"glossary"},
                    no_service_identity_fallback=fallback_disabled,
                )
                if http:
                    request = Mock(headers={"authorization": header})
                    transport = patch(
                        "fastmcp.server.dependencies.get_http_request",
                        return_value=request,
                    )
                else:
                    transport = patch(
                        "fastmcp.server.dependencies.get_http_request",
                        side_effect=RuntimeError,
                    )
                with transport, patch("mcp_server.core.audit.emit") as audit:
                    async with Client(
                        GravitinoMCPServer(setting).mcp
                    ) as client:
                        self._ok(terms=[])
                        await _call(client, "list_terms")
                    self.assertEqual(
                        self.requests[-1].headers.get("Authorization"), expected
                    )
                    record = audit.call_args.kwargs
                    self.assertEqual(
                        (record["tool"], record["metalake"], record["outcome"]),
                        ("list_terms", "lake", "allow"),
                    )
                    if header.startswith("Basic"):
                        self.assertEqual(record["principal"], "alice")
                    elif expected:
                        self.assertTrue(
                            record["principal"].startswith(
                                expected.split()[0].lower() + ":"
                            )
                        )
                    else:
                        self.assertEqual(record["principal"], "anonymous")

        setting = Setting(
            "lake",
            token="service",
            tags={"glossary"},
            no_service_identity_fallback=True,
        )
        with (
            patch(
                "mcp_server.core.context._in_http_request", return_value=True
            ),
            patch(
                "mcp_server.core.context._get_request_authorization",
                return_value="",
            ),
        ):
            async with Client(GravitinoMCPServer(setting).mcp) as client:
                with self.assertRaisesRegex(
                    ToolError, "HTTP request omitted Authorization"
                ):
                    await _call(client, "list_terms")
        self.assertEqual(len(self.requests), 7)
        with patch(
            "mcp_server.core.context._get_request_authorization",
            return_value="Bearer invalid",
        ):
            async with Client(self.mcp) as client:
                self.responses.append(httpx.Response(401))
                with self.assertRaisesRegex(ToolError, "401"):
                    await _call(client, "list_terms")
        self.assertEqual(len(self.requests), 8)
        self.assertEqual(
            self.requests[-1].headers["Authorization"], "Bearer invalid"
        )
