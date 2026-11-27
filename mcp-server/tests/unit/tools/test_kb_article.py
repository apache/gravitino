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

"""Exercise the read-only Article tools through FastMCP and the HTTP transport."""

import json
import unittest
from collections import deque
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


class TestKBArticleTools(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.requests = []
        self.responses = deque()

        def respond(request):
            self.requests.append(request)
            return self.responses.popleft()

        def create_client(**kwargs):
            client = httpx.AsyncClient(
                transport=httpx.MockTransport(respond), **kwargs
            )
            self.addAsyncCleanup(client.aclose)
            return client

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
                side_effect=create_client,
            )
        )
        self.server = GravitinoMCPServer(
            Setting("lake", token="service-token", tags={"kb_article"})
        ).mcp

    async def _call(self, tool_name, **args):
        async with Client(self.server) as client:
            result = await client.call_tool(tool_name, args)
        return json.loads(result.content[0].text)

    def _respond(self, **fields):
        self.responses.append(httpx.Response(200, json={"code": 0, **fields}))

    async def test_only_four_read_only_tools_are_registered(self):
        async with Client(self.server) as client:
            tools = await client.list_tools()
        self.assertEqual(
            {tool.name for tool in tools},
            {
                "search_kb_articles",
                "get_kb_article",
                "list_kb_articles_for_term",
                "list_kb_articles_for_object",
            },
        )
        for tool in tools:
            self.assertTrue(tool.annotations.readOnlyHint)
            self.assertFalse(tool.annotations.destructiveHint)
            self.assertTrue(tool.annotations.idempotentHint)

    async def test_combined_search_filters_and_continuation(self):
        term = {"name": "Net revenue", "status": "APPROVED"}
        summary = {
            "name": "guide",
            "title": "Guide",
            "status": "PUBLISHED",
            "domains": ["sales"],
            "terms": [term],
            "termsHasMore": True,
        }
        namespace = ["cat.dot", "tax/+費"]
        self._respond(articles=[summary], hasMore=True)
        page = await self._call(
            "search_kb_articles",
            domain="sales",
            owner="group:operations",
            q="費 + 5%",
            term=term["name"],
            limit=1,
            offset=2,
            asset={
                "type": "TABLE",
                "namespace": namespace,
                "name": "net.amount",
            },
        )
        self.assertEqual(page, {"articles": [summary], "hasMore": True})
        self.assertEqual(len(self.requests), 1)
        request = self.requests[0]
        self.assertEqual(request.method, "GET")
        self.assertEqual(request.url.path, "/api/metalakes/lake/kb-articles")
        expected = {
            "domain": "sales",
            "owner": "group:operations",
            "q": "費 + 5%",
            "term": "Net revenue",
            "name": "net.amount",
            "objectType": "TABLE",
            "status": "PUBLISHED",
            "limit": "1",
            "offset": "2",
        }
        self.assertEqual(
            {key: request.url.params[key] for key in expected}, expected
        )
        self.assertEqual(request.url.params.get_list("namespace"), namespace)

    async def test_browse_and_reverse_lookups(self):
        for name, args, filters in (
            ("search_kb_articles", {"domain": "sales"}, {"domain": "sales"}),
            (
                "list_kb_articles_for_term",
                {"term": "销售+%/a"},
                {"term": "销售+%/a"},
            ),
            (
                "list_kb_articles_for_object",
                {"object_type": "CATALOG", "namespace": [], "name": "my.cat"},
                {"objectType": "CATALOG", "name": "my.cat"},
            ),
        ):
            with self.subTest(tool=name):
                self._respond(articles=[], hasMore=False)
                self.assertEqual(
                    await self._call(name, **args),
                    {"articles": [], "hasMore": False},
                )
                self.assertEqual(
                    dict(self.requests[-1].url.params),
                    {
                        "limit": "20",
                        "offset": "0",
                        "status": "PUBLISHED",
                        **filters,
                    },
                )
        self.assertEqual(len(self.requests), 3)

    async def test_detail_returns_stored_source_and_binding_previews(self):
        meta = {
            "name": "税务",
            "title": "Tax",
            "status": "PUBLISHED",
            "revisionToken": "opaque-a",
            "domains": ["sales"],
            "terms": [{"name": "Net", "status": "APPROVED"}],
            "termsHasMore": False,
            "objects": [
                {
                    "type": "TABLE",
                    "namespace": ["prod", "sales"],
                    "name": "orders",
                }
            ],
            "objectsHasMore": True,
        }
        for fmt, text in (
            ("MARKDOWN", "# Net <b>revenue</b>"),
            ("HTML", "<p>净额</p>"),
        ):
            with self.subTest(format=fmt):
                body = {"format": fmt, "content": text}
                self._respond(article=meta)
                self._respond(body=body, revisionToken=meta["revisionToken"])
                result = await self._call(
                    "get_kb_article", name=meta["name"], limit=1, offset=2
                )
                for field in (
                    "title",
                    "domains",
                    "terms",
                    "termsHasMore",
                    "objects",
                    "objectsHasMore",
                ):
                    self.assertEqual(result[field], meta[field])
                self.assertEqual(result["body"], body)
                metadata_req = self.requests[-2]
                body_req = self.requests[-1]
                self.assertEqual(
                    dict(metadata_req.url.params), {"limit": "1", "offset": "2"}
                )
                self.assertEqual(
                    metadata_req.url.raw_path.split(b"?")[0],
                    b"/api/metalakes/lake/kb-articles/%E7%A8%8E%E5%8A%A1",
                )
                self.assertEqual(
                    body_req.url.path, metadata_req.url.path + "/content"
                )

    async def test_nonpublished_roots_never_fetch_content(self):
        for status in ("DRAFT", "DEPRECATED"):
            with self.subTest(status=status):
                before = len(self.requests)
                self._respond(
                    article={
                        "status": status,
                        "name": "guide",
                        "title": "private title",
                        "revisionToken": "rev",
                    }
                )
                with self.assertRaises(ToolError) as error:
                    await self._call("get_kb_article", name="guide")
                self.assertNotIn("private title", str(error.exception))
                self.assertEqual(len(self.requests), before + 1)

    async def test_mixed_status_list_returns_no_partial_page(self):
        self._respond(
            articles=[
                {"name": "guide", "status": "PUBLISHED"},
                {"title": "private title", "status": "DRAFT"},
            ],
            hasMore=True,
        )
        with self.assertRaises(ToolError) as error:
            await self._call("search_kb_articles")
        self.assertNotIn("private title", str(error.exception))
        self.assertEqual(len(self.requests), 1)

    async def test_metadata_without_revision_never_fetches_content(self):
        self._respond(article={"name": "guide", "status": "PUBLISHED"})
        with self.assertRaises(ToolError):
            await self._call("get_kb_article", name="guide")
        self.assertEqual(len(self.requests), 1)

    async def test_content_from_different_or_missing_revision_is_hidden(self):
        for token in ("revision-b", ""):
            with self.subTest(token=token):
                self._respond(
                    article={
                        "name": "guide",
                        "status": "PUBLISHED",
                        "revisionToken": "revision-a",
                    }
                )
                self._respond(
                    body={"format": "MARKDOWN", "content": "unreleased text"},
                    revisionToken=token,
                )
                before = len(self.requests)
                with self.assertRaises(ToolError) as error:
                    await self._call("get_kb_article", name="guide")
                self.assertNotIn("unreleased text", str(error.exception))
                self.assertEqual(len(self.requests), before + 2)

    async def test_request_identity_and_metalake(self):
        for auth, lake in (
            ("Bearer reader", "other/lake"),
            ("Basic abc", "third"),
            ("", "lake"),
        ):
            with self.subTest(lake=lake):
                self._respond(articles=[], hasMore=False)
                with patch(
                    "mcp_server.core.context._get_request_authorization",
                    return_value=auth,
                ):
                    await self._call("search_kb_articles", metalake=lake)
                request = self.requests[-1]
                self.assertEqual(
                    request.headers["Authorization"],
                    auth or "Bearer service-token",
                )
                encoded_lake = lake.replace("/", "%2F").encode()
                self.assertEqual(
                    request.url.raw_path.split(b"?")[0],
                    b"/api/metalakes/" + encoded_lake + b"/kb-articles",
                )

    async def test_errors_do_not_retry_with_another_identity_or_scope(self):
        for status in (403, 404):
            before = len(self.requests)
            self.responses.append(
                httpx.Response(status, json={"detail": "unavailable"})
            )
            with patch(
                "mcp_server.core.context._get_request_authorization",
                return_value="Bearer caller",
            ):
                with self.assertRaisesRegex(ToolError, str(status)):
                    await self._call(
                        "list_kb_articles_for_term", term="Revenue"
                    )
            self.assertEqual(len(self.requests), before + 1)
            self.assertEqual(
                self.requests[-1].headers["Authorization"], "Bearer caller"
            )
            self.assertEqual(self.requests[-1].url.params["term"], "Revenue")
