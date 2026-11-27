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

"""Read published Articles and their visible bindings over MCP and Gravitino HTTP."""

import asyncio
import json
from uuid import uuid4

import httpx
import pytest
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
from fastmcp.exceptions import ToolError

from tests.integration.gravitino_setup import basic_auth_header

_CATALOG = {"type": "CATALOG", "namespace": [], "name": "models"}
_SCHEMA = {"type": "SCHEMA", "namespace": ["models"], "name": "sales"}
_MARKDOWN = {
    "format": "MARKDOWN",
    "content": "# Gross revenue\nShared definition.",
}
_HTML = {"format": "HTML", "content": "<p>Shared definition.</p>"}


def _auth(user):
    return {"Authorization": basic_auth_header(user)}


def _provision(rest, lake):
    rest.post("users", json={"name": "writer"}).raise_for_status()
    rest.post(
        "roles",
        json={
            "name": "authors",
            "properties": {},
            "securableObjects": [
                {
                    "fullName": lake,
                    "type": "METALAKE",
                    "privileges": [
                        {"name": "CREATE_DEFINITION", "condition": "ALLOW"}
                    ],
                }
            ],
        },
    ).raise_for_status()
    rest.put(
        "permissions/users/writer/grant", json={"roleNames": ["authors"]}
    ).raise_for_status()
    rest.post(
        "domains",
        json={
            "name": "Finance",
            "backingTag": {"name": "finance", "create": True},
        },
    ).raise_for_status()
    for name in ("Revenue", "Sales"):
        rest.post("terms", json={"name": name}).raise_for_status()
    rest.post(
        "catalogs",
        json={
            "name": "models",
            "type": "MODEL",
            "provider": "model",
            "properties": {},
        },
    ).raise_for_status()
    rest.post(
        "catalogs/models/schemas", json={"name": "sales", "properties": {}}
    ).raise_for_status()
    for name, status, body in (
        ("alpha", "PUBLISHED", _MARKDOWN),
        ("beta", "PUBLISHED", _HTML),
        ("legacy", "DEPRECATED", _MARKDOWN),
    ):
        rest.post(
            "kb-articles",
            json={
                "name": name,
                "title": name,
                "status": status,
                "body": body,
                "domains": ["Finance"],
                "terms": ["Revenue", "Sales"],
                "objects": [_CATALOG, _SCHEMA],
            },
        ).raise_for_status()
    rest.post(
        "kb-articles",
        headers=_auth("writer"),
        json={
            "name": "candidate",
            "title": "Candidate",
            "status": "DRAFT",
            "body": _MARKDOWN,
        },
    ).raise_for_status()


async def _check_reads(url, lake, user):
    async with Client(
        StreamableHttpTransport(url, headers=_auth(user))
    ) as client:

        async def read(tool, **args):
            result = await client.call_tool(tool, {"metalake": lake, **args})
            return json.loads(result.content[0].text)

        for name in ("candidate", "legacy"):
            with pytest.raises(ToolError, match="not found"):
                await read("get_kb_article", name=name)

        queries = [
            ("search_kb_articles", {}),
            ("list_kb_articles_for_term", {"term": "Revenue"}),
        ]
        target = {
            "object_type": "SCHEMA",
            "namespace": ["models"],
            "name": "sales",
        }
        if user == "admin":
            queries.extend(
                (
                    (
                        "search_kb_articles",
                        {
                            "q": "shared definition",
                            "domain": "Finance",
                            "owner": "user:admin",
                            "term": "Revenue",
                            "asset": _SCHEMA,
                        },
                    ),
                    ("list_kb_articles_for_object", target),
                )
            )
        else:
            with pytest.raises(
                ToolError, match="NoSuchMetadataObjectException"
            ):
                await read("list_kb_articles_for_object", **target)
        for tool, args in queries:
            for offset, name in enumerate(("alpha", "beta")):
                page = await read(tool, limit=1, offset=offset, **args)
                [summary] = page["articles"]
                assert (
                    summary["name"] == name and summary["status"] == "PUBLISHED"
                )
                assert "body" not in summary
                assert page["hasMore"] is (offset == 0)

        await _check_content(read, user)


async def _check_content(read, user):
    objects, terms = [], []
    for offset in (0, 1):
        article = await read(
            "get_kb_article", name="alpha", limit=1, offset=offset
        )
        assert article["body"] == _MARKDOWN
        assert article["domains"] == (["Finance"] if user == "admin" else [])
        assert article["objectsHasMore"] is (user == "admin" and offset == 0)
        assert article["termsHasMore"] is (offset == 0)
        objects.extend(article["objects"])
        terms.extend(term["name"] for term in article["terms"])
    assert set(terms) == {"Sales", "Revenue"}
    if user == "admin":
        assert len(objects) == 2 and _CATALOG in objects and _SCHEMA in objects
    else:
        assert not objects
    assert (await read("get_kb_article", name="beta"))["body"] == _HTML


def test_article_visibility_and_discovery(integration_env):
    lake = "kb_mcp_" + uuid4().hex
    uri = integration_env["gravitino_uri"]
    with httpx.Client(
        base_url=f"{uri}/api/metalakes/{lake}/",
        headers=_auth("admin"),
        timeout=30,
    ) as rest:
        rest.post(
            f"{uri}/api/metalakes", json={"name": lake, "properties": {}}
        ).raise_for_status()
        try:
            _provision(rest, lake)
            for user in ("admin", "writer"):
                response = rest.get(
                    "kb-articles/candidate", headers=_auth(user)
                )
                response.raise_for_status()
                assert response.json()["article"]["status"] == "DRAFT"

            async def check_callers():
                await asyncio.gather(
                    *(
                        _check_reads(integration_env["mcp_url"], lake, user)
                        for user in ("admin", "writer")
                    )
                )

            asyncio.run(check_callers())
        finally:
            rest.delete("", params={"force": "true"}).raise_for_status()
