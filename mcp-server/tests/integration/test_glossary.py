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

"""Check approved-only glossary reads against the live authorization-enabled server."""

import asyncio
import json
from uuid import uuid4

import httpx
import pytest
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
from fastmcp.exceptions import ToolError

from tests.integration.gravitino_setup import basic_auth_header


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
                        {"name": name, "condition": "ALLOW"}
                        for name in ("CREATE_DEFINITION", "VIEW_DEFINITION")
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
    rest.post(
        "terms", json={"name": "Revenue", "domains": ["Finance"]}
    ).raise_for_status()
    rest.post(
        "terms",
        headers=_auth("writer"),
        json={"name": "Candidate", "status": "DRAFT", "broader": ["Revenue"]},
    ).raise_for_status()
    rest.post(
        "terms",
        json={
            "name": "Net",
            "broader": ["Candidate"],
            "synonyms": ["net revenue"],
        },
    ).raise_for_status()
    rest.post(
        "terms",
        json={"name": "Legacy", "status": "DEPRECATED", "related": ["Revenue"]},
    ).raise_for_status()
    rest.post(
        "catalogs",
        json={
            "name": "dw",
            "type": "RELATIONAL",
            "provider": "lakehouse-generic",
            "properties": {},
        },
    ).raise_for_status()
    rest.post(
        "catalogs/dw/schemas", json={"name": "sales", "properties": {}}
    ).raise_for_status()
    rest.post(
        "catalogs/dw/schemas/sales/tables",
        json={
            "name": "orders",
            "columns": [
                {"name": "amount", "type": "long"},
                {"name": "old", "type": "long"},
            ],
            "properties": {
                "format": "delta",
                "external": "true",
                "location": f"file:///tmp/{lake}/orders",
            },
        },
    ).raise_for_status()
    for target, terms in (
        ("table/dw.sales.orders", ["Revenue", "Legacy"]),
        ("column/dw.sales.orders.amount", ["Net", "Candidate"]),
        ("column/dw.sales.orders.old", ["Legacy"]),
    ):
        rest.post(
            f"objects/{target}/terms", json={"termsToAdd": terms}
        ).raise_for_status()


async def _check_reads(url, lake, user):
    async with Client(
        StreamableHttpTransport(url, headers=_auth(user))
    ) as client:

        async def read(tool, **args):
            result = await client.call_tool(tool, {"metalake": lake, **args})
            return json.loads(result.content[0].text)

        for status_filtered in ("Candidate", "Legacy"):
            for tool in (
                "get_term",
                "get_term_hierarchy",
                "list_metadata_by_term",
            ):
                with pytest.raises(ToolError, match="NoSuchTermException"):
                    await read(tool, term=status_filtered)

        listed = await read("list_terms")
        assert {term["name"] for term in listed} == {"Revenue", "Net"}
        term = await read("get_term", term="Revenue")
        assert term["related"] == [] and term["narrower"] == []
        assert term["domains"] == (["Finance"] if user == "admin" else [])

        for root, direction in (("Revenue", "NARROWER"), ("Net", "BROADER")):
            assert (
                await read("get_term_hierarchy", term=root, direction=direction)
                == []
            )

        resolved = await read(
            "resolve_business_term",
            words=["net revenue", "Candidate", "Legacy"],
        )
        assert resolved["unmatched"] == ["Candidate", "Legacy"]
        match = resolved["results"][0]["matches"][0]
        assert match["term"] == "Net" and match["matched_by"] == "SYNONYM"
        assert match["broader"] == []
        await _check_bindings(read, user, match["bindings"])


async def _check_bindings(read, user, preview):
    bindings = await read("list_metadata_by_term", term="Net")
    assert bindings == preview
    args = {
        "metadata_type": "table",
        "metadata_full_name": "dw.sales.orders",
    }
    if user == "admin":
        assert bindings == [
            {
                "kind": "IMPLEMENTED_BY",
                "type": "COLUMN",
                "catalog": "dw",
                "schema": "sales",
                "name": "orders",
                "column": "amount",
            }
        ]
        reverse = await read("list_terms_for_metadata", **args)
        assert [term["name"] for term in reverse["terms"]] == ["Revenue"]
        assert [
            (child["name"], [t["name"] for t in child["terms"]])
            for child in reverse["children"]
        ] == [("amount", ["Net"])]
    else:
        assert bindings == []
        with pytest.raises(ToolError, match="ForbiddenException"):
            await read("list_terms_for_metadata", **args)


def test_manager_and_draft_owner_only_read_approved_terms(integration_env):
    lake = "glossary_mcp_" + uuid4().hex
    base = f"{integration_env['gravitino_uri']}/api/metalakes/{lake}/"
    with httpx.Client(
        base_url=base, headers=_auth("admin"), timeout=30
    ) as rest:
        rest.post(
            f"{integration_env['gravitino_uri']}/api/metalakes",
            json={"name": lake, "properties": {}},
        ).raise_for_status()
        try:
            _provision(rest, lake)
            for user in ("admin", "writer"):
                assert (
                    rest.get("terms/Candidate", headers=_auth(user)).json()[
                        "term"
                    ]["status"]
                    == "DRAFT"
                )

            async def check_both_callers():
                await asyncio.gather(
                    *(
                        _check_reads(integration_env["mcp_url"], lake, user)
                        for user in ("admin", "writer")
                    )
                )

            asyncio.run(check_both_callers())
        finally:
            rest.delete("", params={"force": "true"}).raise_for_status()
