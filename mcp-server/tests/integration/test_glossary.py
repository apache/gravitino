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
    for user, privileges in (
        ("writer", ["VIEW_DEFINITION", "CREATE_DEFINITION"]),
        ("manager", ["VIEW_DEFINITION", "MANAGE_DEFINITIONS"]),
        (
            "viewer",
            [
                "VIEW_DEFINITION",
                "USE_CATALOG",
                "USE_SCHEMA",
                "SELECT_TABLE",
                "USE_SEMANTIC_MODEL",
            ],
        ),
    ):
        rest.post("users", json={"name": user}).raise_for_status()
        rest.post(
            "roles",
            json={
                "name": user,
                "properties": {},
                "securableObjects": [
                    {
                        "type": "METALAKE",
                        "fullName": lake,
                        "privileges": [
                            {"name": privilege, "condition": "ALLOW"}
                            for privilege in privileges
                        ],
                    }
                ],
            },
        ).raise_for_status()
        rest.put(
            f"permissions/users/{user}/grant", json={"roleNames": [user]}
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
    rest.post(
        "catalogs/dw/schemas/sales/semantic-models",
        json={
            "name": "sales_model",
            "definition": {
                "datasets": [
                    {
                        "name": "orders",
                        "source": {
                            "namespace": ["dw", "sales"],
                            "name": "orders",
                        },
                    }
                ],
                "metrics": [
                    {
                        "name": "net_revenue",
                        "expression": {
                            "dialects": [
                                {
                                    "dialect": "ANSI_SQL",
                                    "expression": "SUM(orders.amount)",
                                }
                            ]
                        },
                    }
                ],
            },
        },
    ).raise_for_status()
    for target, terms in (
        ("table/dw.sales.orders", ["Revenue", "Legacy"]),
        ("column/dw.sales.orders.amount", ["Net", "Candidate"]),
        ("column/dw.sales.orders.old", ["Legacy"]),
        (
            "semantic_model/dw.sales.sales_model/metrics/net_revenue",
            ["Net", "Legacy"],
        ),
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
    assert len(bindings) == len(preview)
    assert all(binding in preview for binding in bindings)
    table = {"metadata_type": "table", "metadata_full_name": "dw.sales.orders"}
    model = {
        "metadata_type": "semantic_model",
        "metadata_full_name": "dw.sales.sales_model",
    }
    if user in ("manager", "writer"):
        assert bindings == []
        for target in (table, model):
            with pytest.raises(ToolError, match="ForbiddenException"):
                await read("list_terms_for_metadata", **target)
        return

    by_type = {binding["type"]: binding for binding in bindings}
    assert len(bindings) == 2 and set(by_type) == {"COLUMN", "SEMANTIC_MODEL"}
    column = by_type["COLUMN"]
    assert column["kind"] == "IMPLEMENTED_BY" and column["column"] == "amount"
    table_details = await read(
        "get_table_metadata_details",
        catalog_name=column["catalog"],
        schema_name=column["schema"],
        table_name=column["name"],
    )
    assert column["column"] in {
        field["name"] for field in table_details["columns"]
    }

    metric = by_type["SEMANTIC_MODEL"]
    assert metric["kind"] == "USED_BY" and metric["metric"] == "net_revenue"
    model_details = await read(
        "load_semantic_model",
        catalog_name=metric["catalog"],
        schema_name=metric["schema"],
        semantic_model_name=metric["name"],
    )
    assert metric["metric"] in {
        m["name"] for m in model_details["definition"]["metrics"]
    }

    for target, direct, child in (
        (table, ["Revenue"], "amount"),
        (model, [], "net_revenue"),
    ):
        reverse = await read("list_terms_for_metadata", **target)
        assert [term["name"] for term in reverse["terms"]] == direct
        assert [
            (item["name"], [term["name"] for term in item["terms"]])
            for item in reverse["children"]
        ] == [(child, ["Net"])]


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
            for user in ("admin", "manager", "writer"):
                assert (
                    rest.get("terms/Candidate", headers=_auth(user)).json()[
                        "term"
                    ]["status"]
                    == "DRAFT"
                )

            assert (
                rest.get("terms/Candidate", headers=_auth("viewer")).status_code
                == 404
            )

            async def check_callers():
                await asyncio.gather(
                    *(
                        _check_reads(integration_env["mcp_url"], lake, user)
                        for user in ("admin", "manager", "writer", "viewer")
                    )
                )

            asyncio.run(check_callers())
        finally:
            rest.delete("", params={"force": "true"}).raise_for_status()
