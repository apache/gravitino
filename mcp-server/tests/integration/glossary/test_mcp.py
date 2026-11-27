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

"""Resolve a business term and follow its visible bindings via MCP."""

import asyncio
import json

import pytest
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
from fastmcp.exceptions import ToolError

from tests.integration.glossary.client import auth, names
from tests.integration.glossary.graph import ALIAS, LEAF, projected_domains


def test_mcp_multi_batch_domain_followup_and_reverse(mcp_url, lake):
    async def run():
        async with Client(
            StreamableHttpTransport(mcp_url, headers=auth("sales"))
        ) as client:

            async def tool(name, **kw):
                result = await client.call_tool(
                    name, {"metalake": lake.name, **kw}
                )
                return json.loads(result.content[0].text)

            blanks = [f"absent {i}" for i in range(19)]
            for domain in ("Retail", "Sales"):
                result = await tool(
                    "resolve_business_term",
                    words=[ALIAS, *blanks, LEAF, "missing"],
                    domain=domain,
                )
                assert result["unmatched"] == [*blanks, "missing"]
                assert [x["word"] for x in result["results"]] == [ALIAS, LEAF]
                assert sorted(
                    m["term"] for m in result["results"][0]["matches"]
                ) == sorted((LEAF, "RetailAlt"))
                (match,) = result["results"][1]["matches"]
                assert (match["term"], match["matched_by"]) == (LEAF, "NAME")
                assert sorted(match["domains"]) == ["Retail", "Sales"]
                assert sorted(match["broader"]) == ["Left", "Right"] and match[
                    "related"
                ] == ["LivePeer"]
                assert sorted(b["type"] for b in match["bindings"]) == [
                    "COLUMN",
                    "SEMANTIC_MODEL",
                    "TABLE",
                ]
                full = await tool("list_metadata_by_term", term=LEAF)
                assert sorted(full, key=lambda x: x["type"]) == sorted(
                    match["bindings"], key=lambda x: x["type"]
                )
                assert (await tool("get_term", term=LEAF))["name"] == LEAF
                assert names(
                    await tool(
                        "get_term_hierarchy", term=LEAF, direction="BROADER"
                    )
                ) == ["Concept", "Left", "Right"]
                for binding in full:
                    args = {
                        "catalog_name": binding["catalog"],
                        "schema_name": binding["schema"],
                    }
                    if binding["type"] == "COLUMN":
                        table = await tool(
                            "get_table_metadata_details",
                            **args,
                            table_name=binding["name"],
                        )
                        assert binding["column"] in [
                            c["name"] for c in table["columns"]
                        ]
                    if binding["type"] == "SEMANTIC_MODEL":
                        model = await tool(
                            "load_semantic_model",
                            **args,
                            semantic_model_name=binding["name"],
                        )
                        assert binding["metric"] in [
                            m["name"] for m in model["definition"]["metrics"]
                        ]
            assert names(
                await tool(
                    "list_terms", domain="Retail", name_contains="Active"
                )
            ) == [LEAF]
            reverse = await tool(
                "list_terms_for_metadata",
                metadata_type="table",
                metadata_full_name="dw.s.orders",
            )
            assert names(reverse["children"][0]["terms"]) == sorted(
                (LEAF, "FinanceAlt")
            )
            for t in reverse["children"][0]["terms"]:
                assert sorted(t["domains"]) == projected_domains(
                    t["name"], "sales"
                )
            with pytest.raises(ToolError, match="NoSuchDomainException"):
                await tool(
                    "resolve_business_term", words=[ALIAS], domain="Wholesale"
                )

    asyncio.run(run())
