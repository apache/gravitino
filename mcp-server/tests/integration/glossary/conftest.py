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

"""Create an isolated glossary, its domains, callers, and bound metadata for each test."""

import os

import pytest

from tests.integration.glossary.client import Lake, allow, auth
from tests.integration.glossary.graph import (
    ALIAS,
    LEAF,
    MEMBERS,
    PARENTS,
    PEERS,
    STATUS,
)


def create_assets(lake):
    lake.call(
        "POST",
        "catalogs",
        {
            "name": "dw",
            "type": "RELATIONAL",
            "provider": "lakehouse-generic",
            "properties": {},
        },
    )
    lake.call("POST", "catalogs/dw/schemas", {"name": "s", "properties": {}})
    for table in ("orders", "private_orders"):
        lake.call(
            "POST",
            "catalogs/dw/schemas/s/tables",
            {
                "name": table,
                "columns": [{"name": "amount", "type": "long"}],
                "properties": {
                    "format": "delta",
                    "external": "true",
                    "location": f"file:///tmp/{lake.name}/{table}",
                },
            },
        )
    definition = {
        "datasets": [
            {
                "name": "orders",
                "source": {"namespace": ["dw", "s"], "name": "orders"},
            }
        ],
        "metrics": [
            {
                "name": "volume",
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
    }
    lake.call(
        "POST",
        "catalogs/dw/schemas/s/semantic-models",
        {"name": "metrics", "definition": definition},
    )


@pytest.fixture(name="lake")
def glossary_lake():
    primary = os.environ.get("GRAVITINO_URI")
    peer = os.environ.get("GRAVITINO_PEER_URI")
    if not primary or not peer:
        pytest.skip(
            "Set GRAVITINO_URI and GRAVITINO_PEER_URI to run the glossary contract tests."
        )
    if primary.rstrip("/") == peer.rstrip("/"):
        pytest.fail(
            "GRAVITINO_PEER_URI must point to a second Gravitino server sharing the store."
        )
    api = Lake(primary, peer)
    created = False
    try:
        api.http.post(
            api.urls[0],
            json={"name": api.name, "properties": {}},
            headers=auth("admin"),
        ).raise_for_status()
        created = True
        for n, p, published in [
            ("Sales", None, True),
            ("Retail", "Sales", True),
            ("Wholesale", "Sales", False),
            ("Finance", None, True),
            ("Ops", None, True),
        ]:
            api.domain(n, p, published)
        create_assets(api)
        basic = (
            "METALAKE",
            api.name,
            allow("VIEW_DEFINITION", "USE_CATALOG", "USE_SCHEMA"),
        )
        sales = ("DOMAIN", "Sales", allow("USE_DOMAIN"))
        api.role(
            "sales",
            [
                basic,
                sales,
                ("TABLE", "dw.s.orders", allow("SELECT_TABLE")),
                ("SEMANTIC_MODEL", "dw.s.metrics", allow("USE_SEMANTIC_MODEL")),
            ],
        )
        api.role(
            "no_retail",
            [basic, sales, ("DOMAIN", "Retail", [("USE_DOMAIN", "DENY")])],
        )
        api.role(
            "manager", [("METALAKE", api.name, allow("MANAGE_DEFINITIONS"))]
        )
        for n in PEERS:
            api.create(n, status=STATUS.get(n, "APPROVED"), domains=["Finance"])
        for n, parents in PARENTS.items():
            data = {
                "status": STATUS.get(n, "APPROVED"),
                "broader": list(parents),
                "domains": ["Finance"],
            }
            if n == LEAF:
                data.update(synonyms=[ALIAS], related=PEERS, domains=MEMBERS[n])
            api.create(n, **data)
        for n, domains in MEMBERS.items():
            if n != LEAF:
                api.create(
                    n,
                    status=STATUS.get(n, "APPROVED"),
                    domains=domains,
                    synonyms=[ALIAS],
                )
        for target in (
            "table/dw.s.orders",
            "column/dw.s.orders.amount",
            "semantic_model/dw.s.metrics/metrics/volume",
            "table/dw.s.private_orders",
        ):
            api.call(
                "POST",
                "objects/" + target + "/terms",
                {"termsToAdd": [LEAF, "FinanceAlt", "DraftHit", "OldHit"]},
            )
        yield api
    finally:
        try:
            if created:
                api.call("DELETE", "", params={"force": "true"})
        finally:
            api.http.close()


@pytest.fixture
def mcp_url():
    url = os.environ.get("MCP_URL")
    if not url:
        pytest.skip(
            "Set MCP_URL to run the glossary tools against the same Gravitino servers."
        )
    return url
