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

"""End-to-end authorization integration test through the live MCP HTTP server.

Validates the three acceptance scenarios against a real Gravitino with
authorization enabled:

  1. Two principals run the same discovery call and get correctly different,
     authorization-scoped results.
  2. A read-only principal attempts a write through MCP and is denied by
     Gravitino authorization.
  3. Both the reads and the denied write appear as audit records attributed to
     the correct principal.

These tests only run when GRAVITINO_URI / MCP_URL / MCP_METALAKE are set (see
conftest.py); otherwise the suite is skipped.
"""

import asyncio
import json
import os
import time

import pytest
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport

from tests.integration.gravitino_setup import basic_auth_header

ADMIN = "admin"
BOB = "bob"
CATALOG_ALLOWED = "cat_allowed"
CATALOG_DENIED = "cat_denied"


def _client_for(principal: str, mcp_url: str) -> Client:
    """Build an MCP client that authenticates as ``principal`` (simple auth)."""
    transport = StreamableHttpTransport(
        url=mcp_url,
        headers={"Authorization": basic_auth_header(principal)},
    )
    return Client(transport)


async def _list_catalog_names(principal: str, mcp_url: str) -> set:
    async with _client_for(principal, mcp_url) as client:
        result = await client.call_tool("get_list_of_catalogs")
    payload = json.loads(result.content[0].text)
    return {entry["name"] for entry in payload}


def test_authorization_scoped_discovery(gravitino_fixture, integration_env):
    """Admin and bob get different, correctly scoped catalog lists."""
    mcp_url = integration_env["mcp_url"]

    admin_catalogs = asyncio.run(_list_catalog_names(ADMIN, mcp_url))
    bob_catalogs = asyncio.run(_list_catalog_names(BOB, mcp_url))

    # Admin owns the metalake and sees both catalogs.
    assert CATALOG_ALLOWED in admin_catalogs
    assert CATALOG_DENIED in admin_catalogs

    # Bob was granted USE_CATALOG on the allowed catalog only.
    assert CATALOG_ALLOWED in bob_catalogs
    assert CATALOG_DENIED not in bob_catalogs

    # The two principals must receive different results.
    assert admin_catalogs != bob_catalogs


def test_write_denied_for_readonly_principal(
    gravitino_fixture, integration_env
):
    """Bob (no write grant) is denied when creating a tag through MCP."""
    mcp_url = integration_env["mcp_url"]

    async def _attempt_write():
        async with _client_for(BOB, mcp_url) as client:
            await client.call_tool(
                "create_tag",
                {
                    "name": "denied_tag",
                    "comment": "should be denied",
                    "properties": {},
                },
            )

    with pytest.raises(Exception) as exc_info:  # noqa: B017
        asyncio.run(_attempt_write())

    # The failure must be an authorization denial, not an unrelated error
    # (transport failure, missing tool, server crash, ...).
    message = str(exc_info.value).lower()
    assert any(
        token in message
        for token in (
            "forbidden",
            "unauthorized",
            "not authorized",
            "permission",
            "denied",
            "access",
            "403",
        )
    ), f"expected an authorization denial, got: {exc_info.value!r}"


def test_audit_trail_attribution(gravitino_fixture, integration_env):
    """Audit log records reads/writes attributed to the right principal."""
    audit_log = os.environ.get("MCP_AUDIT_LOG")
    if not audit_log or not os.path.exists(audit_log):
        pytest.skip("MCP_AUDIT_LOG not set or file missing")

    mcp_url = integration_env["mcp_url"]

    async def _attempt_write():
        async with _client_for(BOB, mcp_url) as client:
            await client.call_tool(
                "create_tag",
                {"name": "audit_tag", "comment": "", "properties": {}},
            )

    # Generate one allowed read (admin) and one denied write (bob).
    asyncio.run(_list_catalog_names(ADMIN, mcp_url))
    try:
        asyncio.run(_attempt_write())
    except Exception:  # pylint: disable=broad-exception-caught
        # Expected denial for an unauthorized principal.
        pass

    # Give the server a moment to flush the audit handler.
    time.sleep(1.0)

    records = []
    with open(audit_log, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    admin_allows = [
        r
        for r in records
        if r.get("principal") == ADMIN
        and r.get("tool") == "get_list_of_catalogs"
        and r.get("outcome") == "allow"
    ]
    bob_denies = [
        r
        for r in records
        if r.get("principal") == BOB
        and r.get("tool") == "create_tag"
        and r.get("outcome") == "deny"
    ]

    assert admin_allows, "expected an allow record attributed to admin"
    assert bob_denies, "expected a deny record attributed to bob"
