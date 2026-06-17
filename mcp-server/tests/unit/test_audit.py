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

import asyncio
import json
import logging
import unittest

from fastmcp import Client

from mcp_server.client.factory import RESTClientFactory
from mcp_server.client.plain.exception import GravitinoException
from mcp_server.client.plain.plain_rest_client_operation import (
    PlainRESTClientOperation,
)
from mcp_server.core import audit
from mcp_server.core.setting import Setting
from mcp_server.server import GravitinoMCPServer
from tests.unit.tools import MockOperation

# Tests intentionally exercise module-private helpers (e.g. audit._extract_principal)
# and client internals; protected access is expected here.
# pylint: disable=protected-access


class TestAuditEmit(unittest.TestCase):
    """Unit tests for the audit.emit() function."""

    def setUp(self):
        # tests/unit/tools/__init__.py calls logging.disable(logging.INFO).
        # Re-enable here so audit records flow through; restore on teardown.
        logging.disable(logging.NOTSET)
        self.log_records = []
        self.handler = _CapturingHandler(self.log_records)
        audit_logger = logging.getLogger("gravitino.mcp.audit")
        audit_logger.addHandler(self.handler)
        audit_logger.setLevel(logging.INFO)
        audit_logger.propagate = False

    def tearDown(self):
        logging.disable(logging.INFO)
        audit_logger = logging.getLogger("gravitino.mcp.audit")
        audit_logger.removeHandler(self.handler)
        audit_logger.propagate = True

    def test_allow_record_structure(self):
        """emit() writes a JSON record with all required fields on allow."""
        audit.emit(
            principal="bearer:abc12345", tool="list_catalogs", outcome="allow"
        )

        self.assertEqual(len(self.log_records), 1)
        record = json.loads(self.log_records[0])
        self.assertEqual(record["principal"], "bearer:abc12345")
        self.assertEqual(record["tool"], "list_catalogs")
        self.assertEqual(record["outcome"], "allow")
        self.assertIn("timestamp", record)
        self.assertNotIn("error_type", record)

    def test_deny_record_includes_error_type(self):
        """emit() includes error_type in the record when outcome is deny."""
        audit.emit(
            principal="bearer:xyz99999",
            tool="create_tag",
            outcome="deny",
            error_type="GravitinoException",
        )

        record = json.loads(self.log_records[0])
        self.assertEqual(record["outcome"], "deny")
        self.assertEqual(record["error_type"], "GravitinoException")

    def test_anonymous_principal(self):
        """emit() works with anonymous principal."""
        audit.emit(
            principal="anonymous", tool="get_list_of_catalogs", outcome="allow"
        )
        record = json.loads(self.log_records[0])
        self.assertEqual(record["principal"], "anonymous")


class TestExtractPrincipal(unittest.TestCase):
    """Unit tests for audit._extract_principal()."""

    def test_bearer_token_truncated_to_8_chars(self):
        self.assertEqual(
            audit._extract_principal("Bearer abcdefghijklmnop"),
            "bearer:abcdefgh",
        )

    def test_empty_header_returns_anonymous(self):
        self.assertEqual(audit._extract_principal(""), "anonymous")

    def test_none_like_empty_returns_anonymous(self):
        self.assertEqual(audit._extract_principal(None), "anonymous")

    def test_short_token_uses_full_token(self):
        self.assertEqual(audit._extract_principal("Bearer abc"), "bearer:abc")

    def test_basic_auth_decodes_user(self):
        """Simple auth header 'Basic base64(alice:dummy)' -> principal 'alice'."""
        # base64("alice:dummy") == "YWxpY2U6ZHVtbXk="
        self.assertEqual(
            audit._extract_principal("Basic YWxpY2U6ZHVtbXk="), "alice"
        )

    def test_basic_auth_invalid_base64_returns_anonymous(self):
        self.assertEqual(
            audit._extract_principal("Basic not-valid-base64!!"), "anonymous"
        )

    def test_unknown_scheme_returns_anonymous(self):
        self.assertEqual(
            audit._extract_principal("Negotiate abc123"), "anonymous"
        )


class TestAuditMiddlewareIntegration(unittest.TestCase):
    """Integration tests: AuditMiddleware emits records via the full MCP tool path."""

    def setUp(self):
        logging.disable(logging.NOTSET)
        self.log_records = []
        self.handler = _CapturingHandler(self.log_records)
        audit_logger = logging.getLogger("gravitino.mcp.audit")
        audit_logger.addHandler(self.handler)
        audit_logger.setLevel(logging.INFO)
        audit_logger.propagate = False

        RESTClientFactory.set_rest_client(MockOperation)
        server = GravitinoMCPServer(Setting("mock_metalake"))
        self.mcp = server.mcp

    def tearDown(self):
        logging.disable(logging.INFO)
        audit_logger = logging.getLogger("gravitino.mcp.audit")
        audit_logger.removeHandler(self.handler)
        audit_logger.propagate = True
        # Restore original REST client so other tests are not affected.
        RESTClientFactory.set_rest_client(PlainRESTClientOperation)

    def test_successful_tool_call_emits_allow_record(self):
        """A successful tool call produces an audit record with outcome=allow."""

        async def _run():
            async with Client(self.mcp) as client:
                await client.call_tool("get_list_of_catalogs")

        asyncio.run(_run())

        self.assertEqual(len(self.log_records), 1)
        record = json.loads(self.log_records[0])
        self.assertEqual(record["tool"], "get_list_of_catalogs")
        self.assertEqual(record["outcome"], "allow")
        self.assertEqual(record["principal"], "anonymous")

    def test_principal_falls_back_to_startup_token(self):
        """With no request header, the audit principal uses the startup --token."""
        RESTClientFactory.set_rest_client(MockOperation)
        server = GravitinoMCPServer(
            Setting("mock_metalake", token="abcdef123456")
        )

        async def _run():
            async with Client(server.mcp) as client:
                await client.call_tool("get_list_of_catalogs")

        asyncio.run(_run())

        record = json.loads(self.log_records[0])
        self.assertEqual(record["principal"], "bearer:abcdef12")

    def test_failed_tool_call_emits_deny_record(self):
        """A tool call that raises an exception produces an audit record with outcome=deny."""

        class FailingOperation(MockOperation):
            def as_catalog_operation(self):
                return _FailingCatalogOperation()

        RESTClientFactory.set_rest_client(FailingOperation)
        server = GravitinoMCPServer(Setting("mock_metalake"))

        async def _run():
            async with Client(server.mcp) as client:
                try:
                    await client.call_tool("get_list_of_catalogs")
                except Exception:  # pylint: disable=broad-exception-caught
                    pass

        asyncio.run(_run())

        deny_records = [
            json.loads(r) for r in self.log_records if '"deny"' in r
        ]
        self.assertTrue(len(deny_records) >= 1)
        self.assertEqual(deny_records[0]["tool"], "get_list_of_catalogs")
        self.assertEqual(deny_records[0]["outcome"], "deny")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _CapturingHandler(logging.Handler):
    """Logging handler that stores formatted messages in a list."""

    def __init__(self, records: list):
        super().__init__()
        self._records = records

    def emit(self, record: logging.LogRecord) -> None:
        self._records.append(self.format(record))


class _FailingCatalogOperation:
    async def get_list_of_catalogs(self) -> str:
        raise GravitinoException("Error code: 1003, Error type: FORBIDDEN")
