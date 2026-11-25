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

"""Unit tests for Gravitino error metadata and MCP error classification."""

import asyncio
import json
import unittest
from unittest.mock import MagicMock, patch

import httpx
from fastmcp import Client
from fastmcp.exceptions import NotFoundError, ToolError
from fastmcp.server.middleware.middleware import Middleware
from mcp.shared.exceptions import McpError
from mcp.types import ErrorData

from mcp_server.client.factory import RESTClientFactory
from mcp_server.client.plain.exception import GravitinoException
from mcp_server.client.plain.plain_rest_client_operation import (
    PlainRESTClientOperation,
    _GravitinoAsyncClient,
)
from mcp_server.client.plain.utils import (
    extract_content_from_response,
    extract_list_from_response,
    extract_response,
)
from mcp_server.core.context import ServiceIdentityFallbackDisabled
from mcp_server.core.errors import (
    JSONRPC_CONFLICT,
    JSONRPC_INTERNAL_ERROR,
    JSONRPC_INVALID_PARAMS,
    JSONRPC_NOT_FOUND,
    JSONRPC_PERMISSION_OR_TIMEOUT,
    JSONRPC_RESOURCE_NOT_FOUND,
    classify_error,
)
from mcp_server.core.setting import Setting
from mcp_server.server import GravitinoMCPServer

# Tests intentionally exercise internal REST client hooks to inject MockTransport.
# pylint: disable=protected-access


class TestRestResponseExceptionMetadata(unittest.TestCase):
    """REST response extractors attach HTTP status, code, and type to GravitinoException."""

    def test_extract_content_attaches_status_code_and_type(self):
        response = httpx.Response(
            403,
            json={
                "code": 1008,
                "type": "ForbiddenException",
                "message": "User 'alice' is not authorized",
            },
        )
        with self.assertRaises(GravitinoException) as raised:
            extract_content_from_response(response, "table", {})

        exc = raised.exception
        self.assertEqual(exc.status_code, 403)
        self.assertEqual(exc.error_code, 1008)
        self.assertEqual(exc.error_type, "ForbiddenException")
        self.assertIn("User 'alice' is not authorized", str(exc))

    def test_extract_response_attaches_status_code_and_type(self):
        response = httpx.Response(
            409,
            json={
                "code": 1010,
                "type": "CatalogInUseException",
                "message": "Catalog is in use",
            },
        )
        with self.assertRaises(GravitinoException) as raised:
            extract_response(response)

        exc = raised.exception
        self.assertEqual(exc.status_code, 409)
        self.assertEqual(exc.error_code, 1010)
        self.assertEqual(exc.error_type, "CatalogInUseException")

    def test_extract_list_attaches_status_code_and_type(self):
        response = httpx.Response(
            404,
            content=json.dumps(
                {
                    "code": 1003,
                    "type": "NoSuchSchemaException",
                    "message": "Schema not found",
                }
            ).encode("utf-8"),
        )
        with self.assertRaises(GravitinoException) as raised:
            extract_list_from_response(response, "identifiers")

        exc = raised.exception
        self.assertEqual(exc.status_code, 404)
        self.assertEqual(exc.error_code, 1003)
        self.assertEqual(exc.error_type, "NoSuchSchemaException")

    def test_malformed_200_response_has_no_client_error_status(self):
        response = httpx.Response(
            200,
            request=httpx.Request("GET", "http://gravitino/api/test"),
            content=b"not json",
        )
        with self.assertRaises(GravitinoException) as raised:
            extract_list_from_response(response, "identifiers")

        classification = classify_error(raised.exception)
        self.assertEqual(classification.mcp_code, JSONRPC_INTERNAL_ERROR)
        self.assertEqual(classification.outcome, "error")
        self.assertEqual(classification.category, "internal_error")


class TestClassifyError(unittest.TestCase):
    """classify_error maps Gravitino, HTTP, and Python errors consistently."""

    def test_gravitino_http_status_mapping(self):
        cases = [
            (
                401,
                1011,
                "UnauthorizedException",
                JSONRPC_PERMISSION_OR_TIMEOUT,
                "Unauthorized",
                "deny",
                "unauthorized",
            ),
            (
                403,
                1008,
                "ForbiddenException",
                JSONRPC_PERMISSION_OR_TIMEOUT,
                "Permission denied",
                "deny",
                "permission_denied",
            ),
            (
                400,
                1001,
                "IllegalArgumentException",
                JSONRPC_INVALID_PARAMS,
                "Invalid params",
                "error",
                "invalid_params",
            ),
            (
                404,
                1003,
                "NoSuchTableException",
                JSONRPC_NOT_FOUND,
                "Not found",
                "error",
                "not_found",
            ),
            (
                409,
                1004,
                "TableAlreadyExistsException",
                JSONRPC_CONFLICT,
                "Conflict",
                "error",
                "conflict",
            ),
            (
                409,
                1010,
                "CatalogInUseException",
                JSONRPC_CONFLICT,
                "Conflict",
                "error",
                "conflict",
            ),
            (
                500,
                1002,
                "RuntimeException",
                JSONRPC_INTERNAL_ERROR,
                "Internal error",
                "error",
                "internal_error",
            ),
            (
                502,
                1007,
                "ConnectionFailedException",
                JSONRPC_INTERNAL_ERROR,
                "Internal error",
                "error",
                "unavailable",
            ),
            (
                503,
                1013,
                "ServiceUnavailableException",
                JSONRPC_INTERNAL_ERROR,
                "Internal error",
                "error",
                "unavailable",
            ),
        ]
        for (
            status,
            code,
            err_type,
            expected_mcp_code,
            expected_prefix,
            expected_outcome,
            expected_category,
        ) in cases:
            with self.subTest(status=status, err_type=err_type):
                exc = GravitinoException(
                    "detail",
                    status_code=status,
                    error_code=code,
                    error_type=err_type,
                )
                result = classify_error(exc)
                self.assertEqual(result.mcp_code, expected_mcp_code)
                self.assertEqual(result.prefix, expected_prefix)
                self.assertEqual(result.outcome, expected_outcome)
                self.assertEqual(result.error_type, err_type)
                self.assertEqual(result.category, expected_category)

    def test_gravitino_error_code_fallback_without_http_status(self):
        cases = [
            (
                1011,
                "UnauthorizedException",
                JSONRPC_PERMISSION_OR_TIMEOUT,
                "deny",
            ),
            (1008, "ForbiddenException", JSONRPC_PERMISSION_OR_TIMEOUT, "deny"),
            (1001, "IllegalArgumentException", JSONRPC_INVALID_PARAMS, "error"),
            (1003, "NoSuchTableException", JSONRPC_NOT_FOUND, "error"),
            (1004, "AlreadyExistsException", JSONRPC_CONFLICT, "error"),
            (1005, "NonEmptySchemaException", JSONRPC_CONFLICT, "error"),
            (1009, "CatalogNotInUseException", JSONRPC_CONFLICT, "error"),
            (1010, "CatalogInUseException", JSONRPC_CONFLICT, "error"),
            (1012, "OptimisticLockException", JSONRPC_CONFLICT, "error"),
            (
                1007,
                "ConnectionFailedException",
                JSONRPC_INTERNAL_ERROR,
                "error",
            ),
            (
                1013,
                "ServiceUnavailableException",
                JSONRPC_INTERNAL_ERROR,
                "error",
            ),
        ]
        for error_code, error_type, expected_code, expected_outcome in cases:
            with self.subTest(error_code=error_code):
                exc = GravitinoException(
                    "msg", error_code=error_code, error_type=error_type
                )
                result = classify_error(exc)
                self.assertEqual(result.mcp_code, expected_code)
                self.assertEqual(result.outcome, expected_outcome)
                self.assertEqual(result.error_type, error_type)

    def test_unwraps_nested_mcp_and_tool_errors(self):
        root = GravitinoException(
            "denied",
            status_code=403,
            error_code=1008,
            error_type="ForbiddenException",
        )
        tool_err = ToolError("Error calling tool 'drop_table': denied")
        tool_err.__cause__ = root
        mcp_err = McpError(
            ErrorData(
                code=JSONRPC_PERMISSION_OR_TIMEOUT, message="Permission denied"
            )
        )
        mcp_err.__cause__ = tool_err

        result = classify_error(mcp_err)
        self.assertEqual(result.outcome, "deny")
        self.assertEqual(result.error_type, "ForbiddenException")
        self.assertEqual(result.category, "permission_denied")

    def test_non_json_http_status_error_and_connection_errors(self):
        request = httpx.Request("GET", "http://gravitino/api/metalakes")
        for status, expected_code, expected_outcome, expected_cat in [
            (401, JSONRPC_PERMISSION_OR_TIMEOUT, "deny", "unauthorized"),
            (403, JSONRPC_PERMISSION_OR_TIMEOUT, "deny", "permission_denied"),
            (400, JSONRPC_INVALID_PARAMS, "error", "invalid_params"),
            (404, JSONRPC_NOT_FOUND, "error", "not_found"),
            (409, JSONRPC_CONFLICT, "error", "conflict"),
            (503, JSONRPC_INTERNAL_ERROR, "error", "unavailable"),
            (500, JSONRPC_INTERNAL_ERROR, "error", "internal_error"),
        ]:
            resp = httpx.Response(status, request=request, text="plain error")
            exc = httpx.HTTPStatusError("err", request=request, response=resp)
            result = classify_error(exc)
            self.assertEqual(result.mcp_code, expected_code)
            self.assertEqual(result.outcome, expected_outcome)
            self.assertEqual(result.category, expected_cat)

        connect_err = httpx.ConnectError(
            "All connection attempts failed", request=request
        )
        connect_res = classify_error(connect_err)
        self.assertEqual(connect_res.mcp_code, JSONRPC_INTERNAL_ERROR)
        self.assertEqual(connect_res.outcome, "error")
        self.assertEqual(connect_res.error_type, "ConnectError")
        self.assertEqual(connect_res.category, "unavailable")

    def test_python_and_fastmcp_exceptions(self):
        res = classify_error(ServiceIdentityFallbackDisabled("no header"))
        self.assertEqual(res.mcp_code, JSONRPC_PERMISSION_OR_TIMEOUT)
        self.assertEqual(res.outcome, "deny")
        self.assertEqual(res.category, "unauthorized")

        res = classify_error(NotFoundError("missing"), method="resources/read")
        self.assertEqual(res.mcp_code, JSONRPC_RESOURCE_NOT_FOUND)
        self.assertEqual(res.prefix, "Resource not found")

        res = classify_error(PermissionError("no access"))
        self.assertEqual(res.outcome, "deny")
        self.assertEqual(res.category, "permission_denied")

        res = classify_error(ValueError("bad input"))
        self.assertEqual(res.mcp_code, JSONRPC_INVALID_PARAMS)
        self.assertEqual(res.outcome, "error")

        res = classify_error(RuntimeError("unexpected"))
        self.assertEqual(res.mcp_code, JSONRPC_INTERNAL_ERROR)
        self.assertEqual(res.outcome, "error")
        self.assertEqual(res.error_type, "RuntimeError")


class _CaptureMcpErrorMiddleware(Middleware):
    """Middleware placed outside ErrorHandlingMiddleware to inspect McpError codes."""

    def __init__(self, captured: list):
        super().__init__()
        self._captured = captured

    async def on_call_tool(self, context, call_next):
        try:
            return await call_next(context)
        except Exception as exc:
            self._captured.append(exc)
            raise


class TestEndToEndMcpErrorMapping(unittest.TestCase):
    """Verify Gravitino REST responses map to distinct MCP errors through the full stack."""

    def setUp(self):
        RESTClientFactory.set_rest_client(PlainRESTClientOperation)

    def _call_with_transport(
        self,
        handler,
        tool_name: str,
        arguments: dict,
        setting: Setting | None = None,
    ):
        original_init = _GravitinoAsyncClient.__init__
        captured_errors: list = []

        def init_with_transport(client, *args, **kwargs):
            kwargs["transport"] = httpx.MockTransport(handler)
            original_init(client, *args, **kwargs)

        async def _run():
            with patch.object(
                _GravitinoAsyncClient, "__init__", init_with_transport
            ):
                server = GravitinoMCPServer(
                    setting or Setting(metalake="authz")
                )
                server.mcp.middleware.insert(
                    0, _CaptureMcpErrorMiddleware(captured_errors)
                )
                async with Client(server.mcp) as client:
                    return await client.call_tool(
                        tool_name, arguments, raise_on_error=False
                    )

        result = asyncio.run(_run())
        return result, captured_errors

    def test_gravitino_rest_errors_are_not_reported_as_internal_error(self):
        cases = [
            (
                403,
                1008,
                "ForbiddenException",
                "User 'alice' is not authorized to drop table",
                JSONRPC_PERMISSION_OR_TIMEOUT,
                "Permission denied:",
            ),
            (
                401,
                1011,
                "UnauthorizedException",
                "Authentication required",
                JSONRPC_PERMISSION_OR_TIMEOUT,
                "Unauthorized:",
            ),
            (
                400,
                1001,
                "IllegalArgumentException",
                "Malformed json request",
                JSONRPC_INVALID_PARAMS,
                "Invalid params:",
            ),
            (
                404,
                1003,
                "NoSuchTableException",
                "Table does not exist",
                JSONRPC_NOT_FOUND,
                "Not found:",
            ),
            (
                409,
                1010,
                "CatalogInUseException",
                "Catalog is still in use",
                JSONRPC_CONFLICT,
                "Conflict:",
            ),
        ]
        for (
            status,
            code,
            err_type,
            detail,
            expected_mcp_code,
            expected_prefix,
        ) in cases:
            with self.subTest(status=status, err_type=err_type):

                def handler(
                    _request,
                    st=status,
                    ec=code,
                    et=err_type,
                    msg=detail,
                ):
                    return httpx.Response(
                        st, json={"code": ec, "type": et, "message": msg}
                    )

                result, captured = self._call_with_transport(
                    handler,
                    "drop_table",
                    {
                        "catalog_name": "cat",
                        "schema_name": "sch",
                        "table_name": "tbl",
                    },
                )
                self.assertTrue(result.is_error)
                text = " ".join(item.text for item in result.content)
                self.assertTrue(text.startswith(expected_prefix), text)
                self.assertNotIn("Internal error:", text)
                self.assertIn(detail, text)
                self.assertEqual(len(captured), 1)
                self.assertIsInstance(captured[0], McpError)
                self.assertEqual(captured[0].error.code, expected_mcp_code)

    def test_connection_failure_and_5xx_remain_internal_error(self):
        def connect_handler(request):
            raise httpx.ConnectError(
                "All connection attempts failed", request=request
            )

        result, captured = self._call_with_transport(
            connect_handler, "get_list_of_catalogs", {}
        )
        self.assertTrue(result.is_error)
        text = " ".join(item.text for item in result.content)
        self.assertTrue(text.startswith("Internal error:"), text)
        self.assertEqual(captured[0].error.code, JSONRPC_INTERNAL_ERROR)

        def server_err_handler(_request):
            return httpx.Response(
                500,
                json={
                    "code": 1002,
                    "type": "RuntimeException",
                    "message": "Server boom",
                },
            )

        result, captured = self._call_with_transport(
            server_err_handler, "get_list_of_catalogs", {}
        )
        self.assertTrue(result.is_error)
        text = " ".join(item.text for item in result.content)
        self.assertTrue(text.startswith("Internal error:"), text)
        self.assertIn("Server boom", text)
        self.assertEqual(captured[0].error.code, JSONRPC_INTERNAL_ERROR)

    def test_read_timeout_maps_to_request_timeout_with_actionable_text(self):
        def timeout_handler(request):
            raise httpx.ReadTimeout("slow", request=request)

        result, captured = self._call_with_transport(
            timeout_handler,
            "get_table_metadata_details",
            {"catalog_name": "c", "schema_name": "s", "table_name": "t"},
            setting=Setting(metalake="m", gravitino_read_timeout=30),
        )
        self.assertTrue(result.is_error)
        text = " ".join(item.text for item in result.content)
        self.assertTrue(text.startswith("Request timeout:"), text)
        self.assertIn("30 seconds", text)
        self.assertIn("may still complete", text)
        self.assertEqual(captured[0].error.code, JSONRPC_PERMISSION_OR_TIMEOUT)

    def test_missing_required_tool_argument_maps_to_invalid_params(self):
        def unused_handler(_request):
            raise AssertionError("HTTP call should not be reached")

        result, captured = self._call_with_transport(
            unused_handler, "drop_table", {}
        )
        self.assertTrue(result.is_error)
        text = " ".join(item.text for item in result.content)
        self.assertTrue(text.startswith("Invalid params:"), text)
        self.assertEqual(captured[0].error.code, JSONRPC_INVALID_PARAMS)

    def test_service_identity_fallback_disabled_maps_to_unauthorized(self):
        def unused_handler(_request):
            raise AssertionError("HTTP call should not be reached")

        mock_request = MagicMock()
        mock_request.headers.get.return_value = ""
        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=mock_request,
        ):
            result, captured = self._call_with_transport(
                unused_handler,
                "get_list_of_catalogs",
                {},
                setting=Setting(
                    metalake="authz",
                    transport="http",
                    token="static-token",
                    no_service_identity_fallback=True,
                ),
            )
        self.assertTrue(result.is_error)
        text = " ".join(item.text for item in result.content)
        self.assertTrue(text.startswith("Unauthorized:"), text)
        self.assertEqual(captured[0].error.code, JSONRPC_PERMISSION_OR_TIMEOUT)
