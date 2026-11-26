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
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from fastmcp import Client
from fastmcp.exceptions import NotFoundError, ToolError
from mcp.shared.exceptions import McpError, UrlElicitationRequiredError
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
    JSONRPC_RESOURCE_NOT_FOUND,
    JSONRPC_SERVER_ERROR,
    GravitinoErrorHandlingMiddleware,
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
        for extract, args in (
            (extract_response, ()),
            (extract_content_from_response, ("table",)),
            (extract_list_from_response, ("identifiers",)),
        ):
            with self.subTest(extract=extract.__name__):
                with self.assertRaises(GravitinoException) as raised:
                    extract(response, *args)
                self.assertIsInstance(raised.exception.__cause__, ValueError)
                classification = classify_error(raised.exception)
                self.assertEqual(
                    classification.mcp_code, JSONRPC_INTERNAL_ERROR
                )
                self.assertEqual(classification.category, "internal_error")


class TestClassifyError(unittest.TestCase):
    """classify_error maps Gravitino, HTTP, and Python errors consistently."""

    def test_gravitino_http_status_mapping(self):
        cases = [
            (
                401,
                1011,
                "UnauthorizedException",
                JSONRPC_SERVER_ERROR,
                "Unauthorized",
                "deny",
                "unauthorized",
            ),
            (
                403,
                1008,
                "ForbiddenException",
                JSONRPC_SERVER_ERROR,
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
                JSONRPC_SERVER_ERROR,
                "deny",
            ),
            (1008, "ForbiddenException", JSONRPC_SERVER_ERROR, "deny"),
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
            ErrorData(code=JSONRPC_SERVER_ERROR, message="Permission denied")
        )
        mcp_err.__cause__ = tool_err

        result = classify_error(mcp_err)
        self.assertEqual(result.outcome, "deny")
        self.assertEqual(result.error_type, "ForbiddenException")
        self.assertEqual(result.category, "permission_denied")

    def test_non_json_http_status_error_and_connection_errors(self):
        request = httpx.Request("GET", "http://gravitino/api/metalakes")
        for status, expected_code, expected_outcome, expected_cat in [
            (401, JSONRPC_SERVER_ERROR, "deny", "unauthorized"),
            (403, JSONRPC_SERVER_ERROR, "deny", "permission_denied"),
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
        self.assertEqual(res.mcp_code, JSONRPC_SERVER_ERROR)
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


class TestProtocolErrors(unittest.IsolatedAsyncioTestCase):
    async def test_protocol_errors_keep_their_code_and_data(self):
        middleware = GravitinoErrorHandlingMiddleware()
        challenge = UrlElicitationRequiredError([])
        native = McpError(
            ErrorData(
                code=-32602,
                message="Invalid resource URI",
                data={"field": "uri"},
            )
        )
        for method, error in (
            ("tools/call", challenge),
            ("resources/read", native),
        ):
            with self.subTest(method=method):
                with self.assertRaises(McpError) as raised:
                    await middleware.on_message(
                        MagicMock(method=method), AsyncMock(side_effect=error)
                    )
                self.assertIs(raised.exception, error)


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
                async with Client(server.mcp) as client:
                    return await client.call_tool_mcp(tool_name, arguments)

        return asyncio.run(_run())

    def test_gravitino_rest_errors_are_not_reported_as_internal_error(self):
        cases = [
            (
                403,
                1008,
                "ForbiddenException",
                "User 'alice' is not authorized to drop table",
                JSONRPC_SERVER_ERROR,
                "Permission denied:",
            ),
            (
                401,
                1011,
                "UnauthorizedException",
                "Authentication required",
                JSONRPC_SERVER_ERROR,
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

                result = self._call_with_transport(
                    handler,
                    "drop_table",
                    {
                        "catalog_name": "cat",
                        "schema_name": "sch",
                        "table_name": "tbl",
                    },
                )
                self.assertTrue(result.isError)
                text = " ".join(item.text for item in result.content)
                self.assertTrue(text.startswith(expected_prefix), text)
                self.assertNotIn("Internal error:", text)
                self.assertIn(detail, text)
                self.assertEqual(
                    result.meta["org.apache.gravitino/error"]["code"],
                    expected_mcp_code,
                )

    def test_cancel_job_denials_reach_client_and_audit(self):
        for status, rest_code, error_type, category, prefix in (
            (
                401,
                1011,
                "UnauthorizedException",
                "unauthorized",
                "Unauthorized:",
            ),
            (
                403,
                1008,
                "ForbiddenException",
                "permission_denied",
                "Permission denied:",
            ),
        ):
            with self.subTest(status=status):

                handler = MagicMock(
                    return_value=httpx.Response(
                        status,
                        json={
                            "code": rest_code,
                            "type": error_type,
                            "message": "Cannot cancel this job",
                        },
                    )
                )

                with patch("mcp_server.server.audit.emit") as emit:
                    result = self._call_with_transport(
                        handler, "cancel_job", {"job_id": "job-42"}
                    )
                handler.assert_called_once()
                request = handler.call_args.args[0]
                self.assertEqual(request.method, "POST")
                self.assertEqual(
                    request.url.path, "/api/metalakes/authz/jobs/runs/job-42"
                )
                self.assertTrue(result.isError)
                self.assertTrue(result.content[0].text.startswith(prefix))
                self.assertIn("Cannot cancel this job", result.content[0].text)
                self.assertEqual(
                    result.meta["org.apache.gravitino/error"],
                    {
                        "code": JSONRPC_SERVER_ERROR,
                        "category": category,
                    },
                )
                emit.assert_called_once_with(
                    principal="anonymous",
                    tool="cancel_job",
                    outcome="deny",
                    error_type=error_type,
                    error_category=category,
                    metalake="authz",
                    target_type="job",
                    target_name="job-42",
                )

    def test_unresolved_metalake_omits_tenant_and_target_from_audit(self):
        for default, arguments in (("prod", {"metalake": False}), ("", {})):
            with self.subTest(default=default, arguments=arguments):
                rest = MagicMock()
                with patch("mcp_server.core.audit._audit_logger.info") as log:
                    result = self._call_with_transport(
                        rest,
                        "drop_table",
                        {
                            "catalog_name": "c",
                            "schema_name": "s",
                            "table_name": "t",
                            **arguments,
                        },
                        setting=Setting(metalake=default),
                    )
                self.assertTrue(result.isError)
                self.assertEqual(
                    result.meta["org.apache.gravitino/error"],
                    {
                        "code": JSONRPC_INVALID_PARAMS,
                        "category": "invalid_params",
                    },
                )
                rest.assert_not_called()
                log.assert_called_once()
                record = json.loads(log.call_args.args[0])
                self.assertEqual(record["outcome"], "error")
                self.assertEqual(record["error_category"], "invalid_params")
                for field in ("metalake", "target_type", "target_name"):
                    self.assertNotIn(field, record)

    def test_model_version_audit_matches_the_rest_target(self):
        for version in (1.0, "01", True):
            with self.subTest(version=version):
                handler = MagicMock(
                    return_value=httpx.Response(
                        200, json={"code": 0, "dropped": True}
                    )
                )
                with patch("mcp_server.server.audit.emit") as emit:
                    result = self._call_with_transport(
                        handler,
                        "delete_model_version",
                        {
                            "catalog_name": "c",
                            "schema_name": "s",
                            "model_name": " m ",
                            "version": version,
                        },
                    )
                self.assertFalse(result.isError)
                self.assertEqual(
                    handler.call_args.args[0].url.path,
                    "/api/metalakes/authz/catalogs/c/schemas/s/models/ m /versions/1",
                )
                self.assertEqual(
                    emit.call_args.kwargs["target_name"], "authz.c.s. m .1"
                )
                self.assertEqual(
                    emit.call_args.kwargs["target_type"], "model_version"
                )

    def test_connection_failure_and_5xx_remain_internal_error(self):
        def connect_handler(request):
            raise httpx.ConnectError(
                "All connection attempts failed", request=request
            )

        result = self._call_with_transport(
            connect_handler, "get_list_of_catalogs", {}
        )
        self.assertTrue(result.isError)
        text = " ".join(item.text for item in result.content)
        self.assertTrue(text.startswith("Internal error:"), text)
        self.assertEqual(
            result.meta["org.apache.gravitino/error"]["code"],
            JSONRPC_INTERNAL_ERROR,
        )

        def server_err_handler(_request):
            return httpx.Response(
                500,
                json={
                    "code": 1002,
                    "type": "RuntimeException",
                    "message": "Server boom",
                },
            )

        result = self._call_with_transport(
            server_err_handler, "get_list_of_catalogs", {}
        )
        self.assertTrue(result.isError)
        text = " ".join(item.text for item in result.content)
        self.assertTrue(text.startswith("Internal error:"), text)
        self.assertIn("Server boom", text)
        self.assertEqual(
            result.meta["org.apache.gravitino/error"]["code"],
            JSONRPC_INTERNAL_ERROR,
        )

    def test_upstream_rate_limit_is_reported_to_client_and_audit(self):
        for response in (
            httpx.Response(429, text="Too many requests"),
            httpx.Response(
                429,
                json={
                    "code": 429,
                    "type": "RateLimit",
                    "message": "Too many requests",
                },
            ),
        ):
            with self.subTest(response=response), patch(
                "mcp_server.server.audit.emit"
            ) as emit:
                result = self._call_with_transport(
                    MagicMock(return_value=response), "get_list_of_catalogs", {}
                )
                self.assertTrue(result.isError)
                self.assertTrue(
                    result.content[0].text.startswith("Rate limited:")
                )
                self.assertEqual(
                    result.meta["org.apache.gravitino/error"],
                    {"code": -32000, "category": "rate_limited"},
                )
                self.assertEqual(emit.call_args.kwargs["outcome"], "error")
                self.assertEqual(
                    emit.call_args.kwargs["error_category"], "rate_limited"
                )

    def _assert_oauth_failure(self, response, code, category, error_type):
        setting = Setting(
            metalake="m",
            oauth_token_endpoint="https://idp.example/token",
            oauth_client_id="client",
            oauth_client_secret="secret",
        )
        idp = MagicMock(return_value=response)
        rest = MagicMock()
        with patch(
            "mcp_server.core.oauth.httpx.AsyncClient",
            return_value=httpx.AsyncClient(transport=httpx.MockTransport(idp)),
        ), patch("mcp_server.core.audit._audit_logger.info") as log:
            result = self._call_with_transport(
                rest, "get_list_of_catalogs", {}, setting=setting
            )
        idp.assert_called_once()
        rest.assert_not_called()
        self.assertTrue(result.isError)
        self.assertEqual(
            result.meta["org.apache.gravitino/error"],
            {"code": code, "category": category},
        )
        log.assert_called_once()
        record = json.loads(log.call_args.args[0])
        self.assertEqual(record["error_type"], error_type)
        self.assertEqual(record["error_category"], category)
        self.assertEqual(record["outcome"], "error")
        return result.content[0].text

    def test_malformed_oauth_responses_are_upstream_errors(self):
        for body in (
            b"{}",
            b"bad JSON",
            b"[]",
            b'{"access_token":"opaque","expires_in":"invalid"}',
        ):
            with self.subTest(body=body):
                text = self._assert_oauth_failure(
                    httpx.Response(200, content=body),
                    JSONRPC_INTERNAL_ERROR,
                    "internal_error",
                    "OAuthTokenResponseError",
                )
                self.assertIn("OAuth token response", text)

    def test_oauth_endpoint_failures_are_distinct_from_gravitino_errors(self):
        for status, code, category in (
            (400, JSONRPC_INTERNAL_ERROR, "internal_error"),
            (401, JSONRPC_INTERNAL_ERROR, "internal_error"),
            (403, JSONRPC_INTERNAL_ERROR, "internal_error"),
            (404, JSONRPC_INTERNAL_ERROR, "internal_error"),
            (408, JSONRPC_SERVER_ERROR, "timeout"),
            (429, JSONRPC_SERVER_ERROR, "rate_limited"),
            (503, JSONRPC_INTERNAL_ERROR, "unavailable"),
        ):
            with self.subTest(status=status):
                text = self._assert_oauth_failure(
                    httpx.Response(status, json={"error": "invalid_client"}),
                    code,
                    category,
                    "OAuthTokenEndpointError",
                )
                if category == "internal_error":
                    self.assertIn(
                        f"OAuth token endpoint returned HTTP {status}", text
                    )

    def test_read_timeout_maps_to_request_timeout_with_actionable_text(self):
        def timeout_handler(request):
            raise httpx.ReadTimeout("slow", request=request)

        result = self._call_with_transport(
            timeout_handler,
            "get_table_metadata_details",
            {"catalog_name": "c", "schema_name": "s", "table_name": "t"},
            setting=Setting(metalake="m", gravitino_read_timeout=30),
        )
        self.assertTrue(result.isError)
        text = " ".join(item.text for item in result.content)
        self.assertTrue(text.startswith("Request timeout:"), text)
        self.assertIn("30 seconds", text)
        self.assertIn("may still complete", text)
        self.assertEqual(
            result.meta["org.apache.gravitino/error"]["code"],
            JSONRPC_SERVER_ERROR,
        )

    def test_missing_required_tool_argument_maps_to_invalid_params(self):
        def unused_handler(_request):
            raise AssertionError("HTTP call should not be reached")

        result = self._call_with_transport(unused_handler, "drop_table", {})
        self.assertTrue(result.isError)
        text = " ".join(item.text for item in result.content)
        self.assertTrue(text.startswith("Invalid params:"), text)
        self.assertEqual(
            result.meta["org.apache.gravitino/error"]["code"],
            JSONRPC_INVALID_PARAMS,
        )

    def test_service_identity_fallback_disabled_maps_to_unauthorized(self):
        def unused_handler(_request):
            raise AssertionError("HTTP call should not be reached")

        mock_request = MagicMock()
        mock_request.headers.get.return_value = ""
        with patch(
            "fastmcp.server.dependencies.get_http_request",
            return_value=mock_request,
        ):
            result = self._call_with_transport(
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
        self.assertTrue(result.isError)
        text = " ".join(item.text for item in result.content)
        self.assertTrue(text.startswith("Unauthorized:"), text)
        self.assertEqual(
            result.meta["org.apache.gravitino/error"]["code"],
            JSONRPC_SERVER_ERROR,
        )
