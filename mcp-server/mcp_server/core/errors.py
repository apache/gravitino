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
from dataclasses import dataclass
from typing import Optional

import httpx
from fastmcp.exceptions import (
    AuthorizationError,
    DisabledError,
    NotFoundError,
    ToolError,
)
from fastmcp.exceptions import ValidationError as FastMCPValidationError
from fastmcp.server.middleware.error_handling import ErrorHandlingMiddleware
from fastmcp.server.middleware.middleware import MiddlewareContext
from mcp.shared.exceptions import McpError
from mcp.types import ErrorData

from mcp_server.client.plain.exception import GravitinoException
from mcp_server.core.context import ServiceIdentityFallbackDisabled

# Standard JSON-RPC error codes and server-defined codes (-32000..-32099).
JSONRPC_INVALID_PARAMS = -32602
JSONRPC_INTERNAL_ERROR = -32603
JSONRPC_PERMISSION_OR_TIMEOUT = -32000
JSONRPC_NOT_FOUND = -32001
JSONRPC_RESOURCE_NOT_FOUND = -32002
JSONRPC_CONFLICT = -32009

# Gravitino REST ErrorConstants (org.apache.gravitino.dto.responses.ErrorConstants).
_GRAVITINO_ILLEGAL_ARGUMENTS_CODE = 1001
_GRAVITINO_NOT_FOUND_CODE = 1003
_GRAVITINO_ALREADY_EXISTS_CODE = 1004
_GRAVITINO_NON_EMPTY_CODE = 1005
_GRAVITINO_CONNECTION_FAILED_CODE = 1007
_GRAVITINO_FORBIDDEN_CODE = 1008
_GRAVITINO_NOT_IN_USE_CODE = 1009
_GRAVITINO_IN_USE_CODE = 1010
_GRAVITINO_UNAUTHORIZED_CODE = 1011
_GRAVITINO_OPTIMISTIC_LOCK_CONFLICT_CODE = 1012
_GRAVITINO_SERVICE_UNAVAILABLE_CODE = 1013

# (mcp_code, prefix, outcome, category, default_error_type)
_Rule = tuple[int, str, str, str, str]

_UNAUTHORIZED_RULE: _Rule = (
    JSONRPC_PERMISSION_OR_TIMEOUT,
    "Unauthorized",
    "deny",
    "unauthorized",
    "UnauthorizedException",
)
_PERMISSION_DENIED_RULE: _Rule = (
    JSONRPC_PERMISSION_OR_TIMEOUT,
    "Permission denied",
    "deny",
    "permission_denied",
    "ForbiddenException",
)
_INVALID_PARAMS_RULE: _Rule = (
    JSONRPC_INVALID_PARAMS,
    "Invalid params",
    "error",
    "invalid_params",
    "",
)
_NOT_FOUND_RULE: _Rule = (
    JSONRPC_NOT_FOUND,
    "Not found",
    "error",
    "not_found",
    "",
)
_CONFLICT_RULE: _Rule = (
    JSONRPC_CONFLICT,
    "Conflict",
    "error",
    "conflict",
    "",
)
_TIMEOUT_RULE: _Rule = (
    JSONRPC_PERMISSION_OR_TIMEOUT,
    "Request timeout",
    "error",
    "timeout",
    "",
)
_UNAVAILABLE_RULE: _Rule = (
    JSONRPC_INTERNAL_ERROR,
    "Internal error",
    "error",
    "unavailable",
    "",
)
_INTERNAL_ERROR_RULE: _Rule = (
    JSONRPC_INTERNAL_ERROR,
    "Internal error",
    "error",
    "internal_error",
    "",
)

_HTTP_STATUS_RULES: dict[int, _Rule] = {
    400: _INVALID_PARAMS_RULE,
    401: _UNAUTHORIZED_RULE,
    403: _PERMISSION_DENIED_RULE,
    404: _NOT_FOUND_RULE,
    408: _TIMEOUT_RULE,
    409: _CONFLICT_RULE,
    422: _INVALID_PARAMS_RULE,
    502: _UNAVAILABLE_RULE,
    503: _UNAVAILABLE_RULE,
}

_GRAVITINO_CODE_RULES: dict[int, _Rule] = {
    _GRAVITINO_UNAUTHORIZED_CODE: _UNAUTHORIZED_RULE,
    _GRAVITINO_FORBIDDEN_CODE: _PERMISSION_DENIED_RULE,
    _GRAVITINO_ILLEGAL_ARGUMENTS_CODE: _INVALID_PARAMS_RULE,
    _GRAVITINO_NOT_FOUND_CODE: _NOT_FOUND_RULE,
    _GRAVITINO_ALREADY_EXISTS_CODE: _CONFLICT_RULE,
    _GRAVITINO_NON_EMPTY_CODE: _CONFLICT_RULE,
    _GRAVITINO_NOT_IN_USE_CODE: _CONFLICT_RULE,
    _GRAVITINO_IN_USE_CODE: _CONFLICT_RULE,
    _GRAVITINO_OPTIMISTIC_LOCK_CONFLICT_CODE: _CONFLICT_RULE,
    _GRAVITINO_CONNECTION_FAILED_CODE: _UNAVAILABLE_RULE,
    _GRAVITINO_SERVICE_UNAVAILABLE_CODE: _UNAVAILABLE_RULE,
}

_MCP_CODE_RULES: dict[int, _Rule] = {
    JSONRPC_INVALID_PARAMS: _INVALID_PARAMS_RULE,
    JSONRPC_NOT_FOUND: _NOT_FOUND_RULE,
    JSONRPC_RESOURCE_NOT_FOUND: _NOT_FOUND_RULE,
    JSONRPC_CONFLICT: _CONFLICT_RULE,
}


@dataclass(frozen=True)
class ErrorClassification:
    """Normalized classification of a tool-call failure.

    Shared by MCP error transformation (JSON-RPC code and message prefix) and
    audit logging (outcome, error_type, error_category) so clients and audit
    logs agree on why a call failed.
    """

    mcp_code: int
    prefix: str
    outcome: str
    error_type: str
    category: str


def _from_rule(
    rule: _Rule,
    *,
    error_type: str,
    fallback_type: str,
    method: str = "",
) -> ErrorClassification:
    mcp_code, prefix, outcome, category, rule_default_type = rule
    if mcp_code == JSONRPC_NOT_FOUND and method.startswith("resources/"):
        mcp_code = JSONRPC_RESOURCE_NOT_FOUND
        prefix = "Resource not found"
    resolved_type = error_type or rule_default_type or fallback_type
    return ErrorClassification(
        mcp_code=mcp_code,
        prefix=prefix,
        outcome=outcome,
        error_type=resolved_type,
        category=category,
    )


def unwrap_exception(exc: BaseException) -> BaseException:
    """Unwrap MCP/FastMCP wrapper exceptions to find the underlying failure."""
    current = exc
    seen = set()
    while (
        isinstance(current, (McpError, ToolError))
        and current.__cause__ is not None
        and id(current.__cause__) not in seen
    ):
        seen.add(id(current))
        current = current.__cause__
    return current


def _classify_http_status(
    status_code: Optional[int],
    *,
    error_type: str,
    default_error_type: str,
    method: str = "",
) -> Optional[ErrorClassification]:
    """Map an HTTP status code (>= 400) to an ErrorClassification, or None."""
    if status_code is None or status_code < 400:
        return None
    rule = _HTTP_STATUS_RULES.get(status_code)
    if rule is None:
        rule = (
            _INVALID_PARAMS_RULE if status_code < 500 else _INTERNAL_ERROR_RULE
        )
    return _from_rule(
        rule,
        error_type=error_type,
        fallback_type=default_error_type,
        method=method,
    )


def _classify_gravitino_exception(
    exc: GravitinoException, method: str = ""
) -> ErrorClassification:
    """Classify a GravitinoException using timeout cause, HTTP status, or error code."""
    if isinstance(exc.__cause__, httpx.TimeoutException) or (
        exc.error_type == "ReadTimeout"
    ):
        timeout_type = exc.error_type or type(exc.__cause__).__name__
        return _from_rule(
            _TIMEOUT_RULE,
            error_type=timeout_type,
            fallback_type="ReadTimeout",
            method=method,
        )

    by_status = _classify_http_status(
        exc.status_code,
        error_type=exc.error_type,
        default_error_type="GravitinoException",
        method=method,
    )
    if by_status is not None:
        return by_status

    rule = (
        _GRAVITINO_CODE_RULES.get(exc.error_code, _INTERNAL_ERROR_RULE)
        if exc.error_code is not None
        else _INTERNAL_ERROR_RULE
    )
    return _from_rule(
        rule,
        error_type=exc.error_type,
        fallback_type="GravitinoException",
        method=method,
    )


def _classify_bare_mcp_error(exc: McpError) -> ErrorClassification:
    """Classify an McpError raised directly without an inner Python cause."""
    code = exc.error.code
    rule = _MCP_CODE_RULES.get(code)
    if rule is None and code == JSONRPC_PERMISSION_OR_TIMEOUT:
        message = (exc.error.message or "").lower()
        if "unauthorized" in message:
            rule = _UNAUTHORIZED_RULE
        elif "permission" in message or "forbidden" in message:
            rule = _PERMISSION_DENIED_RULE
        elif "timeout" in message:
            rule = _TIMEOUT_RULE
    resolved = rule or _INTERNAL_ERROR_RULE
    return ErrorClassification(
        mcp_code=code,
        prefix=resolved[1],
        outcome=resolved[2],
        error_type="McpError",
        category=resolved[3],
    )


def classify_error(exc: BaseException, method: str = "") -> ErrorClassification:
    """Classify any tool/middleware exception for MCP responses and audit logs."""
    root = unwrap_exception(exc)

    if isinstance(root, GravitinoException):
        return _classify_gravitino_exception(root, method=method)

    if isinstance(root, httpx.HTTPStatusError):
        by_status = _classify_http_status(
            root.response.status_code,
            error_type=type(root).__name__,
            default_error_type="HTTPStatusError",
            method=method,
        )
        if by_status is not None:
            return by_status

    if isinstance(root, McpError):
        return _classify_bare_mcp_error(root)

    root_type = type(root).__name__
    rule = _INTERNAL_ERROR_RULE
    if isinstance(root, ServiceIdentityFallbackDisabled):
        rule = _UNAUTHORIZED_RULE
    elif isinstance(root, (PermissionError, AuthorizationError)):
        rule = _PERMISSION_DENIED_RULE
    elif isinstance(root, (ValueError, TypeError, FastMCPValidationError)):
        rule = _INVALID_PARAMS_RULE
    elif isinstance(
        root, (FileNotFoundError, KeyError, NotFoundError, DisabledError)
    ):
        rule = _NOT_FOUND_RULE
    elif isinstance(
        root, (TimeoutError, asyncio.TimeoutError, httpx.TimeoutException)
    ):
        rule = _TIMEOUT_RULE
    elif isinstance(root, httpx.RequestError):
        rule = _UNAVAILABLE_RULE

    return _from_rule(
        rule, error_type=root_type, fallback_type=root_type, method=method
    )


class GravitinoErrorHandlingMiddleware(ErrorHandlingMiddleware):
    """ErrorHandlingMiddleware that maps Gravitino REST and HTTP errors cleanly.

    FastMCP's stock ErrorHandlingMiddleware only recognizes a handful of Python
    built-ins (by exact type match) and maps every other exception — including
    Gravitino 401/403 authorization denials, 400 invalid requests, 404 not-found
    responses, 409 conflicts, and pydantic ValidationError — to JSON-RPC -32603
    ("Internal error"). This subclass uses ``classify_error`` so clients receive
    accurate error codes and prefixes while keeping Gravitino's actionable text.
    """

    def _transform_error(
        self, error: Exception, context: MiddlewareContext
    ) -> Exception:
        if isinstance(error, McpError):
            return error

        if not self.transform_errors:
            return error

        classification = classify_error(error, method=context.method or "")
        return McpError(
            ErrorData(
                code=classification.mcp_code,
                message=f"{classification.prefix}: {error!s}",
            )
        )
