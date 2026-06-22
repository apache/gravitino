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
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator
from urllib.parse import urlparse

import mcp.types as mt
from fastmcp import FastMCP
from fastmcp.server.middleware.error_handling import ErrorHandlingMiddleware
from fastmcp.server.middleware.logging import (
    LoggingMiddleware,
)
from fastmcp.server.middleware.middleware import (
    CallNext,
    Middleware,
    MiddlewareContext,
)
from fastmcp.server.middleware.timing import TimingMiddleware
from fastmcp.tools.base import ToolResult

from mcp_server.core import audit
from mcp_server.core.context import (
    GravitinoContext,
    _get_request_authorization,
    startup_authorization,
)
from mcp_server.core.setting import Setting
from mcp_server.tools import load_tools


def _get_principal_from_request(fallback_authorization: str = "") -> str:
    """Derive a display principal for audit attribution.

    Uses the incoming HTTP request's Authorization header when present;
    otherwise falls back to the static startup identity (``--token``), which is
    what actually authenticates the call in stdio mode or in HTTP requests that
    carry no Authorization header. Returns "anonymous" when neither is set.
    """
    authorization = _get_request_authorization() or fallback_authorization
    # pylint: disable=protected-access
    return audit._extract_principal(authorization)


class AuditMiddleware(Middleware):
    """Emit a structured audit record for every tool invocation."""

    def __init__(self, fallback_authorization: str = ""):
        super().__init__()
        self._fallback_authorization = fallback_authorization

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        tool_name = context.message.name if context.message else "unknown"
        principal = _get_principal_from_request(self._fallback_authorization)
        try:
            result = await call_next(context)
            audit.emit(principal=principal, tool=tool_name, outcome="allow")
            return result
        except Exception as exc:
            audit.emit(
                principal=principal,
                tool=tool_name,
                outcome="deny",
                error_type=type(exc).__name__,
            )
            raise


def _create_lifespan_manager(gravitino_context: GravitinoContext):

    @asynccontextmanager
    async def app_lifespan(server: FastMCP) -> AsyncIterator[GravitinoContext]:
        logging.info("Add Gravitino context: %s", gravitino_context)
        yield gravitino_context

    return app_lifespan


def _create_gravitino_mcp(setting: Setting) -> FastMCP:
    if setting.tags is not None and len(setting.tags) > 0:
        mcp = FastMCP(
            "Gravitino MCP Server",
            lifespan=_create_lifespan_manager(GravitinoContext(setting)),
            include_tags=setting.tags,
        )
    else:
        mcp = FastMCP(
            "Gravitino MCP Server",
            lifespan=_create_lifespan_manager(GravitinoContext(setting)),
        )

    mcp.add_middleware(AuditMiddleware(startup_authorization(setting)))
    mcp.add_middleware(
        LoggingMiddleware(include_payloads=True, max_payload_length=1000)
    )
    mcp.add_middleware(TimingMiddleware())
    mcp.add_middleware(
        ErrorHandlingMiddleware(
            include_traceback=True,
            transform_errors=True,
        )
    )
    return mcp


def _parse_mcp_url(url: str) -> tuple[str, int, str]:
    try:
        parsed = urlparse(url)
        scheme = parsed.scheme.lower()
        if scheme not in ("http", "https"):
            raise ValueError(
                f"Not supported: {parsed.scheme}, only http/https are supported"
            )

        host = parsed.hostname or "0.0.0.0"

        port = parsed.port
        if port is None:
            port = 443 if scheme == "https" else 80

        path = parsed.path
        if not path.startswith("/"):
            path = "/" + path

        return host, port, path

    except Exception as e:
        raise ValueError(f"Invalid URL: {url}") from e


class GravitinoMCPServer:
    def __init__(self, setting: Setting):
        self.setting = setting
        self.mcp = _create_gravitino_mcp(setting)
        load_tools(self.mcp)

    def run(self):
        if self.setting.transport == "stdio":
            self._run_stdio()
        else:
            self._run_http()

    def _run_stdio(self):
        asyncio.run(self.mcp.run_async(transport="stdio"))

    def _run_http(self):
        _host, _port, _path = _parse_mcp_url(self.setting.mcp_url)
        self._validate_tls_config()
        # FastMCP accepts "http" and "streamable-http" as equivalent aliases.
        transport = (
            "streamable-http"
            if self.setting.transport == "streamable-http"
            else "http"
        )

        run_kwargs = {
            "transport": transport,
            "host": _host,
            "port": _port,
            "path": _path,
        }

        # Serve over TLS when both certificate and key are provided. FastMCP
        # forwards uvicorn_config to the underlying uvicorn server.
        if self.setting.tls_cert and self.setting.tls_key:
            run_kwargs["uvicorn_config"] = {
                "ssl_certfile": self.setting.tls_cert,
                "ssl_keyfile": self.setting.tls_key,
            }
            logging.info(
                "Serving MCP endpoint over TLS (cert=%s)", self.setting.tls_cert
            )

        asyncio.run(self.mcp.run_async(**run_kwargs))

    def _validate_tls_config(self):
        """Reject inconsistent TLS configuration before starting the server.

        Guards against serving plain HTTP on an ``https://`` URL (or TLS behind
        an ``http://`` URL), and against providing only one of cert/key.
        """
        cert, key = self.setting.tls_cert, self.setting.tls_key
        if bool(cert) != bool(key):
            raise ValueError(
                "Both --tls-cert and --tls-key must be provided together "
                "(or neither)."
            )
        scheme = urlparse(self.setting.mcp_url).scheme.lower()
        tls_enabled = bool(cert and key)
        if scheme == "https" and not tls_enabled:
            raise ValueError(
                f"mcp_url '{self.setting.mcp_url}' uses https but TLS is not "
                "configured; provide --tls-cert and --tls-key."
            )
        if scheme == "http" and tls_enabled:
            raise ValueError(
                "--tls-cert/--tls-key are set but mcp_url "
                f"'{self.setting.mcp_url}' uses http; use an https URL."
            )
