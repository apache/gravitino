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

from fastmcp import FastMCP
from fastmcp.server.middleware.error_handling import ErrorHandlingMiddleware
from fastmcp.server.middleware.logging import (
    LoggingMiddleware,
)
from fastmcp.server.middleware.timing import TimingMiddleware

from mcp_server.core.context import GravitinoContext
from mcp_server.core.setting import Setting
from mcp_server.tools import load_tools


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


def _parse_mcp_url(url: str) -> ():
    try:
        parsed = urlparse(url)
        if parsed.scheme.lower() != "http":
            raise ValueError(f"Not support: {parsed.scheme}，only support HTTP")

        host = parsed.hostname or "0.0.0.0"

        port = parsed.port
        if port is None:
            port = 80

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
        asyncio.run(
            self.mcp.run_async(
                transport="http", host=_host, port=_port, path=_path
            )
        )
