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

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastmcp import FastMCP
from fastmcp.server.middleware.error_handling import ErrorHandlingMiddleware
from fastmcp.server.middleware.logging import (
    LoggingMiddleware,
    StructuredLoggingMiddleware,
)
from fastmcp.server.middleware.timing import TimingMiddleware

from mcp_server.core.context import GravitinoContext
from mcp_server.core.setting import Setting
from mcp_server.tools import load_tools
import asyncio


def create_lifespan_manager(gravitino_context: GravitinoContext):

    @asynccontextmanager
    async def app_lifespan(server: FastMCP) -> AsyncIterator[GravitinoContext]:
        logging.info(f"Add Gravitino context: {gravitino_context}")
        yield gravitino_context

    return app_lifespan


def create_gravition_mcp(setting: Setting) -> FastMCP:
    mcp = FastMCP(
        "Gravitino MCP Server",
        lifespan=create_lifespan_manager(GravitinoContext(setting)),
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


class GravitinoMCPServer:
    def __init__(self, setting: Setting):
        self.setting = setting
        self.mcp = create_gravition_mcp(setting)
        load_tools(self.mcp)

    def run(self):
        asyncio.run(self.mcp.run_async())