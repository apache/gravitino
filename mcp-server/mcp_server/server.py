import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from mcp_server.core.context import GravitinoContext
from mcp_server.core.setting import Setting
from fastmcp import FastMCP
from fastmcp.server.middleware.error_handling import ErrorHandlingMiddleware
from fastmcp.server.middleware.logging import (LoggingMiddleware,
                                               StructuredLoggingMiddleware)
from fastmcp.server.middleware.timing import TimingMiddleware
from mcp_server.tools import load_tools


def create_lifespan_manager(gravitino_context: GravitinoContext):
    @asynccontextmanager
    async def app_lifespan(server: FastMCP) -> AsyncIterator[GravitinoContext]:
        logging.info(f"Add Gravitino context: {gravitino_context}")
        yield gravitino_context
        # try:
        #     yield gravitino_context
        # finally:
        #     logging.info("Close Gravitino context")

    return app_lifespan


def create_gravition_mcp(setting: Setting) -> FastMCP:
    mcp = FastMCP(
        "Gravitino MCP Server",
        lifespan=create_lifespan_manager(GravitinoContext(setting)),
    )

    mcp.add_middleware(
        LoggingMiddleware(include_payloads=True, max_payload_length=1000)
    )

    mcp.add_middleware(StructuredLoggingMiddleware(include_payloads=True))
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

    def run(self):
        load_tools(self.mcp)
        self.mcp.run()
