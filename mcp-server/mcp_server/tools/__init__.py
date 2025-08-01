from fastmcp import FastMCP
from mcp_server.tools.catalog import load_catalog_tools
from mcp_server.tools.schema import load_schema_tools
from mcp_server.tools.table import load_table_tools


def load_tools(mcp: FastMCP):
    load_catalog_tools(mcp)
    load_schema_tools(mcp)
    load_table_tools(mcp)
