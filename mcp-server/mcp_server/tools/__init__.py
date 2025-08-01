from fastmcp import FastMCP
from tools.catalog import load_catalog_tools
from tools.schema import load_schema_tools
from tools.table import load_table_tools


def load_tools(mcp: FastMCP):
    load_catalog_tools(mcp)
    load_schema_tools(mcp)
    load_table_tools(mcp)
