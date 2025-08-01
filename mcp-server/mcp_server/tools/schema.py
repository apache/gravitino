from fastmcp import Context, FastMCP


def load_schema_tools(mcp: FastMCP):
    @mcp.tool(
        name="get_list_of_schemas",
        description="Get a list of schemas, filtered by catalog it belongs to.",
    )
    def get_list_of_schemas(ctx: Context, catalog_name: str):
        """
        Get a list of schemas, filtered by catalog it belongs to.

        Args:
            catalog_name (str):  name of the catalog
        """
        connector = ctx.request_context.lifespan_context.connector()
        return connector.as_schema_operation().get_list_of_schemas(catalog_name)
