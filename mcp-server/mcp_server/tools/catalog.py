from fastmcp import Context, FastMCP


def load_catalog_tools(mcp: FastMCP):
    @mcp.tool(
        name="get_list_of_catalogs",
        description="Get a list of catalogs.",
    )
    def get_list_of_catalogs(ctx: Context):
        """
        Get a list of catalogs.
        """
        connector = ctx.request_context.lifespan_context.connector()
        return connector.as_catalog_operation().get_list_of_catalogs()
