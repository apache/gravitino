from fastmcp import Context, FastMCP

def load_table_tools(mcp: FastMCP):
    @mcp.tool(
        name="get_list_of_tables",
        description="Get a list of tables, filtered by catalog and schema it belongs to.",
    )
    def get_list_of_tables(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
    ):
        """
        Get a list of tables, optionally filtered by database it belongs to.
        """
        connector = ctx.request_context.lifespan_context.connector()
        return connector.as_table_operation().get_list_of_tables(
            catalog_name, schema_name
        )

    @mcp.tool(
        name="get_table_detail_by_name",
        description="Get table detail information by catalog_name, schema_name and table_name.",
    )
    def get_table_metadata_details(
        ctx: Context, catalog_name: str, schema_name: str, table_name: str
    ):
        """
        Get table detail information by catalog_name, schema_name and table_name.
        Args:
            catalog_name (str):  name of the catalog
            schema_name (str):  name of the schema
            table_name (str):  name of the table
        Returns:
            str: A json str include table details including columns, comment, partitioning, distribution, sort order,etc
        """
        connector = ctx.request_context.lifespan_context.connector()
        return connector.as_table_operation().load_table(
            catalog_name, schema_name, table_name
        )
