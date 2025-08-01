import logging

import httpx
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

    def _get_table_by_fqn_response(session: httpx.Client, fully_qualified_name: str):
        table_names = fully_qualified_name.split(".")
        # metalake=table_names[0]
        catalog_name = table_names[0]
        schema_name = table_names[1]
        table_name = table_names[2]
        response = session.get(
            f"/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}"
        )
        response.raise_for_status()
        response_json = response.json()
        logging.info(f"response: {response_json}")
        return response_json

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
