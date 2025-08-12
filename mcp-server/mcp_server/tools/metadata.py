
from fastmcp import Context, FastMCP


def load_metadata_tool(mcp: FastMCP):
    @mcp.tool(tags={"metadata"})
    async def metadata_type_to_fullname_formats(
        ctx: Context) -> dict:

        """
        Get metadata type to full name formats.
        This tool provides the fullname format strings for different metadata types
        such as catalog, schema, table, model, topic, fileset, and column.

        Args:
            ctx (Context): Context object containing request metadata.
        Returns:
            dict: Dictionary containing formats for different metadata types.
            Key is the metadata type and value is the fullname format string.
        """

        return {
            "catalog": "{catalog}",
            "schema": "{catalog}.{schema}",
            "table": "{catalog}.{schema}.{table}",
            "model": "{catalog}.{schema}.{model}",
            "topic": "{catalog}.{schema}.{topic}",
            "fileset": "{catalog}.{schema}.{fileset}",
            "column": "{catalog}.{schema}.{table}.{column}",
        }