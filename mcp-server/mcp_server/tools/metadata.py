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

import json

from fastmcp import Context, FastMCP


async def _find_table_metadata(
    client, name: str, case_sensitive: bool
) -> dict:
    """Locate tables by name across relational catalogs and schemas.

    Composes the existing catalog, schema, and table listing operations into
    a single server-side crawl. A catalog or schema that cannot be listed is
    recorded under "skipped" so one bad node does not abort the search.
    """
    if not name or not name.strip():
        raise ValueError("name must be a non-empty string")
    needle = name if case_sensitive else name.casefold()

    def _matches(candidate: str) -> bool:
        hay = candidate if case_sensitive else candidate.casefold()
        return needle in hay

    matches = []
    skipped = []
    searched_catalogs = 0
    searched_schemas = 0

    catalogs = json.loads(
        await client.as_catalog_operation().get_list_of_catalogs()
    )
    for catalog in catalogs:
        if not isinstance(catalog, dict):
            continue
        if str(catalog.get("type", "")).casefold() != "relational":
            continue
        catalog_name = catalog.get("name", "")
        searched_catalogs += 1
        try:
            schemas = json.loads(
                await client.as_schema_operation().get_list_of_schemas(
                    catalog_name
                )
            )
        except Exception as exc:  # pylint: disable=broad-except
            skipped.append({"location": catalog_name, "reason": str(exc)})
            continue
        for schema in schemas:
            schema_name = (
                schema.get("name", "")
                if isinstance(schema, dict)
                else str(schema)
            )
            searched_schemas += 1
            try:
                tables = json.loads(
                    await client.as_table_operation().get_list_of_tables(
                        catalog_name, schema_name
                    )
                )
            except Exception as exc:  # pylint: disable=broad-except
                skipped.append(
                    {
                        "location": f"{catalog_name}.{schema_name}",
                        "reason": str(exc),
                    }
                )
                continue
            for table in tables:
                table_name = (
                    table.get("name", "")
                    if isinstance(table, dict)
                    else str(table)
                )
                if _matches(table_name):
                    matches.append(
                        {
                            "catalog": catalog_name,
                            "schema": schema_name,
                            "table": table_name,
                            "fullName": (
                                f"{catalog_name}.{schema_name}.{table_name}"
                            ),
                            "catalogType": catalog.get("type", ""),
                        }
                    )
    return {
        "matches": matches,
        "searched": {
            "catalogs": searched_catalogs,
            "schemas": searched_schemas,
        },
        "skipped": skipped,
    }


def load_metadata_tool(mcp: FastMCP):
    @mcp.tool(tags={"tag", "policy"})
    async def metadata_type_to_fullname_formats(ctx: Context) -> dict:
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

    @mcp.tool(tags={"table"})
    async def find_metadata(
        ctx: Context, name: str, case_sensitive: bool = False
    ) -> str:
        """
        Locate table metadata by name across every relational catalog and
        schema in the metalake, in a single call.

        Use this to answer "where does table <name> live" or "which catalogs
        contain a table called <name>" without knowing the catalog or schema
        in advance. Prefer it over listing catalogs and schemas by hand, and
        prefer it over discovering table locations through a SQL query engine.
        The tool returns metadata only and never returns table rows.

        The lookup crawls the metalake server-side: it lists relational
        catalogs, then schemas within each, then tables within each schema,
        and returns every table whose name matches. Matching is a
        case-insensitive substring match by default. A catalog or schema that
        cannot be listed, for example a schema reported as managed by multiple
        catalogs, is skipped and recorded under "skipped" rather than aborting
        the search.

        Args:
            ctx (Context): The request context object.
            name (str): The table name or name fragment to search for.
            case_sensitive (bool): Match the name by case when True. Defaults
                to False (case-insensitive).

        Returns:
            str: A JSON string with the following structure:
                {
                    "matches": [
                        {
                            "catalog": "catalog_postgres",
                            "schema": "hr",
                            "table": "employees",
                            "fullName": "catalog_postgres.hr.employees",
                            "catalogType": "relational"
                        }
                    ],
                    "searched": {"catalogs": 3, "schemas": 7},
                    "skipped": [
                        {
                            "location": "catalog_hive.sales",
                            "reason": "Schema managed by multiple catalogs"
                        }
                    ]
                }

        Special Considerations:
            - Only relational catalogs are searched. Filesets, topics, and
              models are out of scope for this tool.
            - On a large metalake the crawl issues one listing call per catalog
              and per schema, so it is heavier than a direct load when the
              catalog and schema are already known. In that case use
              get_list_of_tables or get_table_metadata_details instead.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return json.dumps(
            await _find_table_metadata(client, name, case_sensitive)
        )
