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

from fastmcp import Context, FastMCP


def load_view_tools(mcp: FastMCP):
    @mcp.tool(tags={"view"})
    async def list_of_views(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
    ):
        """
        Retrieve a list of views within a specific catalog and schema.

        Views are supported by relational catalogs such as Hive and
        lakehouse-iceberg.

        Parameters:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            catalog_name (str): The name of the catalog to filter views by.
            schema_name (str): The name of the schema to filter views by.

        Returns:
            str: A JSON string representing an array of view objects with the
                following structure:
                - namespace: The hierarchical namespace of the view
                             ([metalake, catalog, schema]).
                - name: The name of the view.

            Example Return Value:
                [
                  {
                    "namespace": ["test", "iceberg_catalog", "default"],
                    "name": "daily_summary"
                  }
                ]
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_view_operation().list_of_views(
            catalog_name, schema_name
        )

    @mcp.tool(tags={"view"})
    async def load_view(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        view_name: str,
    ):
        """
        Load detailed information of a specific view.

        Views are supported by relational catalogs such as Hive and
        lakehouse-iceberg.

        Parameters:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            catalog_name (str): The name of the catalog containing the view.
            schema_name (str): The name of the schema containing the view.
            view_name (str): The name of the view to load.

        Returns:
            str: A JSON string containing full view metadata with the following
                structure:
                {
                  "name": "view-name",              # View name
                  "comment": "description",         # Human-readable description
                  "columns": [                      # Output column definitions
                    {
                      "name": "column-name",
                      "type": "data-type",
                      "comment": "column-description",
                      "nullable": true
                    }
                  ],
                  "representations": [              # Engine-specific definitions
                    {
                      "type": "sql",
                      "dialect": "spark",
                      "sql": "SELECT ..."
                    }
                  ],
                  "defaultCatalog": "catalog-name", # Default catalog for name
                                                    # resolution (may be null)
                  "defaultSchema": "schema-name",   # Default schema for name
                                                    # resolution (may be null)
                  "properties": {"key": "value"},   # View properties
                  "audit": {
                    "creator": "creator-name",
                    "createTime": "ISO-8601-timestamp"
                  }
                }

        Example Return Value:
            {
              "name": "daily_summary",
              "comment": "Daily aggregated metrics",
              "columns": [
                {
                  "name": "dt",
                  "type": "date",
                  "comment": "partition date",
                  "nullable": true
                }
              ],
              "representations": [
                {
                  "type": "sql",
                  "dialect": "spark",
                  "sql": "SELECT dt FROM events GROUP BY dt"
                }
              ],
              "defaultCatalog": null,
              "defaultSchema": "default",
              "properties": {},
              "audit": {
                "creator": "anonymous",
                "createTime": "2025-08-03T11:36:04.856145Z"
              }
            }
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_view_operation().load_view(
            catalog_name, schema_name, view_name
        )

    @mcp.tool(tags={"view"})
    # pylint: disable=too-many-positional-arguments
    async def create_view(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        name: str,
        comment: str,
        columns: list,
        representations: list,
        properties: dict,
        default_catalog: str = None,
        default_schema: str = None,
    ) -> str:
        """
        Create a new view within a schema.

        Args:
            ctx (Context): The request context object.
            catalog_name (str): Name of the catalog.
            schema_name (str): Name of the schema.
            name (str): Name of the view to create.
            comment (str): Human-readable description.
            columns (list): Output column definitions. Each column is a dict:
                {
                  "name": "dt",
                  "type": "date",
                  "comment": "partition date",
                  "nullable": true
                }
            representations (list): Engine-specific definitions. At least one
                is required, and SQL dialects must not repeat. Each
                representation is a dict:
                {"type": "sql", "dialect": "spark", "sql": "SELECT ..."}
            properties (dict): View properties.
            default_catalog (str): Optional catalog used to resolve
                unqualified identifiers in the representations.
            default_schema (str): Optional schema used to resolve unqualified
                identifiers in the representations.

        Returns:
            str: JSON-formatted string containing the created view.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_view_operation().create_view(
            catalog_name,
            schema_name,
            name,
            comment,
            columns,
            representations,
            properties,
            default_catalog,
            default_schema,
        )

    @mcp.tool(tags={"view"})
    async def alter_view(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        view_name: str,
        updates: list,
    ) -> str:
        """
        Alter an existing view.

        Args:
            ctx (Context): The request context object.
            catalog_name (str): Name of the catalog.
            schema_name (str): Name of the schema.
            view_name (str): Name of the view to alter.
            updates (list): List of update operations. Example:
                [
                  {"@type": "rename", "newName": "renamed"},
                  {"@type": "setProperty", "property": "k", "value": "v"},
                  {"@type": "removeProperty", "property": "k"},
                  {"@type": "replaceView", "comment": "rewritten",
                   "columns": [], "representations": []}
                ]

        Returns:
            str: JSON-formatted string containing the altered view.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_view_operation().alter_view(
            catalog_name, schema_name, view_name, updates
        )

    @mcp.tool(tags={"view"})
    async def drop_view(
        ctx: Context, catalog_name: str, schema_name: str, view_name: str
    ) -> str:
        """
        Drop a view by its name.

        Args:
            ctx (Context): The request context object.
            catalog_name (str): Name of the catalog.
            schema_name (str): Name of the schema.
            view_name (str): Name of the view to drop.

        Returns:
            str: JSON-formatted string indicating whether the view was
                dropped.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_view_operation().drop_view(
            catalog_name, schema_name, view_name
        )
