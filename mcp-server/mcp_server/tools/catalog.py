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


def load_catalog_tools(mcp: FastMCP):
    @mcp.tool(tags={"catalog"})
    async def get_list_of_catalogs(ctx: Context) -> str:
        """
        Retrieve a list of all catalogs in the system.

        This function executes a catalog operation returns
        a JSON-formatted string containing detailed information about all available catalogs.

        Parameters:
            ctx (Context): The context object containing Gravitino context.

        Returns:
            str: A JSON string representing an array of catalog objects with the following structure:
                [
                    {
                        "name": "catalog-name",
                        "type": "catalog-type",
                        "provider": "provider-name",
                        "comment": "description-text",
                        "properties": {
                            "key1": "value1",
                            "key2": "value2",
                            ...
                        },
                        "audit": {
                            "creator": "creator-name",
                            "createTime": "ISO-8601-timestamp",
                            "lastModifier": "modifier-name",
                            "lastModifiedTime": "ISO-8601-timestamp"
                        }
                    },
                    ...
                ]

                name: The unique name of the catalog.
                type: The type of the catalog, such as "relational", "fileset", "message", or "model".
                provider: The specific provider of the catalog, which can vary based on the catalog type.
                comment: A human-readable description of the catalog.
                properties: A dictionary of configuration properties for the catalog,
                            which may vary based on the catalog type and provider.

        Example Return Value:
            [
              {
                "name": "iceberg",
                "type": "relational",
                "provider": "lakehouse-iceberg",
                "comment": "iceberg pg catalog",
                "properties": {
                  "catalog-backend": "jdbc",
                  "jdbc-user": "postgres",
                  "jdbc-password": "abc123",
                  "jdbc-driver": "org.postgresql.Driver",
                  "jdbc-initialize": "true",
                  "warehouse": "file:///tmp/jdbc",
                  "uri": "jdbc:postgresql://127.0.0.1:5432/postgres",
                  "in-use": "true"
                },
                "audit": {
                  "creator": "anonymous",
                  "createTime": "2025-07-28T09:06:35.649088Z",
                  "lastModifier": "anonymous",
                  "lastModifiedTime": "2025-07-28T09:06:35.649088Z"
                }
              },
              {
                "name": "pg",
                "type": "relational",
                "provider": "jdbc-postgresql",
                "comment": "comment",
                "properties": {
                  "jdbc-url": "jdbc:postgresql://localhost:5432/postgres",
                  "jdbc-user": "postgres",
                  "jdbc-driver": "org.postgresql.Driver",
                  "jdbc-database": "postgres",
                  "jdbc-password": "abc123",
                  "in-use": "true"
                },
                "audit": {
                  "creator": "anonymous",
                  "createTime": "2025-07-28T09:06:49.547400Z",
                  "lastModifier": "anonymous",
                  "lastModifiedTime": "2025-07-28T09:06:49.547400Z"
                }
              }
            ]
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_catalog_operation().get_list_of_catalogs()

    @mcp.tool(tags={"catalog"})
    # pylint: disable=too-many-positional-arguments
    async def create_catalog(
        ctx: Context,
        name: str,
        catalog_type: str,
        provider: str,
        comment: str,
        properties: dict,
    ) -> str:
        """
        Create a new catalog within the metalake.

        Authorization is enforced by Gravitino: a principal without the
        required grant receives an authorization denial.

        Args:
            ctx (Context): The request context object.
            name (str): Name of the catalog to create.
            catalog_type (str): Catalog type, one of "relational", "fileset",
                "messaging", "model".
            provider (str): Provider implementation, e.g. "hive",
                "lakehouse-iceberg", "jdbc-postgresql", "kafka". May be empty
                for model/fileset catalogs that have a single provider.
            comment (str): Human-readable description.
            properties (dict): Catalog configuration properties.

        Returns:
            str: JSON-formatted string containing the created catalog.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_catalog_operation().create_catalog(
            name, catalog_type, provider, comment, properties
        )

    @mcp.tool(tags={"catalog"})
    async def alter_catalog(
        ctx: Context, catalog_name: str, updates: list
    ) -> str:
        """
        Alter an existing catalog.

        Args:
            ctx (Context): The request context object.
            catalog_name (str): Name of the catalog to alter.
            updates (list): List of update operations. Example:
                [
                  {"@type": "rename", "newName": "new_catalog"},
                  {"@type": "updateComment", "newComment": "updated"},
                  {"@type": "setProperty", "property": "k", "value": "v"},
                  {"@type": "removeProperty", "property": "k"}
                ]

        Returns:
            str: JSON-formatted string containing the altered catalog.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_catalog_operation().alter_catalog(
            catalog_name, updates
        )

    @mcp.tool(tags={"catalog"})
    async def drop_catalog(ctx: Context, catalog_name: str) -> str:
        """
        Drop a catalog by its name.

        Args:
            ctx (Context): The request context object.
            catalog_name (str): Name of the catalog to drop.

        Returns:
            str: JSON-formatted string indicating whether the catalog was dropped.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_catalog_operation().drop_catalog(catalog_name)
