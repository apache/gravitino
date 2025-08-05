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
                        "name": "catalog-name",            # A unique name for the catalog
                        "type": "catalog-type",             # Type of catalog (e.g., "relational", "fileset", "message", "model")
                        "provider": "provider-name",       # The catalog provider. One catalog type may have different providers, for "relational" catalog type, the provider may be "jdbc-postgresql", "jdbc-mysql", "lakehouse-iceberg", etc.
                        "comment": "description-text",      # Human-readable description
                        "properties": {                     # Configuration properties, the property key may differ for different catalog types and providers
                            "key1": "value1",
                            "key2": "value2",
                            ...
                        },
                        "audit": {                         # Audit metadata
                            "creator": "creator-name",
                            "createTime": "ISO-8601-timestamp",
                            "lastModifier": "modifier-name",
                            "lastModifiedTime": "ISO-8601-timestamp"
                        }
                    },
                    ...
                ]

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
        connector = ctx.request_context.lifespan_context.connector()
        return await connector.as_catalog_operation().get_list_of_catalogs()
