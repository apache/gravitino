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


def load_schema_tools(mcp: FastMCP):
    @mcp.tool(tags={"schema"})
    async def get_list_of_schemas(ctx: Context, catalog_name: str) -> str:
        """
        Retrieve a list of schemas belonging to a specific catalog.

        This function returns a JSON-formatted string containing schema information
        filtered by the specified catalog name.

        Parameters:
            ctx (Context): The context object containing Gravitino context.
            catalog_name (str): The name of the catalog to filter schemas by.

        Returns:
            str: A JSON string representing an array of schema objects with the following structure:
                [
                    {
                        "namespace": ["metalake_name", "catalog_name"],  # Namespace hierarchy
                        "name": "schema_name"                            # Schema identifier
                    },
                    ...
                ]

        Example Return Value:
            [
              {
                "namespace": ["test", "iceberg"],
                "name": "db1"
              },
              {
                "namespace": ["test", "iceberg"],
                "name": "db2"
              }
            ]

        Special Considerations:
            - Namespace elements represent a hierarchical path (e.g., ["test", "iceberg"]
              correspond to test.iceberg in dot notation)
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_schema_operation().get_list_of_schemas(
            catalog_name
        )
