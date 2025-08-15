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


def load_table_tools(mcp: FastMCP):
    @mcp.tool(tags={"table"})
    async def get_list_of_tables(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
    ):
        """
        Retrieve a list of tables within a specific catalog and schema.

        This function returns a JSON-formatted string containing table identifiers
        filtered by the specified catalog and schema names.

        Parameters:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            catalog_name (str): The name of the catalog to filter tables by.
            schema_name (str): The name of the schema to filter tables by.

        Returns:
            str: A JSON string representing an array of table objects with the following structure:
                [
                    {
                        "namespace": ["metalake_name", "catalog_name", "schema_name"],  # Namespace hierarchy
                        "name": "table_name"                            # Table name
                    },
                    ...
                ]

        Example Return Value:
            [
              {
                "namespace": ["test", "pg", "public"],
                "name": "catalog_meta"
              },
              {
                "namespace": ["test", "pg", "public"],
                "name": "table_column_version_info"
              },
              {
                "namespace": ["test", "pg", "public"],
                "name": "table_meta"
              }
            ]

        Special Considerations:
            - Namespace elements represent a hierarchical path (e.g., ["test", "pg", "public"]
              correspond to test.pg.public in dot notation)
            - The namespace array typically contains three elements: [metalake, catalog, schema]

        Filtering Behavior:
            - Only tables belonging to the specified catalog_name and schema_name are returned
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_table_operation().get_list_of_tables(
            catalog_name, schema_name
        )

    @mcp.tool(tags={"table"})
    async def get_table_metadata_details(
        ctx: Context, catalog_name: str, schema_name: str, table_name: str
    ):
        """
        Retrieve comprehensive metadata details for a specific table.

        This function returns a JSON-formatted string containing detailed metadata
        about the specified table, including its schema, properties, partitioning,
        distribution, index, and other technical details.

        Parameters:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            catalog_name (str): The name of the catalog containing the table.
            schema_name (str): The name of the schema containing the table.
            table_name (str): The name of the table to retrieve metadata for.

        Returns:
            str: A JSON string representing a table metadata object with the following structure:
                {
                    "name": "table-name",                  # Table name
                    "comment": "table-description",        # Human-readable description
                    "columns": [                           # List of column definitions
                        {
                            "name": "column-name",         # Column identifier
                            "type": "data-type",           # Column data type
                            "comment": "column-description", # Column description
                            "nullable": true/false,        # Nullability constraint
                            "autoIncrement": true/false    # Auto-increment status
                        },
                        ...
                    ],
                    "properties": {
                        "key1": "value1",
                        "key2": "value2",
                        ...
                    },
                    "audit": {                             # Audit metadata
                        "creator": "creator-name",
                        "createTime": "ISO-8601-timestamp"
                    },
                    "distribution": {                      # Data distribution strategy
                        "strategy": "distribution-type",
                        "number": distribution-factor,
                        "funcArgs": [function-arguments]
                    },
                    "sortOrders": [                        # Sorting specifications
                        {
                            "sortTerm": {
                                "type": "field/expression",
                                "fieldName": ["field", "path"]
                            },
                            "direction": "asc/desc",
                            "nullOrdering": "nulls_first/nulls_last"
                        },
                        ...
                    ],
                    "partitioning": [                       # Partitioning strategy
                        {
                            "strategy": "partition-type",
                            "fieldName": ["partition", "field"]
                        },
                        ...
                    ],
                    "indexes": [                            # Table indexes
                        {
                            # Index definition details
                        },
                        ...
                    ]
                }
                name: The unique name of the table.
                comment: A human-readable description of the table.
                columns: A list of column definitions, each containing:
                        - name: The name of the column.
                        - type: The data type of the column (e.g., string, integer).
                        - comment: A description of the column.
                        - nullable: Indicates whether the column can contain null values.
                        - autoIncrement: Indicates whether the column is auto-incrementing.
                properties: A dictionary of table-specific properties, which may vary based on the catalog provider.
                audit: Metadata about the table's creation, including:
                        - creator: The name of the user who created the table.
                        - createTime: The timestamp when the table was created, in ISO-8601 format.
                distribution: Information about how the data is distributed across files/parts, including:
                        - strategy: The distribution strategy used (e.g., hash, range).
                        - number: The number of buckets or parts created.
                sortOrders: A list of sorting specifications for the table, each containing:
                        - sortTerm: The term used for sorting, which can be a field or an expression.
                        - direction: The sorting direction (ascending or descending).
                        - nullOrdering: How null values are ordered (nulls first or last).

                partitioning: A list of partitioning strategies used for the table, each containing:
                        - strategy: The partitioning strategy used (e.g., identity, bucket[N], truncate[W], list,
                                    range, func).
                        - fieldName: The field(s) used for partitioning, specified as a list of strings.
                indexes: A list of table indexes, such as primary keys or unique keys.

        Example Return Value:
            {
              "name": "test2",
              "comment": "This is an example table",
              "columns": [
                {
                  "name": "id",
                  "type": "string",
                  "comment": "id column comment",
                  "nullable": true,
                  "autoIncrement": false
                },
                {
                  "name": "dt",
                  "type": "string",
                  "comment": "datetime",
                  "nullable": true,
                  "autoIncrement": false
                },
                {
                  "name": "name",
                  "type": "string",
                  "comment": "name column comment",
                  "nullable": true,
                  "autoIncrement": false
                }
              ],
              "properties": {
                "sort-order": "name ASC NULLS FIRST",
                "current-snapshot-id": "none",
                "provider": "iceberg",
                "write.parquet.compression-codec": "zstd",
                "format": "iceberg/parquet",
                "format-version": "2",
                "location": "file:///tmp/jdbc/db/test2",
                "write.distribution-mode": "range"
              },
              "audit": {
                "creator": "anonymous",
                "createTime": "2025-08-03T11:36:04.856145Z"
              },
              "distribution": {
                "strategy": "range",
                "number": 0,
                "funcArgs": []
              },
              "sortOrders": [
                {
                  "sortTerm": {
                    "type": "field",
                    "fieldName": ["name"]
                  },
                  "direction": "asc",
                  "nullOrdering": "nulls_first"
                }
              ],
              "partitioning": [
                {
                  "strategy": "identity",
                  "fieldName": ["dt"]
                }
              ],
              "indexes": []
            }

        Special Considerations:
            - The catalog type which containing the table must be "relational"
            - The table in different catalog provider may have different properties
            - "distribution" a.k.a (Clustering) is a technique to split the data
                into more manageable files/parts, (By specifying the number of
                buckets to create). The value of the distribution column will be
                hashed by a user-defined number into buckets. Supporting "hash",
                "range", "even", etc distribution strategies.
            - "partitioning" is a partitioning strategy that is used to split a
                table into parts based on partition keys. Supporting diverse
                partitioning strategies like "identity", "bucket[N]", "truncate[W]",
                 "list", "range", "func", etc.
            - "indexes" represents the table index, such as primary key or unique key.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_table_operation().load_table(
            catalog_name, schema_name, table_name
        )
