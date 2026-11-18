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


def load_partition_tools(mcp: FastMCP):
    @mcp.tool(tags={"partition"})
    async def list_of_partitions(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        details: bool = False,
    ):
        """
        Retrieve the partitions of a specific table.

        Only catalogs that support the partition API (for example Hive) expose
        partitions. Catalogs that use hidden partitioning, such as
        lakehouse-iceberg, do not support this operation and return an
        "unsupported operation" error; for those catalogs the partitioning is
        described by the table's partition spec, available via
        'get_table_metadata_details'. Use the returned partition names as input
        to partition-scoped tools such as 'list_statistics_for_partition' and
        'get_partition'.

        Parameters:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            catalog_name (str): The name of the catalog containing the table.
            schema_name (str): The name of the schema containing the table.
            table_name (str): The name of the table to list partitions for.
            details (bool): When False (default), return partition names only.
                When True, return the full metadata of every partition.

        Returns:
            str: When details is False, a JSON string containing an array of
                partition names:
                    ["dt=2025-01-01", "dt=2025-01-02"]
                When details is True, a JSON string containing an array of
                partition objects, each with the following structure:
                    [
                      {
                        "type": "identity",
                        "name": "dt=2025-01-01",
                        "fieldNames": [["dt"]],
                        "values": [
                          {"type": "literal", "dataType": "date", "value": "2025-01-01"}
                        ],
                        "properties": {}
                      }
                    ]

                type: The partition type (identity, range, or list).
                name: The unique name of the partition.
                properties: A dictionary of partition-specific properties.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_partition_operation().list_of_partitions(
            catalog_name, schema_name, table_name, details
        )

    @mcp.tool(tags={"partition"})
    async def get_partition(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        partition_name: str,
    ):
        """
        Load detailed information of a specific partition.

        Only catalogs that support the partition API (for example Hive) support
        this operation; catalogs with hidden partitioning such as
        lakehouse-iceberg return an "unsupported operation" error. Partition
        names can be discovered with the 'list_of_partitions' tool.

        Parameters:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            catalog_name (str): The name of the catalog containing the table.
            schema_name (str): The name of the schema containing the table.
            table_name (str): The name of the table containing the partition.
            partition_name (str): The name of the partition to load.

        Returns:
            str: A JSON string containing full partition metadata.

        Example Return Value:
            {
              "type": "identity",
              "name": "dt=2025-01-01",
              "fieldNames": [["dt"]],
              "values": [
                {"type": "literal", "dataType": "date", "value": "2025-01-01"}
              ],
              "properties": {}
            }
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_partition_operation().get_partition(
            catalog_name, schema_name, table_name, partition_name
        )
