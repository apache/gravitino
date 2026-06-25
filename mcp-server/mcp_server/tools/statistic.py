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


def load_statistic_tools(mcp: FastMCP):
    # Disable the update_statistics tool by default as it is a write operation.
    @mcp.tool(tags={"statistic"})
    async def update_statistics(
        ctx: Context,
        metalake_name: str,
        metadata_type: str,
        metadata_fullname: str,
        statistics: dict,
    ) -> None:
        """
        Update (create or overwrite) custom statistics for a metadata object.
        Only custom (non-reserved) statistics can be updated. Currently this only
        supports statistics for tables, so `metadata_type` should always be "table"
        and `metadata_fullname` should be in the format "{catalog}.{schema}.{table}".

        Args:
            ctx (Context): The request context.
            metalake_name (str): The name of the metalake.
            metadata_type (str): The type of metadata (e.g., table).
            metadata_fullname (str): The full name of the metadata object.
            statistics (dict): Dictionary mapping statistic name to its value.

        Example input for statistics:
            {
              "custom-key1": "value1",
              "custom-key2": 100
            }

        Returns:
            None

        Raises:
            Exception: If the update fails, an exception is raised with an error message.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_statistic_operation().update_statistics(
            metalake_name, metadata_type, metadata_fullname, statistics
        )

    # Disable the drop_statistics tool by default as it can be destructive.
    @mcp.tool(tags={"statistic"})
    async def drop_statistics(
        ctx: Context,
        metalake_name: str,
        metadata_type: str,
        metadata_fullname: str,
        statistic_names: list,
    ) -> str:
        """
        Drop custom statistics from a metadata object. Currently this only supports
        statistics for tables, so `metadata_type` should always be "table" and
        `metadata_fullname` should be in the format "{catalog}.{schema}.{table}".

        Args:
            ctx (Context): The request context.
            metalake_name (str): The name of the metalake.
            metadata_type (str): The type of metadata (e.g., table).
            metadata_fullname (str): The full name of the metadata object.
            statistic_names (list): List of statistic names to drop.

        Example input for statistic_names:
            ["custom-key1", "custom-key2"]

        Returns:
            str: A JSON string indicating whether any statistic was dropped (true/false).
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_statistic_operation().drop_statistics(
            metalake_name, metadata_type, metadata_fullname, statistic_names
        )

    # pylint: disable=R0917
    # Disable the update_partition_statistics tool by default as it is a write operation.
    @mcp.tool(tags={"statistic"})
    async def update_partition_statistics(
        ctx: Context,
        metalake_name: str,
        metadata_type: str,
        metadata_fullname: str,
        partition_updates: list,
    ) -> None:
        """
        Update (create or overwrite) custom statistics for partitions of a table.
        Note: This is currently only supported for partitions of tables, so
        `metadata_type` should always be "table".

        Args:
            ctx (Context): The request context.
            metalake_name (str): The name of the metalake.
            metadata_type (str): The type of metadata, should be "table".
            metadata_fullname (str): The full name of the table, the format should be
                "{catalog}.{schema}.{table}".
            partition_updates (list): List of partition statistic updates.

        Example input for partition_updates:
            [
              {
                "partitionName": "p1",
                "statistics": {
                  "custom-key1": "value1"
                }
              }
            ]

        Returns:
            None

        Raises:
            Exception: If the update fails, an exception is raised with an error message.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return (
            await client.as_statistic_operation().update_partition_statistics(
                metalake_name,
                metadata_type,
                metadata_fullname,
                partition_updates,
            )
        )

    # pylint: disable=R0917
    # Disable the drop_partition_statistics tool by default as it can be destructive.
    @mcp.tool(tags={"statistic"})
    async def drop_partition_statistics(
        ctx: Context,
        metalake_name: str,
        metadata_type: str,
        metadata_fullname: str,
        partition_drops: list,
    ) -> str:
        """
        Drop custom statistics from partitions of a table.
        Note: This is currently only supported for partitions of tables, so
        `metadata_type` should always be "table".

        Args:
            ctx (Context): The request context.
            metalake_name (str): The name of the metalake.
            metadata_type (str): The type of metadata, should be "table".
            metadata_fullname (str): The full name of the table, the format should be
                "{catalog}.{schema}.{table}".
            partition_drops (list): List of partition statistic drops.

        Example input for partition_drops:
            [
              {
                "partitionName": "p1",
                "statisticNames": ["custom-key1"]
              }
            ]

        Returns:
            str: A JSON string indicating whether any statistic was dropped (true/false).
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_statistic_operation().drop_partition_statistics(
            metalake_name, metadata_type, metadata_fullname, partition_drops
        )

    @mcp.tool(tags={"statistic"})
    async def list_statistics_for_metadata(
        ctx: Context,
        metalake_name: str,
        metadata_type: str,
        metadata_fullname: str,
    ) -> str:
        """
        Retrieve a list of statistics for a specific metadata object. Currently,
            this tool only supports statistics for tables, so `metadata_type`
            should always be "table" and metadata_fullname should be in the format
            "{catalog}.{schema}.{table}". For more information about the metadata
            type and full name formats, please refer to the tool
            'metadata_type_to_fullname_formats'.

        Args:
            ctx (Context): The request context.
            metalake_name (str): The name of the metalake.
            metadata_type (str): The type of metadata (e.g., table, column). For
                more, please refer to too 'metadata_type_to_fullname_formats'
            metadata_fullname (str): The full name of the metadata object. For
                more, please refer to tool 'metadata_type_to_fullname_formats'.


        Returns:
            str: A JSON string containing the list of statistics.

        Example Return Value:
            [
              {
                "name": "custom-key1",
                "value": "value1",
                "reserved": false,
                "modifiable": true,
                "audit": {
                  "creator": "anonymous",
                  "createTime": "2025-08-20T07:33:41.233089Z",
                  "lastModifier": "anonymous",
                  "lastModifiedTime": "2025-08-20T07:33:41.233104Z"
                }
              }
            ]

            name: The name of the statistic.
            value: The value of the statistic.
            reserved: Indicates if the statistic is reserved.
            modifiable: Indicates if the statistic can be modified.
            audit: Metadata about the statistic's creation and modification.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_statistic_operation().list_of_statistics(
            metalake_name, metadata_type, metadata_fullname
        )

    # pylint: disable=R0917
    @mcp.tool(tags={"statistic"})
    async def list_statistics_for_partition(
        ctx: Context,
        metalake_name: str,
        metadata_type: str,
        metadata_fullname: str,
        from_partition_name: str,
        to_partition_name: str,
        from_inclusive: bool = True,
        to_inclusive: bool = False,
    ) -> str:
        """
        Retrieve statistics for a specific partition range of a metadata item.
        Note: This method is currently only supported list statistics for partitions of tables.
            So `metadata_type` should always be "table".

        Args:
            ctx (Context): The request context.
            metalake_name (str): The name of the metalake.
            metadata_type (str): The type of metadata, should be "table" for partition statistics.
            metadata_fullname (str): The full name of the metadata item, the format should be
                "{catalog}.{schema}.{table}".
            from_partition_name (str): Starting partition name.
            to_partition_name (str): Ending partition name.
            from_inclusive (bool): Whether to include the starting partition.
            to_inclusive (bool): Whether to include the ending partition.

        Returns:
            str: A JSON string containing statistics for the specified partitions.

        Example Return Value:
            [
              {
                "partitionName": "partitionName_92f3fa736e47",
                "statistics": [
                  {
                    "name": "custom-key1",
                    "value": "value1",
                    "reserved": false,
                    "modifiable": true,
                    "audit": {
                      "creator": "anonymous",
                      "createTime": "2025-08-20T07:33:41.233089Z",
                      "lastModifier": "anonymous",
                      "lastModifiedTime": "2025-08-20T07:33:41.233104Z"
                    }
                  }
                ]
              }
            ]

            partitionName: The name of the partition.
            statistics: A list of statistics for the partition, each statistic includes:
                - name: The name of the statistic.
                - value: The value of the statistic.
                - reserved: Indicates if the statistic is reserved.
                - modifiable: Indicates if the statistic can be modified.
                - audit: Metadata about the statistic's creation and modification.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return (
            await client.as_statistic_operation().list_statistic_for_partition(
                metalake_name,
                metadata_type,
                metadata_fullname,
                from_partition_name,
                to_partition_name,
                from_inclusive,
                to_inclusive,
            )
        )

    mcp.disable(
        names={
            "update_statistics",
            "drop_statistics",
            "update_partition_statistics",
            "drop_partition_statistics",
        },
        components={"tool"},
    )
