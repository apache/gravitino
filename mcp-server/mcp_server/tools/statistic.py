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
