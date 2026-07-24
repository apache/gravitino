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

from mcp_server.client.partition_operation import PartitionOperation
from mcp_server.client.plain.utils import (
    encode_path_segment,
    extract_content_from_response,
)


class PlainRESTClientPartitionOperation(PartitionOperation):
    """
    Implementation of PartitionOperation using a plain REST client.
    """

    def __init__(self, metalake_name: str, rest_client):
        self.metalake_name = metalake_name
        self.rest_client = rest_client

    async def list_of_partitions(
        self,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        details: bool = False,
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}"
            f"/tables/{encode_path_segment(table_name)}/partitions",
            params={"details": details},
        )
        field = "partitions" if details else "names"
        return extract_content_from_response(response, field, [])

    async def get_partition(
        self,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        partition_name: str,
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}"
            f"/tables/{encode_path_segment(table_name)}"
            f"/partitions/{encode_path_segment(partition_name)}"
        )
        return extract_content_from_response(response, "partition", {})
