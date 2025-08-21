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

from mcp_server.client.plain.utils import extract_content_from_response
from mcp_server.client.statistic_operation import StatisticOperation


class PlainRESTClientStatisticOperation(StatisticOperation):
    def __init__(self, metalake_name: str, rest_client):
        self.metalake_name = metalake_name
        self.rest_client = rest_client

    async def list_of_statistics(
        self, metalake_name: str, metadata_type: str, metadata_fullname: str
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{metalake_name}/objects/{metadata_type}/{metadata_fullname}/statistics"
        )
        return extract_content_from_response(response, "statistics", [])

    # pylint: disable=R0917
    async def list_statistic_for_partition(
        self,
        metalake_name: str,
        metadata_type: str,
        metadata_fullname: str,
        from_partition_name: str,
        to_partition_name: str,
        from_inclusive: bool = True,
        to_inclusive: bool = False,
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{metalake_name}/objects/{metadata_type}/{metadata_fullname}/statistics/"
            f"partitions?from={from_partition_name}&to={to_partition_name}&fromInclusive={from_inclusive}"
            f"&toInclusive={to_inclusive}"
        )
        return extract_content_from_response(
            response, "partitionStatistics", []
        )
