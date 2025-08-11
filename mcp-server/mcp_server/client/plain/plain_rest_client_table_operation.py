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

from httpx import AsyncClient

from mcp_server.client import TableOperation
from mcp_server.client.plain.utils import extract_content_from_response


class PlainRESTClientTableOperation(TableOperation):

    def __init__(self, metalake_name: str, rest_client: AsyncClient):
        self.metalake_name = metalake_name
        self.rest_client = rest_client

    async def get_list_of_tables(
        self, catalog_name: str, schema_name: str
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/tables"
        )
        return extract_content_from_response(response, "identifiers", [])

    async def load_table(
        self, catalog_name: str, schema_name: str, table_name: str
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}"
        )
        return extract_content_from_response(response, "table", {})
