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
from mcp_server.client.plain.utils import (
    encode_path_segment,
    extract_content_from_response,
)


class PlainRESTClientTableOperation(TableOperation):

    def __init__(self, metalake_name: str, rest_client: AsyncClient):
        self.metalake_name = metalake_name
        self.rest_client = rest_client

    async def get_list_of_tables(
        self, catalog_name: str, schema_name: str
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}/tables"
        )
        return extract_content_from_response(response, "identifiers", [])

    async def load_table(
        self, catalog_name: str, schema_name: str, table_name: str
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}"
            f"/tables/{encode_path_segment(table_name)}"
        )
        return extract_content_from_response(response, "table", {})

    # pylint: disable=too-many-positional-arguments
    async def create_table(
        self,
        catalog_name: str,
        schema_name: str,
        name: str,
        comment: str,
        columns: list,
        properties: dict,
        partitioning: list = None,
        distribution: dict = None,
        sort_orders: list = None,
        indexes: list = None,
    ) -> str:
        request = {
            "name": name,
            "comment": comment,
            "columns": columns,
            "properties": properties,
        }
        optional_fields = {
            "partitioning": partitioning,
            "distribution": distribution,
            "sortOrders": sort_orders,
            "indexes": indexes,
        }
        request.update({k: v for k, v in optional_fields.items() if v})
        response = await self.rest_client.post(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}/tables",
            json=request,
        )
        return extract_content_from_response(response, "table", {})

    async def alter_table(
        self,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        updates: list,
    ) -> str:
        response = await self.rest_client.put(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}"
            f"/tables/{encode_path_segment(table_name)}",
            json={"updates": updates},
        )
        return extract_content_from_response(response, "table", {})

    async def drop_table(
        self, catalog_name: str, schema_name: str, table_name: str, purge: bool
    ) -> str:
        response = await self.rest_client.delete(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}"
            f"/tables/{encode_path_segment(table_name)}",
            params={"purge": purge},
        )
        return extract_content_from_response(response, "dropped", False)
