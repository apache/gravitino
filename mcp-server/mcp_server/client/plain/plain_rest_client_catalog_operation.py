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

from mcp_server.client import CatalogOperation
from mcp_server.client.plain.utils import (
    encode_path_segment,
    extract_content_from_response,
    extract_response,
)


class PlainRESTClientCatalogOperation(CatalogOperation):
    def __init__(self, metalake_name: str, rest_client: AsyncClient):
        self.metalake_name = metalake_name
        self.rest_client = rest_client

    async def get_list_of_catalogs(self) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}/catalogs?details=true"
        )
        return extract_content_from_response(response, "catalogs", [])

    # pylint: disable=too-many-positional-arguments
    async def create_catalog(
        self,
        name: str,
        catalog_type: str,
        provider: str,
        comment: str,
        properties: dict,
    ) -> str:
        response = await self.rest_client.post(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}/catalogs",
            json={
                "name": name,
                "type": catalog_type,
                "provider": provider,
                "comment": comment,
                "properties": properties,
            },
        )
        return extract_content_from_response(response, "catalog", {})

    async def alter_catalog(self, catalog_name: str, updates: list) -> str:
        response = await self.rest_client.put(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}",
            json={"updates": updates},
        )
        return extract_content_from_response(response, "catalog", {})

    async def drop_catalog(self, catalog_name: str, force: bool) -> str:
        response = await self.rest_client.delete(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}",
            params={"force": force},
        )
        return extract_content_from_response(response, "dropped", False)

    async def set_catalog_in_use(self, catalog_name: str, in_use: bool) -> str:
        response = await self.rest_client.patch(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}",
            json={"inUse": in_use},
        )
        return extract_response(response)
