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

from mcp_server.client.plain.utils import (
    encode_path_segment,
    extract_content_from_response,
)
from mcp_server.client.view_operation import ViewOperation


class PlainRESTClientViewOperation(ViewOperation):
    """
    Implementation of ViewOperation using a plain REST client.
    """

    def __init__(self, metalake_name: str, rest_client):
        self.metalake_name = metalake_name
        self.rest_client = rest_client

    async def list_of_views(self, catalog_name: str, schema_name: str) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}/views"
        )
        return extract_content_from_response(response, "identifiers", [])

    async def load_view(
        self, catalog_name: str, schema_name: str, view_name: str
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}"
            f"/views/{encode_path_segment(view_name)}"
        )
        return extract_content_from_response(response, "view", {})

    # pylint: disable=too-many-positional-arguments
    async def create_view(
        self,
        catalog_name: str,
        schema_name: str,
        name: str,
        comment: str,
        columns: list,
        representations: list,
        properties: dict,
        default_catalog: str = None,
        default_schema: str = None,
    ) -> str:
        request = {
            "name": name,
            "comment": comment,
            "columns": columns,
            "representations": representations,
            "properties": properties,
        }
        optional_fields = {
            "defaultCatalog": default_catalog,
            "defaultSchema": default_schema,
        }
        request.update({k: v for k, v in optional_fields.items() if v})
        response = await self.rest_client.post(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}/views",
            json=request,
        )
        return extract_content_from_response(response, "view", {})

    async def alter_view(
        self,
        catalog_name: str,
        schema_name: str,
        view_name: str,
        updates: list,
    ) -> str:
        response = await self.rest_client.put(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}"
            f"/views/{encode_path_segment(view_name)}",
            json={"updates": updates},
        )
        return extract_content_from_response(response, "view", {})

    async def drop_view(
        self, catalog_name: str, schema_name: str, view_name: str
    ) -> str:
        response = await self.rest_client.delete(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}"
            f"/views/{encode_path_segment(view_name)}"
        )
        return extract_content_from_response(response, "dropped", False)
