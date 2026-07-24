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

from mcp_server.client import ModelOperation
from mcp_server.client.plain.utils import (
    encode_path_segment,
    extract_content_from_response,
    extract_response,
)


class PlainRESTClientModelOperation(ModelOperation):
    """
    Implementation of ModelOperation using a plain REST client.
    """

    def __init__(self, metalake_name: str, rest_client):
        self.metalake_name = metalake_name
        self.rest_client = rest_client

    async def list_of_models(self, catalog_name: str, schema_name: str) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}/models"
        )
        return extract_content_from_response(response, "identifiers", [])

    async def load_model(
        self, catalog_name: str, schema_name: str, model_name: str
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}"
            f"/models/{encode_path_segment(model_name)}"
        )
        return extract_content_from_response(response, "model", {})

    async def list_model_versions(
        self, catalog_name: str, schema_name: str, model_name: str
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}"
            f"/models/{encode_path_segment(model_name)}/versions?details=true"
        )
        return extract_content_from_response(response, "infos", [])

    async def load_model_version(
        self, catalog_name: str, schema_name: str, model_name: str, version: int
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}"
            f"/models/{encode_path_segment(model_name)}"
            f"/versions/{encode_path_segment(version)}"
        )
        return extract_content_from_response(response, "modelVersion", {})

    async def load_model_version_by_alias(
        self, catalog_name: str, schema_name: str, model_name: str, alias: str
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}"
            f"/models/{encode_path_segment(model_name)}"
            f"/aliases/{encode_path_segment(alias)}"
        )
        return extract_content_from_response(response, "modelVersion", {})

    # pylint: disable=too-many-positional-arguments
    async def register_model(
        self,
        catalog_name: str,
        schema_name: str,
        name: str,
        comment: str,
        properties: dict,
    ) -> str:
        response = await self.rest_client.post(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}/models",
            json={"name": name, "comment": comment, "properties": properties},
        )
        return extract_content_from_response(response, "model", {})

    async def delete_model(
        self, catalog_name: str, schema_name: str, model_name: str
    ) -> str:
        response = await self.rest_client.delete(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}"
            f"/models/{encode_path_segment(model_name)}"
        )
        return extract_content_from_response(response, "dropped", False)

    # pylint: disable=too-many-positional-arguments
    async def link_model_version(
        self,
        catalog_name: str,
        schema_name: str,
        model_name: str,
        uri: str,
        aliases: list,
        comment: str,
        properties: dict,
    ) -> str:
        response = await self.rest_client.post(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}"
            f"/models/{encode_path_segment(model_name)}/versions",
            json={
                "uri": uri,
                "aliases": aliases,
                "comment": comment,
                "properties": properties,
            },
        )
        return extract_response(response)

    async def delete_model_version(
        self, catalog_name: str, schema_name: str, model_name: str, version: int
    ) -> str:
        response = await self.rest_client.delete(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}"
            f"/models/{encode_path_segment(model_name)}"
            f"/versions/{encode_path_segment(version)}"
        )
        return extract_content_from_response(response, "dropped", False)

    async def delete_model_version_by_alias(
        self, catalog_name: str, schema_name: str, model_name: str, alias: str
    ) -> str:
        response = await self.rest_client.delete(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}"
            f"/models/{encode_path_segment(model_name)}"
            f"/aliases/{encode_path_segment(alias)}"
        )
        return extract_content_from_response(response, "dropped", False)

    async def alter_model(
        self,
        catalog_name: str,
        schema_name: str,
        model_name: str,
        updates: list,
    ) -> str:
        response = await self.rest_client.put(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}"
            f"/models/{encode_path_segment(model_name)}",
            json={"updates": updates},
        )
        return extract_content_from_response(response, "model", {})

    # pylint: disable=too-many-positional-arguments
    async def alter_model_version(
        self,
        catalog_name: str,
        schema_name: str,
        model_name: str,
        version: int,
        updates: list,
    ) -> str:
        response = await self.rest_client.put(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}"
            f"/models/{encode_path_segment(model_name)}"
            f"/versions/{encode_path_segment(version)}",
            json={"updates": updates},
        )
        return extract_content_from_response(response, "modelVersion", {})

    # pylint: disable=too-many-positional-arguments
    async def alter_model_version_by_alias(
        self,
        catalog_name: str,
        schema_name: str,
        model_name: str,
        alias: str,
        updates: list,
    ) -> str:
        response = await self.rest_client.put(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}"
            f"/models/{encode_path_segment(model_name)}"
            f"/aliases/{encode_path_segment(alias)}",
            json={"updates": updates},
        )
        return extract_content_from_response(response, "modelVersion", {})

    # pylint: disable=R0917
    async def update_model_version_aliases(
        self,
        catalog_name: str,
        schema_name: str,
        model_name: str,
        version: int,
        aliases_to_add: list,
        aliases_to_remove: list,
    ) -> str:
        response = await self.rest_client.put(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}"
            f"/models/{encode_path_segment(model_name)}"
            f"/versions/{encode_path_segment(version)}",
            json={
                "updates": [
                    {
                        "@type": "updateAliases",
                        "aliasesToAdd": aliases_to_add,
                        "aliasesToRemove": aliases_to_remove,
                    }
                ]
            },
        )
        return extract_content_from_response(response, "modelVersion", {})
