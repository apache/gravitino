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

from typing import Optional

from httpx import AsyncClient

from mcp_server.client.plain.utils import (
    encode_path_segment,
    extract_content_from_response,
)
from mcp_server.client.semantic_model_operation import SemanticModelOperation


class PlainRESTClientSemanticModelOperation(SemanticModelOperation):
    """Access Semantic Models through Gravitino's native REST API."""

    def __init__(self, metalake_name: str, rest_client: AsyncClient):
        self.metalake_name = metalake_name
        self.rest_client = rest_client

    async def list_of_semantic_models(
        self, catalog_name: str, schema_name: str
    ) -> str:
        response = await self.rest_client.get(
            self._models_path(catalog_name, schema_name)
        )
        return extract_content_from_response(response, "identifiers", [])

    async def load_semantic_model(
        self, catalog_name: str, schema_name: str, semantic_model_name: str
    ) -> str:
        response = await self.rest_client.get(
            self._model_path(catalog_name, schema_name, semantic_model_name)
        )
        return extract_content_from_response(response, "semanticModel", {})

    # pylint: disable=too-many-positional-arguments
    async def create_semantic_model(
        self,
        catalog_name: str,
        schema_name: str,
        name: str,
        definition: dict,
        comment: Optional[str] = None,
        properties: Optional[dict[str, str]] = None,
    ) -> str:
        body = {"name": name, "definition": definition}
        if comment is not None:
            body["comment"] = comment
        if properties is not None:
            body["properties"] = properties
        response = await self.rest_client.post(
            self._models_path(catalog_name, schema_name), json=body
        )
        return extract_content_from_response(response, "semanticModel", {})

    async def alter_semantic_model(
        self,
        catalog_name: str,
        schema_name: str,
        semantic_model_name: str,
        updates: list[dict],
    ) -> str:
        response = await self.rest_client.put(
            self._model_path(catalog_name, schema_name, semantic_model_name),
            json={"updates": updates},
        )
        return extract_content_from_response(response, "semanticModel", {})

    async def drop_semantic_model(
        self, catalog_name: str, schema_name: str, semantic_model_name: str
    ) -> str:
        response = await self.rest_client.delete(
            self._model_path(catalog_name, schema_name, semantic_model_name)
        )
        return extract_content_from_response(response, "dropped", False)

    def _models_path(self, catalog_name: str, schema_name: str) -> str:
        return (
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}/semantic-models"
        )

    def _model_path(
        self, catalog_name: str, schema_name: str, semantic_model_name: str
    ) -> str:
        return (
            f"{self._models_path(catalog_name, schema_name)}"
            f"/{encode_path_segment(semantic_model_name)}"
        )
