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


"""Gravitino REST implementation of ontology operations."""

from __future__ import annotations

import simplejson
from httpx import AsyncClient

from mcp_server.client.ontology_operation import OntologyOperation
from mcp_server.client.plain.utils import (
    encode_path_segment,
    extract_content_from_response,
    extract_list_from_response,
)


# pylint: disable=too-many-arguments,too-many-positional-arguments
class PlainRESTClientOntologyOperation(OntologyOperation):
    """Uses the current caller's REST client for ontology and mapping operations."""

    def __init__(self, metalake: str, client: AsyncClient):
        self.client = client
        self._metalake = metalake

    @property
    def _metalake_path(self) -> str:
        return f"/api/metalakes/{encode_path_segment(self._metalake)}"

    @property
    def _ontologies(self) -> str:
        return f"{self._metalake_path}/ontologies"

    def _mappings(self, catalog: str, schema: str, model: str) -> str:
        return (
            f"{self._metalake_path}/catalogs/{encode_path_segment(catalog)}"
            f"/schemas/{encode_path_segment(schema)}"
            f"/semantic-models/{encode_path_segment(model)}/ontology-mappings"
        )

    async def list_ontologies(self) -> str:
        response = await self.client.get(self._ontologies)
        return extract_list_from_response(response, "names")

    async def load_ontology(self, name: str) -> str:
        response = await self.client.get(
            self._ontologies + "/" + encode_path_segment(name)
        )
        return extract_content_from_response(response, "ontology")

    async def create_ontology(
        self,
        name: str,
        definition: dict,
        comment: str | None = None,
        properties: dict[str, str] | None = None,
    ) -> str:
        response = await self.client.post(
            self._ontologies,
            json={
                "name": name,
                "definition": definition,
                "comment": comment,
                "properties": properties,
            },
        )
        return extract_content_from_response(response, "ontology")

    async def alter_ontology(self, name: str, updates: list[dict]) -> str:
        response = await self.client.put(
            self._ontologies + "/" + encode_path_segment(name),
            json={"updates": updates},
        )
        return extract_content_from_response(response, "ontology")

    async def drop_ontology(self, name: str) -> str:
        response = await self.client.delete(
            self._ontologies + "/" + encode_path_segment(name)
        )
        return extract_content_from_response(response, "dropped")

    async def list_ontology_mappings(
        self, catalog_name: str, schema_name: str, semantic_model_name: str
    ) -> str:
        response = await self.client.get(
            self._mappings(catalog_name, schema_name, semantic_model_name)
        )
        return extract_list_from_response(response, "identifiers")

    async def load_ontology_mapping(
        self,
        catalog_name: str,
        schema_name: str,
        semantic_model_name: str,
        name: str,
    ) -> str:
        response = await self.client.get(
            self._mappings(catalog_name, schema_name, semantic_model_name)
            + "/"
            + encode_path_segment(name)
        )
        return extract_content_from_response(response, "ontologyMapping")

    async def create_ontology_mapping(
        self,
        catalog_name: str,
        schema_name: str,
        semantic_model_name: str,
        name: str,
        ontology: str,
        definition: dict,
        comment: str | None = None,
        properties: dict[str, str] | None = None,
    ) -> str:
        response = await self.client.post(
            self._mappings(catalog_name, schema_name, semantic_model_name),
            json={
                "name": name,
                "definition": definition,
                "comment": comment,
                "properties": properties,
                "ontology": ontology,
            },
        )
        return extract_content_from_response(response, "ontologyMapping")

    async def alter_ontology_mapping(
        self,
        catalog_name: str,
        schema_name: str,
        semantic_model_name: str,
        name: str,
        updates: list[dict],
    ) -> str:
        response = await self.client.put(
            self._mappings(catalog_name, schema_name, semantic_model_name)
            + "/"
            + encode_path_segment(name),
            json={"updates": updates},
        )
        return extract_content_from_response(response, "ontologyMapping")

    async def drop_ontology_mapping(
        self,
        catalog_name: str,
        schema_name: str,
        semantic_model_name: str,
        name: str,
    ) -> str:
        response = await self.client.delete(
            self._mappings(catalog_name, schema_name, semantic_model_name)
            + "/"
            + encode_path_segment(name)
        )
        return extract_content_from_response(response, "dropped")

    async def list_mappings_by_ontology(self, name: str) -> str:
        response = await self.client.get(
            f"{self._ontologies}/{encode_path_segment(name)}/mappings"
        )
        identifiers = simplejson.loads(
            extract_list_from_response(response, "identifiers"),
            use_decimal=True,
        )
        for identifier in identifiers:
            namespace = identifier["namespace"]
            if len(namespace) == 4:
                identifier["namespace"] = namespace[1:]
        return simplejson.dumps(identifiers, use_decimal=True)
