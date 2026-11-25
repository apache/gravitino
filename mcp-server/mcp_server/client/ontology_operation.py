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


"""The ontology operations used by the MCP tools."""

from __future__ import annotations

from abc import ABC, abstractmethod


class OntologyOperation(ABC):
    """Access ontologies and model mappings."""

    @abstractmethod
    async def list_ontologies(self) -> str:
        """List visible ontology names."""

    @abstractmethod
    async def load_ontology(self, name: str) -> str:
        """Load an ontology and its definition."""

    @abstractmethod
    async def create_ontology(
        self,
        name: str,
        definition: dict,
        comment: str | None = None,
        properties: dict[str, str] | None = None,
    ) -> str:
        """Create an ontology from its native definition."""

    @abstractmethod
    async def alter_ontology(self, name: str, updates: list[dict]) -> str:
        """Alter an ontology with an ordered batch of changes."""

    @abstractmethod
    async def drop_ontology(self, name: str) -> str:
        """Drop an ontology that has no live mappings."""

    @abstractmethod
    async def list_ontology_mappings(
        self, catalog_name: str, schema_name: str, semantic_model_name: str
    ) -> str:
        """List mapping identifiers under a semantic model."""

    @abstractmethod
    async def load_ontology_mapping(
        self,
        catalog_name: str,
        schema_name: str,
        semantic_model_name: str,
        name: str,
    ) -> str:
        """Load a mapping and its definition."""

    @abstractmethod
    # pylint: disable=too-many-positional-arguments
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
        """Create a mapping bound to an ontology."""

    @abstractmethod
    # pylint: disable=too-many-positional-arguments
    async def alter_ontology_mapping(
        self,
        catalog_name: str,
        schema_name: str,
        semantic_model_name: str,
        name: str,
        updates: list[dict],
    ) -> str:
        """Alter a mapping with an ordered batch of changes."""

    @abstractmethod
    async def drop_ontology_mapping(
        self,
        catalog_name: str,
        schema_name: str,
        semantic_model_name: str,
        name: str,
    ) -> str:
        """Drop a mapping."""

    @abstractmethod
    async def list_mappings_by_ontology(self, name: str) -> str:
        """List mappings of an ontology whose semantic models the caller can load."""
