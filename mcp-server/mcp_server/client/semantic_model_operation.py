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

from abc import ABC, abstractmethod
from typing import Optional


class SemanticModelOperation(ABC):
    """Operations on schema-scoped Gravitino Semantic Models."""

    @abstractmethod
    async def list_of_semantic_models(
        self, catalog_name: str, schema_name: str
    ) -> str:
        """Return Semantic Model identifiers as a JSON array."""
        pass

    @abstractmethod
    async def load_semantic_model(
        self, catalog_name: str, schema_name: str, semantic_model_name: str
    ) -> str:
        """Return the Semantic Model's metadata and definition as JSON."""
        pass

    # pylint: disable=too-many-positional-arguments
    @abstractmethod
    async def create_semantic_model(
        self,
        catalog_name: str,
        schema_name: str,
        name: str,
        definition: dict,
        comment: Optional[str] = None,
        properties: Optional[dict[str, str]] = None,
    ) -> str:
        """Create a Semantic Model with a native REST definition."""
        pass

    @abstractmethod
    async def alter_semantic_model(
        self,
        catalog_name: str,
        schema_name: str,
        semantic_model_name: str,
        updates: list[dict],
    ) -> str:
        """Apply native REST update operations atomically and return JSON."""
        pass

    @abstractmethod
    async def drop_semantic_model(
        self, catalog_name: str, schema_name: str, semantic_model_name: str
    ) -> str:
        """Return a JSON boolean indicating whether the model was dropped."""
        pass
