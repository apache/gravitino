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

from gravitino.api.audit import Audit
from gravitino.api.metadata_object import MetadataObject
from gravitino.api.metadata_objects import MetadataObjects
from gravitino.api.semantic.semantic_model import SemanticModel
from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition
from gravitino.api.tag.supports_tags import SupportsTags
from gravitino.api.tag.tag import Tag
from gravitino.api.tag.tag_list import TagList
from gravitino.client.metadata_object_tag_operations import MetadataObjectTagOperations
from gravitino.dto.semantic.semantic_model_dto import SemanticModelDTO
from gravitino.namespace import Namespace
from gravitino.utils import HTTPClient


class GenericSemanticModel(SemanticModel, SupportsTags):
    """An immutable client-side Semantic Model returned by the Gravitino REST API."""

    def __init__(
        self,
        semantic_model_dto: SemanticModelDTO,
        rest_client: HTTPClient,
        semantic_model_ns: Namespace,
    ):
        """Create a GenericSemanticModel from a SemanticModelDTO.

        Args:
            semantic_model_dto (SemanticModelDTO): The Semantic Model DTO.
            rest_client (HTTPClient): The REST client for tag operations.
            semantic_model_ns (Namespace): The full metalake.catalog.schema namespace.
        """
        self._name = semantic_model_dto.name()
        self._comment = semantic_model_dto.comment()
        self._definition = semantic_model_dto.definition()
        self._properties = dict(semantic_model_dto.properties())
        self._audit = semantic_model_dto.audit_info()
        model_object = MetadataObjects.of(
            [semantic_model_ns.level(1), semantic_model_ns.level(2), self._name],
            MetadataObject.Type.SEMANTIC_MODEL,
        )
        self._model_tag_operations = MetadataObjectTagOperations(
            semantic_model_ns.level(0), model_object, rest_client
        )

    def name(self) -> str:
        return self._name

    def comment(self) -> Optional[str]:
        return self._comment

    def definition(self) -> SemanticModelDefinition:
        return self._definition

    def properties(self) -> dict[str, str]:
        return dict(self._properties)

    def audit_info(self) -> Audit:
        return self._audit

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GenericSemanticModel):
            return False
        return (
            self._name == other.name()
            and self._comment == other.comment()
            and self._definition == other.definition()
            and self._properties == other.properties()
            and self._audit == other.audit_info()
        )

    def __hash__(self) -> int:
        return hash((self._name, self._comment, self._definition))

    def __repr__(self) -> str:
        return (
            f"GenericSemanticModel(name={self._name!r}, comment={self._comment!r}, "
            f"definition={self._definition!r}, properties={self._properties!r}, "
            f"audit={self._audit!r})"
        )

    def supports_tags(self) -> SupportsTags:
        return self

    def list_tags(self) -> list[str]:
        return self._model_tag_operations.list_tags()

    def list_tags_with_visibility(self) -> TagList[str]:
        return self._model_tag_operations.list_tags_with_visibility()

    def list_tags_info(self) -> list[Tag]:
        return self._model_tag_operations.list_tags_info()

    def list_tags_info_with_visibility(self) -> TagList[Tag]:
        return self._model_tag_operations.list_tags_info_with_visibility()

    def get_tag(self, name: str) -> Tag:
        return self._model_tag_operations.get_tag(name)

    def assign_tags(
        self,
        tags_to_add: list[str | dict[str, str | None]] | None = None,
        tags_to_remove: list[str | dict[str, str | None]] | None = None,
    ) -> list[str]:
        return self._model_tag_operations.assign_tags(tags_to_add, tags_to_remove)

    def associate_tags(
        self, tags_to_add: list[str], tags_to_remove: list[str]
    ) -> list[str]:
        return self._model_tag_operations.associate_tags(tags_to_add, tags_to_remove)
