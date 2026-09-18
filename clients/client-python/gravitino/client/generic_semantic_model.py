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
from gravitino.api.semantic.semantic_model import SemanticModel
from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition
from gravitino.dto.semantic.semantic_model_dto import SemanticModelDTO


class GenericSemanticModel(SemanticModel):
    """An immutable client-side Semantic Model returned by the Gravitino REST API."""

    def __init__(self, semantic_model_dto: SemanticModelDTO):
        """Create a GenericSemanticModel from a SemanticModelDTO.

        Args:
            semantic_model_dto (SemanticModelDTO): The Semantic Model DTO.
        """
        self._name = semantic_model_dto.name()
        self._comment = semantic_model_dto.comment()
        self._definition = semantic_model_dto.definition()
        self._properties = dict(semantic_model_dto.properties())
        self._audit = semantic_model_dto.audit_info()

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
