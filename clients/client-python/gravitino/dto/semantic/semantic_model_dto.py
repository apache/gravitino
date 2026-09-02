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

from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.semantic.semantic_model import SemanticModel
from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.semantic.semantic_model_definition_dto import (
    SemanticModelDefinitionDTO,
)
from gravitino.utils.precondition import Precondition
from gravitino.dto.semantic.semantic_dto_utils import is_none


@dataclass
class SemanticModelDTO(SemanticModel, DataClassJsonMixin):
    """Represents a schema-scoped Semantic Model DTO."""

    _name: Optional[str] = field(
        default=None, metadata=config(field_name="name", exclude=is_none)
    )
    _comment: Optional[str] = field(
        default=None,
        metadata=config(field_name="comment", exclude=is_none),
    )
    _definition: Optional[SemanticModelDefinitionDTO] = field(
        default=None, metadata=config(field_name="definition", exclude=is_none)
    )
    _properties: Optional[dict[str, str]] = field(
        default=None, metadata=config(field_name="properties", exclude=is_none)
    )
    _audit: Optional[AuditDTO] = field(
        default=None, metadata=config(field_name="audit", exclude=is_none)
    )

    def name(self) -> Optional[str]:
        return self._name

    def comment(self) -> Optional[str]:
        return self._comment

    def definition(self) -> SemanticModelDefinition:
        Precondition.check_argument(
            self._definition is not None, "definition must not be null"
        )
        return self._definition.to_definition()

    def definition_dto(self) -> Optional[SemanticModelDefinitionDTO]:
        """Returns the definition DTO without converting it to the API type."""
        return self._definition

    def properties(self) -> dict[str, str]:
        return self._properties or {}

    def audit_info(self) -> Optional[AuditDTO]:
        return self._audit
