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

from dataclasses_json import config

from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition
from gravitino.dto.semantic.semantic_dto_utils import is_none
from gravitino.dto.semantic.semantic_model_definition_dto import (
    SemanticModelDefinitionDTO,
)
from gravitino.rest.rest_message import RESTRequest
from gravitino.utils.precondition import Precondition


@dataclass
class SemanticModelCreateRequest(RESTRequest):
    """Represents a request to create a Semantic Model."""

    _name: Optional[str] = field(default=None, metadata=config(field_name="name"))
    _comment: Optional[str] = field(
        default=None,
        metadata=config(field_name="comment", exclude=is_none),
    )
    _definition: Optional[SemanticModelDefinitionDTO] = field(
        default=None, metadata=config(field_name="definition")
    )
    _properties: Optional[dict[str, str]] = field(
        default=None, metadata=config(field_name="properties")
    )

    def validate(self) -> None:
        Precondition.check_string_not_empty(
            self._name, '"name" field is required and cannot be empty'
        )
        Precondition.check_argument(
            self._definition is not None,
            '"definition" field is required and cannot be null',
        )
        Precondition.check_argument(
            self._properties is not None,
            '"properties" field is required and cannot be null',
        )
        self.to_definition()

    def to_definition(self) -> SemanticModelDefinition:
        """Convert the definition in this request to an API definition.

        Raises:
            IllegalArgumentException: If a definition field is invalid.
        """
        return self._definition.to_definition()
