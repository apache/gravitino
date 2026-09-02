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
from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import config, dataclass_json

from gravitino.api.semantic.semantic_model_change import SemanticModelChange
from gravitino.dto.semantic.semantic_model_definition_dto import (
    SemanticModelDefinitionDTO,
)
from gravitino.rest.rest_message import RESTRequest
from gravitino.utils.precondition import Precondition


@dataclass_json
@dataclass
class SemanticModelUpdateRequestBase(RESTRequest, ABC):
    """Base class for all Semantic Model update requests."""

    _type: str = field(init=False, metadata=config(field_name="@type"))

    @abstractmethod
    def semantic_model_change(self) -> SemanticModelChange:
        """Convert to a Semantic Model change operation."""


class SemanticModelUpdateRequest:
    """Namespace for all Semantic Model update request types."""

    @dataclass_json
    @dataclass
    class RenameSemanticModelRequest(SemanticModelUpdateRequestBase):
        """Update request to rename a Semantic Model."""

        _new_name: str = field(metadata=config(field_name="newName"))

        def __post_init__(self):
            self._type = "rename"

        def validate(self):
            Precondition.check_string_not_empty(
                self._new_name, '"newName" field is required and cannot be empty'
            )

        def semantic_model_change(self) -> SemanticModelChange:
            return SemanticModelChange.rename(self._new_name)

    @dataclass_json
    @dataclass
    class UpdateSemanticModelCommentRequest(SemanticModelUpdateRequestBase):
        """Update request to update or clear a Semantic Model comment."""

        _new_comment: Optional[str] = field(
            default=None, metadata=config(field_name="newComment")
        )

        def __post_init__(self):
            self._type = "updateComment"

        def validate(self):
            """Validate the request.

            A null comment clears the current comment, and an empty comment is
            stored as supplied, so there is nothing to check.
            """

        def semantic_model_change(self) -> SemanticModelChange:
            return SemanticModelChange.update_comment(self._new_comment)

    @dataclass_json
    @dataclass
    class SetSemanticModelPropertyRequest(SemanticModelUpdateRequestBase):
        """Update request to set a Semantic Model property."""

        _property: str = field(metadata=config(field_name="property"))
        _value: str = field(metadata=config(field_name="value"))

        def __post_init__(self):
            self._type = "setProperty"

        def validate(self):
            Precondition.check_string_not_empty(
                self._property, '"property" field is required and cannot be empty'
            )
            Precondition.check_argument(
                self._value is not None, '"value" field is required and cannot be null'
            )

        def semantic_model_change(self) -> SemanticModelChange:
            return SemanticModelChange.set_property(self._property, self._value)

    @dataclass_json
    @dataclass
    class RemoveSemanticModelPropertyRequest(SemanticModelUpdateRequestBase):
        """Update request to remove a Semantic Model property."""

        _property: str = field(metadata=config(field_name="property"))

        def __post_init__(self):
            self._type = "removeProperty"

        def validate(self):
            Precondition.check_string_not_empty(
                self._property, '"property" field is required and cannot be empty'
            )

        def semantic_model_change(self) -> SemanticModelChange:
            return SemanticModelChange.remove_property(self._property)

    @dataclass_json
    @dataclass
    class ReplaceSemanticModelDefinitionRequest(SemanticModelUpdateRequestBase):
        """Update request to replace the complete Semantic Model definition."""

        _definition: Optional[SemanticModelDefinitionDTO] = field(
            default=None, metadata=config(field_name="definition")
        )

        def __post_init__(self):
            self._type = "replaceDefinition"

        def validate(self):
            Precondition.check_argument(
                self._definition is not None,
                '"definition" field is required and cannot be null',
            )
            self._definition.to_definition()

        def semantic_model_change(self) -> SemanticModelChange:
            return SemanticModelChange.replace_definition(
                self._definition.to_definition()
            )
