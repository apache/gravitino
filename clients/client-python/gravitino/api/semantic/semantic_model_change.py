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

from abc import ABC
from dataclasses import dataclass
from typing import Optional

from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition
from gravitino.utils.precondition import Precondition


class SemanticModelChange(ABC):
    """Defines changes that can be applied to a Semantic Model.

    Owner, tag, and policy changes use their existing governance stores and are
    outside this contract.
    """

    @staticmethod
    def rename(new_name: str) -> "RenameSemanticModel":
        """Create a change for renaming a Semantic Model."""
        return RenameSemanticModel(new_name)

    @staticmethod
    def update_comment(new_comment: Optional[str]) -> "UpdateComment":
        """Create a change for updating or clearing a Semantic Model comment."""
        return UpdateComment(new_comment)

    @staticmethod
    def set_property(property_name: str, value: str) -> "SetProperty":
        """Create a change for setting a Semantic Model property."""
        return SetProperty(property_name, value)

    @staticmethod
    def remove_property(property_name: str) -> "RemoveProperty":
        """Create a change for removing a Semantic Model property."""
        return RemoveProperty(property_name)

    @staticmethod
    def replace_definition(
        definition: SemanticModelDefinition,
    ) -> "ReplaceDefinition":
        """Create a change for replacing the complete Semantic Model definition."""
        return ReplaceDefinition(definition)


@dataclass(frozen=True)
class RenameSemanticModel(SemanticModelChange):
    """A SemanticModelChange to rename a Semantic Model."""

    _new_name: str

    def __post_init__(self):
        Precondition.check_string_not_empty(
            self._new_name, "New name must not be null or blank"
        )

    def new_name(self) -> str:
        """Returns the new Semantic Model name."""
        return self._new_name

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RenameSemanticModel):
            return False
        return self._new_name == other.new_name()

    def __hash__(self) -> int:
        return hash(self._new_name)

    def __str__(self) -> str:
        return f"RENAMESEMANTICMODEL {self._new_name}"


@dataclass(frozen=True)
class UpdateComment(SemanticModelChange):
    """A SemanticModelChange to update a Semantic Model comment."""

    _new_comment: Optional[str]

    def new_comment(self) -> Optional[str]:
        """Returns the new comment, `None` clears the current comment."""
        return self._new_comment

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, UpdateComment):
            return False
        return self._new_comment == other.new_comment()

    def __hash__(self) -> int:
        return hash(self._new_comment)

    def __str__(self) -> str:
        return f"UPDATECOMMENT {self._new_comment}"


@dataclass(frozen=True)
class SetProperty(SemanticModelChange):
    """A SemanticModelChange to set a Semantic Model property."""

    _property: str
    _value: str

    def __post_init__(self):
        Precondition.check_string_not_empty(
            self._property, "Property name must not be null or blank"
        )
        Precondition.check_argument(
            self._value is not None, "Property value must not be null"
        )

    def property(self) -> str:
        """Returns the property name."""
        return self._property

    def value(self) -> str:
        """Returns the property value."""
        return self._value

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SetProperty):
            return False
        return self._property == other.property() and self._value == other.value()

    def __hash__(self) -> int:
        return hash((self._property, self._value))

    def __str__(self) -> str:
        return f"SETPROPERTY {self._property} {self._value}"


@dataclass(frozen=True)
class RemoveProperty(SemanticModelChange):
    """A SemanticModelChange to remove a Semantic Model property."""

    _property: str

    def __post_init__(self):
        Precondition.check_string_not_empty(
            self._property, "Property name must not be null or blank"
        )

    def property(self) -> str:
        """Returns the property name."""
        return self._property

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RemoveProperty):
            return False
        return self._property == other.property()

    def __hash__(self) -> int:
        return hash(self._property)

    def __str__(self) -> str:
        return f"REMOVEPROPERTY {self._property}"


@dataclass(frozen=True)
class ReplaceDefinition(SemanticModelChange):
    """A SemanticModelChange to replace the complete Semantic Model definition."""

    _definition: SemanticModelDefinition

    def __post_init__(self):
        Precondition.check_argument(
            self._definition is not None, "Definition must not be null"
        )

    def definition(self) -> SemanticModelDefinition:
        """Returns the replacement definition."""
        return self._definition

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ReplaceDefinition):
            return False
        return self._definition == other.definition()

    def __hash__(self) -> int:
        return hash(self._definition)

    def __str__(self) -> str:
        return f"REPLACEDEFINITION {self._definition}"
