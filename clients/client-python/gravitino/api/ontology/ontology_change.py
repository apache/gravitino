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
from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field
from typing import Any, Optional, Union

from dataclasses_json import config

from gravitino.api.ontology.ontology import Ontology
from gravitino.api.ontology.ontology_definition import OntologyDefinition
from gravitino.utils.precondition import Precondition


class OntologyChange(ABC):
    """A change that can be applied to an ontology.

    All the change factories are static, so a caller writes
    `OntologyChange.rename("Finance")` to build one.
    """

    @staticmethod
    def rename(new_name: str) -> RenameOntology:
        """Create a change that renames an ontology.

        Args:
            new_name (str): The new ontology name.

        Returns:
            RenameOntology: The rename change.
        """
        return OntologyChange.RenameOntology(new_name)

    @staticmethod
    def update_comment(new_comment: Optional[str]) -> UpdateComment:
        """Create a change that replaces an ontology comment.

        Args:
            new_comment (Optional[str]): The new comment, or `None` to remove
                the current comment.

        Returns:
            UpdateComment: The comment change.
        """
        return OntologyChange.UpdateComment(new_comment)

    @staticmethod
    def set_property(property_name: str, value: str) -> SetProperty:
        """Create a change that sets a Gravitino-specific ontology property.

        If the property already exists, its value is replaced.

        Args:
            property_name (str): The property name.
            value (str): The property value.

        Returns:
            SetProperty: The property change.
        """
        return OntologyChange.SetProperty(property_name, value)

    @staticmethod
    def remove_property(property_name: str) -> RemoveProperty:
        """Create a change that removes a Gravitino-specific ontology property.

        If the property does not exist, applying the change succeeds without
        modifying the ontology.

        Args:
            property_name (str): The property name.

        Returns:
            RemoveProperty: The property removal change.
        """
        return OntologyChange.RemoveProperty(property_name)

    @staticmethod
    def replace_definition(
        definition: Union[OntologyDefinition, dict[str, Any]],
    ) -> ReplaceDefinition:
        """Create a change that atomically replaces the complete definition.

        The ontology name, comment, status and Gravitino-specific properties are
        not affected.

        Args:
            definition (Union[OntologyDefinition, dict[str, Any]]): The
                replacement definition, either an `OntologyDefinitionDTO` or the
                JSON object that carries it.

        Returns:
            ReplaceDefinition: The definition replacement change.
        """
        return OntologyChange.ReplaceDefinition(definition)

    @staticmethod
    def update_status(status: str) -> UpdateStatus:
        """Create a change that publishes or unpublishes an ontology.

        Setting the status the ontology already has has no effect.

        Args:
            status (str): The new status, one of `Ontology.Status.DRAFT` or
                `Ontology.Status.PUBLISHED`.

        Returns:
            UpdateStatus: The status change.
        """
        return OntologyChange.UpdateStatus(status)

    @dataclass(frozen=True, eq=True)
    class RenameOntology:
        """A change that renames an ontology."""

        _new_name: str = field(metadata=config(field_name="newName"))

        def __post_init__(self) -> None:
            Precondition.check_argument(
                self._new_name is not None and self._new_name.strip() != "",
                "New name must not be null or blank",
            )

        @property
        def new_name(self) -> str:
            """The new name of the ontology."""
            return self._new_name

        def __str__(self) -> str:
            return f"RENAMEONTOLOGY {self._new_name}"

    @dataclass(frozen=True, eq=True)
    class UpdateComment:
        """A change that replaces an ontology comment."""

        _new_comment: Optional[str] = field(
            default=None, metadata=config(field_name="newComment")
        )

        @property
        def new_comment(self) -> Optional[str]:
            """The new comment of the ontology, `None` removes the comment."""
            return self._new_comment

        def __str__(self) -> str:
            return f"UPDATEONTOLOGYCOMMENT {self._new_comment}"

    @dataclass(frozen=True, eq=True)
    class SetProperty:
        """A change that sets a property-value pair for an ontology."""

        _property: str = field(metadata=config(field_name="property"))
        _value: str = field(metadata=config(field_name="value"))

        def __post_init__(self) -> None:
            Precondition.check_argument(
                self._property is not None and self._property.strip() != "",
                "Property name must not be null or blank",
            )
            Precondition.check_argument(
                self._value is not None, "Property value must not be null"
            )

        @property
        def name(self) -> str:
            """The property to set."""
            return self._property

        @property
        def value(self) -> str:
            """The value to set."""
            return self._value

        def __str__(self) -> str:
            return f"SETONTOLOGYPROPERTY {self._property} = {self._value}"

    @dataclass(frozen=True, eq=True)
    class RemoveProperty:
        """A change that removes a property-value pair from an ontology."""

        _property: str = field(metadata=config(field_name="property"))

        def __post_init__(self) -> None:
            Precondition.check_argument(
                self._property is not None and self._property.strip() != "",
                "Property name must not be null or blank",
            )

        @property
        def removed_property(self) -> str:
            """The property to remove."""
            return self._property

        def __str__(self) -> str:
            return f"REMOVEONTOLOGYPROPERTY {self._property}"

    @dataclass(frozen=True, eq=True)
    class ReplaceDefinition:
        """A change that atomically replaces the complete ontology definition."""

        _definition: Union[OntologyDefinition, dict[str, Any]] = field(
            metadata=config(field_name="definition")
        )

        def __post_init__(self) -> None:
            Precondition.check_argument(
                self._definition is not None, "Definition must not be null"
            )

        @property
        def definition(self) -> Union[OntologyDefinition, dict[str, Any]]:
            """The replacement definition."""
            return self._definition

        def __str__(self) -> str:
            return f"REPLACEONTOLOGYDEFINITION {self._definition}"

    @dataclass(frozen=True, eq=True)
    class UpdateStatus:
        """A change that publishes or unpublishes an ontology."""

        _status: str = field(metadata=config(field_name="status"))

        def __post_init__(self) -> None:
            Precondition.check_argument(
                self._status is not None, "Status must not be null"
            )
            Precondition.check_argument(
                self._status in Ontology.Status.ALL,
                f"Unknown ontology status: {self._status}",
            )

        @property
        def status(self) -> str:
            """The new status of the ontology."""
            return self._status

        def __str__(self) -> str:
            return f"UPDATEONTOLOGYSTATUS {self._status}"
