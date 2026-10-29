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

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import config, dataclass_json

from gravitino.api.ontology.ontology import Ontology
from gravitino.api.ontology.ontology_change import OntologyChange
from gravitino.dto.ontology.ontology_definition_dto import (
    OntologyDefinitionDTO,
    to_ontology_definition_dto,
)
from gravitino.rest.rest_message import RESTRequest
from gravitino.utils.precondition import Precondition


@dataclass
@dataclass_json
class OntologyUpdateRequestBase(RESTRequest, ABC):
    """The base of one ontology update request."""

    # Gravitino serializes the update discriminator as "@type" for every update
    # request; the ontology wire format follows the same convention.
    _type: str = field(init=False, metadata=config(field_name="@type"))

    @abstractmethod
    def ontology_change(self) -> OntologyChange:
        """Returns the ontology change this request represents.

        Raises:
            NotImplementedError: if the method is not implemented.

        Returns:
            OntologyChange: the ontology change.
        """
        raise NotImplementedError()


class OntologyUpdateRequest:
    """Request to update an ontology."""

    @dataclass_json
    @dataclass
    class RenameOntologyRequest(OntologyUpdateRequestBase):
        """The ontology update request for renaming an ontology."""

        _new_name: str = field(init=True, metadata=config(field_name="newName"))

        def __post_init__(self) -> None:
            self._type = "rename"

        def validate(self) -> None:
            """Validate the request.

            Raises:
                IllegalArgumentException: If the new name is blank.
            """
            Precondition.check_string_not_empty(
                self._new_name, '"newName" must not be blank'
            )

        @property
        def new_name(self) -> str:
            """The new name of the ontology."""
            return self._new_name

        def ontology_change(self) -> OntologyChange.RenameOntology:
            """Returns the ontology change this request represents.

            Returns:
                OntologyChange.RenameOntology: the rename change.
            """
            return OntologyChange.rename(self._new_name)

    @dataclass_json
    @dataclass
    class UpdateOntologyCommentRequest(OntologyUpdateRequestBase):
        """The ontology update request for updating an ontology comment."""

        _new_comment: Optional[str] = field(
            default=None, metadata=config(field_name="newComment")
        )

        def __post_init__(self) -> None:
            self._type = "updateComment"

        def validate(self) -> None:
            """Validate the request."""
            # A null comment clears the current comment, so there is nothing to check.
            pass

        @property
        def new_comment(self) -> Optional[str]:
            """The new comment of the ontology."""
            return self._new_comment

        def ontology_change(self) -> OntologyChange.UpdateComment:
            """Returns the ontology change this request represents.

            Returns:
                OntologyChange.UpdateComment: the comment change.
            """
            return OntologyChange.UpdateComment(self._new_comment)

    @dataclass_json
    @dataclass
    class SetOntologyPropertyRequest(OntologyUpdateRequestBase):
        """The ontology update request for setting an ontology property."""

        _property: str = field(metadata=config(field_name="property"))
        _value: str = field(metadata=config(field_name="value"))

        def __post_init__(self) -> None:
            self._type = "setProperty"

        def validate(self) -> None:
            """Validate the request.

            Raises:
                IllegalArgumentException: If the property name is blank.
            """
            Precondition.check_string_not_empty(
                self._property, '"property" must not be blank'
            )

        @property
        def prop(self) -> str:
            """The property to set."""
            return self._property

        @property
        def value(self) -> str:
            """The value to set."""
            return self._value

        def ontology_change(self) -> OntologyChange.SetProperty:
            """Returns the ontology change this request represents.

            Returns:
                OntologyChange.SetProperty: the property change.
            """
            return OntologyChange.set_property(self._property, self._value)

    @dataclass_json
    @dataclass
    class RemoveOntologyPropertyRequest(OntologyUpdateRequestBase):
        """The ontology update request for removing an ontology property."""

        _property: str = field(metadata=config(field_name="property"))

        def __post_init__(self) -> None:
            self._type = "removeProperty"

        def validate(self) -> None:
            """Validate the request.

            Raises:
                IllegalArgumentException: If the property name is blank.
            """
            Precondition.check_string_not_empty(
                self._property, '"property" must not be blank'
            )

        @property
        def prop(self) -> str:
            """The property to remove."""
            return self._property

        def ontology_change(self) -> OntologyChange.RemoveProperty:
            """Returns the ontology change this request represents.

            Returns:
                OntologyChange.RemoveProperty: the property removal change.
            """
            return OntologyChange.remove_property(self._property)

    @dataclass_json
    @dataclass
    class ReplaceOntologyDefinitionRequest(OntologyUpdateRequestBase):
        """The ontology update request for replacing an ontology definition."""

        _definition: Optional[OntologyDefinitionDTO] = field(
            default=None, metadata=config(field_name="definition")
        )

        def __post_init__(self) -> None:
            self._type = "replaceDefinition"
            if self._definition is not None:
                self._definition = to_ontology_definition_dto(self._definition)

        def validate(self) -> None:
            """Validate the request.

            Raises:
                IllegalArgumentException: If the definition is missing or
                    invalid.
            """
            Precondition.check_argument(
                self._definition is not None, '"definition" must not be null'
            )
            self._definition.validate()

        @property
        def definition(self) -> Optional[OntologyDefinitionDTO]:
            """The replacement definition."""
            return self._definition

        def ontology_change(self) -> OntologyChange.ReplaceDefinition:
            """Returns the ontology change this request represents.

            Returns:
                OntologyChange.ReplaceDefinition: the definition change.
            """
            return OntologyChange.replace_definition(self._definition)

    @dataclass_json
    @dataclass
    class UpdateOntologyStatusRequest(OntologyUpdateRequestBase):
        """The ontology update request for changing the ontology status."""

        _status: str = field(metadata=config(field_name="status"))

        def __post_init__(self) -> None:
            self._type = "updateStatus"

        def validate(self) -> None:
            """Validate the request.

            Raises:
                IllegalArgumentException: If the status is not a known value.
            """
            Precondition.check_argument(
                self._status is not None, '"status" must not be null'
            )
            Precondition.check_argument(
                self._status in Ontology.Status.ALL,
                f'"status" must be one of {Ontology.Status.ALL}, '
                f"got {self._status}",
            )

        @property
        def status(self) -> str:
            """The new status of the ontology."""
            return self._status

        def ontology_change(self) -> OntologyChange.UpdateStatus:
            """Returns the ontology change this request represents.

            Returns:
                OntologyChange.UpdateStatus: the status change.
            """
            return OntologyChange.update_status(self._status)
