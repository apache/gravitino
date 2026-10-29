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

from dataclasses_json import config, dataclass_json

from gravitino.api.ontology.ontology_relationship import OntologyRelationship
from gravitino.dto.ontology.ontology_dto_utils import normalize_multiplicity
from gravitino.dto.ontology.ontology_role_dto import OntologyRoleDTO
from gravitino.dto.semantic.semantic_dto_utils import is_none
from gravitino.utils.precondition import Precondition


@dataclass_json
@dataclass
class OntologyRelationshipDTO(
    OntologyRelationship
):  # pylint: disable=too-many-instance-attributes
    """Represents one relationship of an ontology concept."""

    _name: Optional[str] = field(
        default=None, metadata=config(field_name="name", exclude=is_none)
    )
    _description: Optional[str] = field(
        default=None, metadata=config(field_name="description", exclude=is_none)
    )
    _verbalizes: Optional[list[str]] = field(
        default=None, metadata=config(field_name="verbalizes", exclude=is_none)
    )
    _roles: Optional[list[OntologyRoleDTO]] = field(
        default=None, metadata=config(field_name="roles", exclude=is_none)
    )
    _multiplicity: Optional[str] = field(
        default=None,
        metadata=config(
            field_name="multiplicity",
            encoder=normalize_multiplicity,
            decoder=normalize_multiplicity,
            exclude=is_none,
        ),
    )
    _derived_by: Optional[list[str]] = field(
        default=None, metadata=config(field_name="derivedBy", exclude=is_none)
    )
    _requires: Optional[list[str]] = field(
        default=None, metadata=config(field_name="requires", exclude=is_none)
    )
    _iri: Optional[str] = field(
        default=None, metadata=config(field_name="iri", exclude=is_none)
    )
    _id: Optional[str] = field(
        default=None, metadata=config(field_name="id", exclude=is_none)
    )

    def id(self) -> Optional[str]:
        """The stable identifier Gravitino minted for this relationship.

        Returns:
            Optional[str]: The identifier, or `None` when the relationship has
                not been stored yet.
        """
        return self._id

    def name(self) -> str:
        """The relationship name, unique across the ontology.

        Returns:
            str: The relationship name.
        """
        return self._name

    def verbalizes(self) -> list[str]:
        """The natural-language phrasings of this relationship.

        Returns:
            list[str]: The phrasings, empty if the response omitted them.
        """
        return self._verbalizes if self._verbalizes is not None else []

    def description(self) -> Optional[str]:
        """The relationship description.

        Returns:
            Optional[str]: The description, or `None` if it is not set.
        """
        return self._description

    def roles(self) -> Optional[list[OntologyRoleDTO]]:
        """The roles this relationship connects.

        Returns:
            Optional[list[OntologyRoleDTO]]: The roles, or `None` if the
                relationship has none.
        """
        return self._roles

    def multiplicity(self) -> Optional[str]:
        """The cardinality of this relationship.

        Returns:
            Optional[str]: One of `Multiplicity.MANY_TO_ONE` or
                `Multiplicity.ONE_TO_ONE`, or `None` if it is unconstrained.
        """
        return self._multiplicity

    def derived_by(self) -> Optional[list[str]]:
        """The ANSI SQL expressions that derive this relationship.

        Returns:
            Optional[list[str]]: The expressions, or `None` if they are not set.
        """
        return self._derived_by

    def requires(self) -> Optional[list[str]]:
        """The ANSI SQL expressions that constrain this relationship.

        Returns:
            Optional[list[str]]: The expressions, or `None` if they are not set.
        """
        return self._requires

    def iri(self) -> Optional[str]:
        """The global identifier of this relationship.

        Returns:
            Optional[str]: The IRI, or `None` if it is not set.
        """
        return self._iri

    def validate(self) -> None:
        """Validate the relationship DTO.

        Raises:
            IllegalArgumentException: If the name or a phrasing is blank, or if
                `verbalizes` is empty.
        """
        Precondition.check_string_not_empty(self._name, '"name" must not be blank')
        Precondition.check_argument(
            self._verbalizes is not None and len(self._verbalizes) > 0,
            '"verbalizes" must have at least one entry',
        )
        for verbalize in self._verbalizes:
            Precondition.check_string_not_empty(
                verbalize, '"verbalizes" must not contain a blank entry'
            )
        if self._roles is None:
            return
        for role in self._roles:
            Precondition.check_argument(role is not None, "roles must not contain null")
            role.validate()
