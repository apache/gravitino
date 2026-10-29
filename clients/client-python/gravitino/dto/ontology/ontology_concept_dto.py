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

from gravitino.api.ontology.ontology_concept import OntologyConcept
from gravitino.dto.ontology.ontology_dto_utils import normalize_concept_type
from gravitino.dto.ontology.ontology_relationship_dto import OntologyRelationshipDTO
from gravitino.dto.semantic.semantic_dto_utils import is_none
from gravitino.utils.precondition import Precondition


@dataclass_json
@dataclass
class OntologyConceptDTO(
    OntologyConcept
):  # pylint: disable=too-many-instance-attributes
    """Represents one concept of an ontology."""

    _concept: Optional[str] = field(
        default=None, metadata=config(field_name="concept", exclude=is_none)
    )
    _type: Optional[str] = field(
        default=None,
        metadata=config(
            field_name="type",
            encoder=normalize_concept_type,
            decoder=normalize_concept_type,
            exclude=is_none,
        ),
    )
    _description: Optional[str] = field(
        default=None, metadata=config(field_name="description", exclude=is_none)
    )
    _extends: Optional[list[str]] = field(
        default=None, metadata=config(field_name="extends", exclude=is_none)
    )
    _derived_by: Optional[list[str]] = field(
        default=None, metadata=config(field_name="derivedBy", exclude=is_none)
    )
    _identify_by: Optional[list[str]] = field(
        default=None, metadata=config(field_name="identifyBy", exclude=is_none)
    )
    _requires: Optional[list[str]] = field(
        default=None, metadata=config(field_name="requires", exclude=is_none)
    )
    _relationships: Optional[list[OntologyRelationshipDTO]] = field(
        default=None, metadata=config(field_name="relationships", exclude=is_none)
    )
    _iri: Optional[str] = field(
        default=None, metadata=config(field_name="iri", exclude=is_none)
    )

    def concept(self) -> str:
        """The concept name, unique across the ontology.

        Returns:
            str: The concept name.
        """
        return self._concept

    def type(self) -> str:
        """The concept type.

        Returns:
            str: One of `ConceptType.ENTITY_TYPE` or `ConceptType.VALUE_TYPE`.
        """
        return self._type

    def description(self) -> Optional[str]:
        """The concept description.

        Returns:
            Optional[str]: The description, or `None` if it is not set.
        """
        return self._description

    def extends_concepts(self) -> Optional[list[str]]:
        """The names of the concepts this concept extends.

        Returns:
            Optional[list[str]]: The extended concept names, or `None` if the
                concept extends nothing.
        """
        return self._extends

    def derived_by(self) -> Optional[list[str]]:
        """The ANSI SQL expressions that derive this concept.

        Returns:
            Optional[list[str]]: The expressions, or `None` if they are not set.
        """
        return self._derived_by

    def identify_by(self) -> Optional[list[str]]:
        """The names of the relationships that identify this concept.

        Returns:
            Optional[list[str]]: The identifying relationship names, or `None`
                if they are not set.
        """
        return self._identify_by

    def requires(self) -> Optional[list[str]]:
        """The ANSI SQL expressions that constrain this concept's population.

        Returns:
            Optional[list[str]]: The expressions, or `None` if they are not set.
        """
        return self._requires

    def relationships(self) -> Optional[list[OntologyRelationshipDTO]]:
        """The relationships keyed primarily by this concept.

        Returns:
            Optional[list[OntologyRelationshipDTO]]: The relationships, or
                `None` if the concept has none.
        """
        return self._relationships

    def iri(self) -> Optional[str]:
        """The global identifier of this concept.

        Returns:
            Optional[str]: The IRI, or `None` if it is not set.
        """
        return self._iri

    def validate(self) -> None:
        """Validate the concept DTO.

        Raises:
            IllegalArgumentException: If the concept name or type is blank, or
                if a relationship is invalid.
        """
        Precondition.check_string_not_empty(
            self._concept, '"concept" must not be blank'
        )
        Precondition.check_string_not_empty(self._type, '"type" must not be blank')
        if self._relationships is None:
            return
        for relationship in self._relationships:
            Precondition.check_argument(
                relationship is not None, "relationships must not contain null"
            )
            relationship.validate()
