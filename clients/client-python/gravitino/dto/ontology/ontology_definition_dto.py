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
from typing import Any, Optional, Union

from dataclasses_json import config, dataclass_json

from gravitino.api.ontology.ontology_concept import OntologyConcept
from gravitino.api.ontology.ontology_definition import OntologyDefinition
from gravitino.api.semantic.ai_context import AIContext
from gravitino.dto.ontology.ontology_concept_dto import OntologyConceptDTO
from gravitino.dto.semantic.ai_context_dto import AIContextDTO
from gravitino.dto.semantic.json_serdes.ai_context_serdes import AIContextSerdes
from gravitino.dto.semantic.semantic_dto_utils import is_none
from gravitino.utils.precondition import Precondition


@dataclass_json
@dataclass
class OntologyDefinitionDTO(OntologyDefinition):
    """Represents the concept graph of an ontology."""

    _ai_context: Optional[AIContextDTO] = field(
        default=None,
        metadata=config(
            field_name="aiContext",
            encoder=AIContextSerdes.serialize,
            decoder=AIContextSerdes.deserialize,
            exclude=is_none,
        ),
    )
    _requires: Optional[list[str]] = field(
        default=None, metadata=config(field_name="requires", exclude=is_none)
    )
    _concepts: Optional[list[OntologyConceptDTO]] = field(
        default=None, metadata=config(field_name="concepts", exclude=is_none)
    )
    _prefixes: Optional[dict[str, str]] = field(
        default=None, metadata=config(field_name="prefixes", exclude=is_none)
    )

    def ai_context(self) -> Optional[AIContext]:
        """The AI context shared by the whole ontology.

        Returns:
            Optional[AIContext]: The AI context, or `None` if it is not set.
        """
        return None if self._ai_context is None else self._ai_context.to_ai_context()

    def requires(self) -> list[str]:
        """The ANSI SQL expressions that constrain the whole ontology.

        Returns:
            list[str]: The expressions, empty if none are set.
        """
        return self._requires if self._requires is not None else []

    def concepts(self) -> list[OntologyConcept]:
        """The concepts of the ontology.

        Returns:
            list[OntologyConcept]: The concepts, empty if none were received.
        """
        return self._concepts if self._concepts is not None else []

    def prefixes(self) -> dict[str, str]:
        """The namespace prefixes that abbreviate IRIs.

        Returns:
            dict[str, str]: A map of prefix to IRI, empty if none are set.
        """
        return self._prefixes if self._prefixes is not None else {}

    def validate(self) -> None:
        """Validate the ontology definition DTO.

        Raises:
            IllegalArgumentException: If no concept is declared, a concept is
                invalid, a `requires` expression is blank or a prefix is blank.
        """
        Precondition.check_argument(
            self._concepts is not None and len(self._concepts) > 0,
            '"concepts" must be set',
        )
        for concept in self._concepts:
            Precondition.check_argument(
                concept is not None, "concepts must not contain null"
            )
            concept.validate()
        if self._requires is not None:
            for expression in self._requires:
                Precondition.check_string_not_empty(
                    expression, '"requires" must not contain a blank expression'
                )
        if self._prefixes is not None:
            for prefix, iri in self._prefixes.items():
                Precondition.check_argument(
                    prefix is not None
                    and prefix.strip() != ""
                    and iri is not None
                    and iri.strip() != "",
                    "prefixes must not contain a blank prefix or IRI",
                )


def to_ontology_definition_dto(
    definition: Union[OntologyDefinition, dict[str, Any]],
) -> OntologyDefinitionDTO:
    """Normalize a replacement or creation definition to its DTO form.

    Args:
        definition (Union[OntologyDefinition, dict[str, Any]]): The ontology
            definition DTO, or the JSON object that carries it.

    Returns:
        OntologyDefinitionDTO: The definition DTO.

    Raises:
        IllegalArgumentException: If the definition is null, or is neither an
            `OntologyDefinitionDTO` nor a dictionary.
    """
    if isinstance(definition, OntologyDefinitionDTO):
        return definition
    Precondition.check_argument(definition is not None, '"definition" must not be null')
    Precondition.check_argument(
        isinstance(definition, dict),
        "definition must be an OntologyDefinitionDTO or a dict",
    )
    return OntologyDefinitionDTO.from_dict(definition)
