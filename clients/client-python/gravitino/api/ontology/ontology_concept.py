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
from typing import Optional

from gravitino.api.ontology.ontology_relationship import OntologyRelationship


class OntologyConcept(ABC):
    """One concept of an ontology.

    A concept is identified by its name within the ontology. The optional IRI
    gives it a global identity that outlives Gravitino.
    """

    @abstractmethod
    def concept(self) -> str:
        """The concept name, unique across the ontology.

        Returns:
            str: The concept name.
        """
        pass

    @abstractmethod
    def type(self) -> str:
        """The concept type.

        Returns:
            str: One of `ConceptType.ENTITY_TYPE` or `ConceptType.VALUE_TYPE`.
        """
        pass

    @abstractmethod
    def description(self) -> Optional[str]:
        """The concept description.

        Returns:
            Optional[str]: The description, or `None` if it is not set.
        """
        pass

    @abstractmethod
    def extends_concepts(self) -> Optional[list[str]]:
        """The names of the concepts this concept extends.

        Returns:
            Optional[list[str]]: The extended concept names, or `None` if the
                concept extends nothing.
        """
        pass

    @abstractmethod
    def derived_by(self) -> Optional[list[str]]:
        """The ANSI SQL expressions that derive this concept.

        Returns:
            Optional[list[str]]: The expressions, or `None` if they are not set.
        """
        pass

    @abstractmethod
    def identify_by(self) -> Optional[list[str]]:
        """The names of the relationships that identify this concept.

        Returns:
            Optional[list[str]]: The identifying relationship names, or `None`
                if they are not set.
        """
        pass

    @abstractmethod
    def requires(self) -> Optional[list[str]]:
        """The ANSI SQL expressions that constrain this concept's population.

        Returns:
            Optional[list[str]]: The expressions, or `None` if they are not set.
        """
        pass

    @abstractmethod
    def relationships(self) -> Optional[list[OntologyRelationship]]:
        """The relationships keyed primarily by this concept.

        Returns:
            Optional[list[OntologyRelationship]]: The relationships, or `None`
                if the concept has none.
        """
        pass

    @abstractmethod
    def iri(self) -> Optional[str]:
        """The global identifier of this concept.

        Returns:
            Optional[str]: A full IRI or a QName resolved against the ontology's
                prefixes, or `None` if it is not set.
        """
        pass
