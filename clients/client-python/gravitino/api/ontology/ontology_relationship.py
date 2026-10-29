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

from gravitino.api.ontology.ontology_role import OntologyRole


class OntologyRelationship(ABC):
    """A relationship between concepts in an ontology.

    `verbalizes` is required and holds at least one natural-language phrasing,
    because a relationship an agent cannot phrase is a relationship it cannot
    use to answer a question.
    """

    @abstractmethod
    def name(self) -> str:
        """The relationship name, unique across the ontology.

        Returns:
            str: The relationship name.
        """
        pass

    @abstractmethod
    def verbalizes(self) -> list[str]:
        """The natural-language phrasings of this relationship.

        Returns:
            list[str]: The non-empty phrasings.
        """
        pass

    @abstractmethod
    def description(self) -> Optional[str]:
        """The relationship description.

        Returns:
            Optional[str]: The description, or `None` if it is not set.
        """
        pass

    @abstractmethod
    def roles(self) -> Optional[list[OntologyRole]]:
        """The roles this relationship connects.

        Returns:
            Optional[list[OntologyRole]]: The roles, or `None` if they are not set.
        """
        pass

    @abstractmethod
    def multiplicity(self) -> Optional[str]:
        """The cardinality of this relationship.

        Returns:
            Optional[str]: One of `Multiplicity.MANY_TO_ONE` or
                `Multiplicity.ONE_TO_ONE`, or `None` if the relationship is
                unconstrained.
        """
        pass

    @abstractmethod
    def derived_by(self) -> Optional[list[str]]:
        """The ANSI SQL expressions that derive this relationship.

        Returns:
            Optional[list[str]]: The expressions, or `None` if they are not set.
        """
        pass

    @abstractmethod
    def requires(self) -> Optional[list[str]]:
        """The ANSI SQL expressions that constrain this relationship.

        Returns:
            Optional[list[str]]: The expressions, or `None` if they are not set.
        """
        pass

    @abstractmethod
    def iri(self) -> Optional[str]:
        """The global identifier of this relationship.

        Returns:
            Optional[str]: A full IRI or a QName resolved against the ontology's
                prefixes, or `None` if it is not set.
        """
        pass
