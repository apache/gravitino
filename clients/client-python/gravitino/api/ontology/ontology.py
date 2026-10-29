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

from abc import abstractmethod
from typing import ClassVar, Optional

from gravitino.api.auditable import Auditable
from gravitino.api.ontology.ontology_definition import OntologyDefinition


class Ontology(Auditable):
    """A metalake-level ontology: an Apache Ossie ontology document that
    declares the concepts of a business and the relationships between them.

    An ontology is a machine-readable graph, not a glossary. A concept has a
    type from the closed `ConceptType` set and relationships with cardinality,
    but no definition, synonyms, owner or approval state; those belong to the
    glossary's term object, which can reference the concepts here.

    Ontologies are addressed by name alone within a metalake. A new ontology is
    in `Status.DRAFT` status and is returned only to the callers who can manage
    it until it is published.
    """

    PROPERTY_OSSIE_VERSION: ClassVar[str] = "ossie-version"
    """The property key for the Apache Ossie document version."""

    class Status:
        """The lifecycle status of an ontology."""

        DRAFT: ClassVar[str] = "DRAFT"
        """The ontology is returned only to the callers who can manage it."""

        PUBLISHED: ClassVar[str] = "PUBLISHED"
        """The ontology is returned to every caller who can use it."""

        ALL: ClassVar[tuple[str, ...]] = (DRAFT, PUBLISHED)
        """All the status values, used to validate user input."""

    @abstractmethod
    def name(self) -> str:
        """The name of the ontology.

        Returns:
            str: The ontology name.
        """
        pass

    @abstractmethod
    def comment(self) -> Optional[str]:
        """The comment of the ontology.

        Returns:
            Optional[str]: The comment, or `None` if it is not set.
        """
        pass

    @abstractmethod
    def properties(self) -> dict[str, str]:
        """The Gravitino-specific properties of the ontology.

        Returns:
            dict[str, str]: The properties, empty if none are set.
        """
        pass

    @abstractmethod
    def status(self) -> str:
        """The status of the ontology.

        Returns:
            str: One of `Ontology.Status.DRAFT` or `Ontology.Status.PUBLISHED`.
        """
        pass

    @abstractmethod
    def definition(self) -> OntologyDefinition:
        """The immutable concept graph of the ontology.

        Returns:
            OntologyDefinition: The ontology definition.
        """
        pass
