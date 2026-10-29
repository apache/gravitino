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
from typing import Any, Optional, Union

from gravitino.api.ontology.ontology import Ontology
from gravitino.api.ontology.ontology_change import OntologyChange
from gravitino.api.ontology.ontology_definition import OntologyDefinition


class OntologyOperations(ABC):
    """Interface for the ontology operations of a metalake.

    An ontology in `Ontology.Status.DRAFT` status is returned only to the
    callers who can manage it. For other callers it is omitted from lists, and
    getting it throws `NoSuchOntologyException`.
    """

    @abstractmethod
    def list_ontologies(self) -> list[str]:
        """List the names of the ontologies in the metalake.

        Returns:
            list[str]: The ontology names.

        Raises:
            NoSuchMetalakeException: If the metalake does not exist.
        """
        pass

    @abstractmethod
    def list_ontologies_info(self) -> list[Ontology]:
        """List the ontologies in the metalake, with details.

        Returns:
            list[Ontology]: The ontologies.

        Raises:
            NoSuchMetalakeException: If the metalake does not exist.
        """
        pass

    @abstractmethod
    def get_ontology(self, ontology_name: str) -> Ontology:
        """Get an ontology by its name.

        Args:
            ontology_name (str): The ontology name.

        Returns:
            Ontology: The ontology.

        Raises:
            NoSuchOntologyException: If the ontology does not exist.
        """
        pass

    @abstractmethod
    def create_ontology(
        self,
        ontology_name: str,
        comment: Optional[str],
        properties: Optional[dict[str, str]],
        definition: Union[OntologyDefinition, dict[str, Any]],
    ) -> Ontology:
        """Create an ontology in `Ontology.Status.DRAFT` status.

        Args:
            ontology_name (str): The ontology name.
            comment (Optional[str]): The comment, or `None`.
            properties (Optional[dict[str, str]]): The Gravitino-specific
                properties, or `None`.
            definition (Union[OntologyDefinition, dict[str, Any]]): The concept
                graph of the ontology.

        Returns:
            Ontology: The created ontology.

        Raises:
            OntologyAlreadyExistsException: If an ontology with the same name
                already exists.
            IllegalOntologyException: If the definition is not a valid ontology.
            IllegalArgumentException: If the name is null or blank.
        """
        pass

    @abstractmethod
    def alter_ontology(self, ontology_name: str, *changes: OntologyChange) -> Ontology:
        """Alter an ontology.

        Renaming an ontology does not rename the concepts inside it, so
        references to a concept by name are unaffected; references to the
        ontology by name are not.

        Args:
            ontology_name (str): The ontology name.
            changes (OntologyChange): The changes to apply.

        Returns:
            Ontology: The altered ontology.

        Raises:
            NoSuchOntologyException: If the ontology does not exist.
            OntologyAlreadyExistsException: If the ontology is renamed and an
                ontology with the new name already exists.
            IllegalOntologyException: If a replacement definition is not a valid
                ontology.
            IllegalArgumentException: If the changes cannot be applied.
        """
        pass

    @abstractmethod
    def drop_ontology(self, ontology_name: str) -> bool:
        """Drop an ontology.

        Args:
            ontology_name (str): The ontology name.

        Returns:
            bool: True if the ontology was dropped, False if it did not exist.

        Raises:
            NonEmptyOntologyException: If a mapping or a term reference still
                points at the ontology.
        """
        pass
