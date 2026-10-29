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

from gravitino.api.ontology.ontology_concept import OntologyConcept
from gravitino.api.semantic.ai_context import AIContext


class OntologyDefinition(ABC):
    """The concept graph of an ontology, without its name, comment, status or
    Gravitino-specific properties.

    Every concept reference resolves inside this definition, because the Apache
    Ossie ontology document has no place for external references.
    """

    @abstractmethod
    def ai_context(self) -> Optional[AIContext]:
        """The AI context shared by the whole ontology.

        Returns:
            Optional[AIContext]: The AI context, or `None` if it is not set.
        """
        pass

    @abstractmethod
    def requires(self) -> list[str]:
        """The ANSI SQL expressions that constrain the whole ontology.

        Returns:
            list[str]: The expressions, empty if none are set.
        """
        pass

    @abstractmethod
    def concepts(self) -> list[OntologyConcept]:
        """The concepts of the ontology.

        Returns:
            list[OntologyConcept]: The concepts, never empty.
        """
        pass

    @abstractmethod
    def prefixes(self) -> dict[str, str]:
        """The namespace prefixes that abbreviate IRIs.

        Returns:
            dict[str, str]: A map of prefix to IRI, empty if none are set.
        """
        pass
