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

from abc import ABC, abstractmethod
from typing import Optional

from gravitino.api.semantic.semantic_model import SemanticModel
from gravitino.api.semantic.semantic_model_change import SemanticModelChange
from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition
from gravitino.exceptions.base import NoSuchSemanticModelException
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace


class SemanticModelCatalog(ABC):
    """The `SemanticModelCatalog` interface defines the public API for managing
    Semantic Models in a schema.

    Semantic Models are always managed by Gravitino, so this support does not
    depend on whether the underlying connector implements a semantic-model
    capability.
    """

    @abstractmethod
    def list_semantic_models(self, namespace: Namespace) -> list[NameIdentifier]:
        """List the Semantic Models in a namespace from the catalog.

        Identifiers rather than complete definitions are returned, so listing a
        schema does not transfer every model body.

        Args:
            namespace (Namespace): A schema namespace.

        Returns:
            list[NameIdentifier]: The Semantic Model identifiers in the namespace.

        Raises:
            NoSuchSchemaException: If the schema does not exist.
        """

    @abstractmethod
    def load_semantic_model(self, identifier: NameIdentifier) -> SemanticModel:
        """Load Semantic Model metadata by `NameIdentifier` from the catalog.

        Args:
            identifier (NameIdentifier): A Semantic Model identifier.

        Returns:
            SemanticModel: The Semantic Model metadata.

        Raises:
            NoSuchSemanticModelException: If the Semantic Model does not exist.
        """

    def semantic_model_exists(self, identifier: NameIdentifier) -> bool:
        """Check if a Semantic Model with the given name exists in the catalog.

        Args:
            identifier (NameIdentifier): A Semantic Model identifier.

        Returns:
            bool: `True` if the Semantic Model exists, `False` otherwise.
        """
        try:
            self.load_semantic_model(identifier)
            return True
        except NoSuchSemanticModelException:
            return False

    @abstractmethod
    def create_semantic_model(
        self,
        identifier: NameIdentifier,
        comment: Optional[str],
        definition: SemanticModelDefinition,
        properties: Optional[dict[str, str]] = None,
    ) -> SemanticModel:
        """Create a Semantic Model in the catalog.

        Args:
            identifier (NameIdentifier):
                A Semantic Model identifier.
            comment (str, optional):
                The Semantic Model comment.
            definition (SemanticModelDefinition):
                The complete Ossie-compatible definition.
            properties (dict[str, str], optional):
                The Gravitino-specific properties. Defaults to `None`.

        Returns:
            SemanticModel: The created Semantic Model metadata.

        Raises:
            NoSuchSchemaException:
                If the schema does not exist.
            SemanticModelAlreadyExistsException:
                If the Semantic Model already exists.
            IllegalSemanticModelException:
                If the definition is invalid.
        """

    @abstractmethod
    def alter_semantic_model(
        self, identifier: NameIdentifier, *changes: SemanticModelChange
    ) -> SemanticModel:
        """Alter a Semantic Model in the catalog.

        All changes are applied atomically to the current Semantic Model.

        Args:
            identifier (NameIdentifier): A Semantic Model identifier.
            *changes: The Semantic Model changes to apply.

        Returns:
            SemanticModel: The updated Semantic Model metadata.

        Raises:
            NoSuchSemanticModelException:
                If the Semantic Model does not exist.
            SemanticModelAlreadyExistsException:
                If a rename targets an existing Semantic Model name.
            IllegalSemanticModelException:
                If the resulting definition is invalid.
        """

    @abstractmethod
    def drop_semantic_model(self, identifier: NameIdentifier) -> bool:
        """Drop a Semantic Model from the catalog.

        Args:
            identifier (NameIdentifier): A Semantic Model identifier.

        Returns:
            bool:
                `True` if the Semantic Model is dropped, `False` if it does not exist.
        """
