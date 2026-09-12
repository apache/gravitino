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

from abc import abstractmethod
from typing import Optional

from gravitino.api.auditable import Auditable
from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition


class SemanticModel(Auditable):
    """A schema-scoped analytical semantic model compatible with Apache Ossie Core.

    A Semantic Model is always managed by Gravitino and is never persisted in an
    underlying catalog. It is a separate metadata type and lifecycle from the
    Gravitino model, which represents an ML model artifact.
    """

    @abstractmethod
    def name(self) -> str:
        """Returns the Semantic Model name."""

    def comment(self) -> Optional[str]:
        """Returns the Semantic Model comment, `None` if it is not set."""
        return None

    @abstractmethod
    def definition(self) -> SemanticModelDefinition:
        """Returns the Ossie-compatible Semantic Model definition."""

    def properties(self) -> dict[str, str]:
        """Returns the Gravitino-specific Semantic Model properties."""
        return {}
