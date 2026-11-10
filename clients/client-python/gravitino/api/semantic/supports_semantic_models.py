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

from gravitino.name_identifier import NameIdentifier


class SupportsSemanticModels(ABC):
    """Source-scoped discovery of datasets directly associated with tables or Views."""

    @abstractmethod
    def list_semantic_model_datasets(self) -> list[NameIdentifier]:
        """List datasets from models the caller can load in the source's metalake.

        Identifiers contain [metalake, catalog, schema, semanticModel] as their
        namespace and the exact dataset name, including any literal dots. Results
        preserve server ordering: namespace components followed by dataset name,
        comparing each component case-sensitively by UTF-16 code units.
        For a returned dataset, ``NameIdentifier.of(*dataset.namespace().levels())``
        identifies its containing model.

        Associations reflect the latest definition write, not execution validity
        or transitive dependencies.

        Returns:
            Fully qualified dataset identifiers, or an empty list.

        Raises:
            NoSuchMetadataObjectException: If the source does not exist.
        """
        raise NotImplementedError
