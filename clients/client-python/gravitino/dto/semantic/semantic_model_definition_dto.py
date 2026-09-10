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

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition
from gravitino.dto.semantic.ai_context_dto import AIContextDTO
from gravitino.dto.semantic.custom_extension_dto import CustomExtensionDTO
from gravitino.dto.semantic.dataset_dto import DatasetDTO
from gravitino.dto.semantic.json_serdes.ai_context_serdes import AIContextSerdes
from gravitino.dto.semantic.metric_dto import MetricDTO
from gravitino.dto.semantic.relationship_dto import RelationshipDTO
from gravitino.dto.semantic.semantic_dto_utils import convert_list, is_none


@dataclass
class SemanticModelDefinitionDTO(DataClassJsonMixin):
    """Represents a Semantic Model definition DTO."""

    _ai_context: Optional[AIContextDTO] = field(
        default=None,
        metadata=config(
            field_name="ai_context",
            encoder=AIContextSerdes.serialize,
            decoder=AIContextSerdes.deserialize,
            exclude=is_none,
        ),
    )
    _datasets: Optional[list[DatasetDTO]] = field(
        default=None, metadata=config(field_name="datasets", exclude=is_none)
    )
    _relationships: Optional[list[RelationshipDTO]] = field(
        default=None,
        metadata=config(field_name="relationships", exclude=is_none),
    )
    _metrics: Optional[list[MetricDTO]] = field(
        default=None,
        metadata=config(field_name="metrics", exclude=is_none),
    )
    _custom_extensions: Optional[list[CustomExtensionDTO]] = field(
        default=None,
        metadata=config(field_name="custom_extensions", exclude=is_none),
    )

    def ai_context(self) -> Optional[AIContextDTO]:
        """Returns the model-level AI context, or `None` if it is not set."""
        return self._ai_context

    def datasets(self) -> Optional[list[DatasetDTO]]:
        """Returns the datasets."""
        return self._datasets

    def relationships(self) -> Optional[list[RelationshipDTO]]:
        """Returns the relationships, or `None` if they are not set."""
        return self._relationships

    def metrics(self) -> Optional[list[MetricDTO]]:
        """Returns the metrics, or `None` if they are not set."""
        return self._metrics

    def custom_extensions(self) -> Optional[list[CustomExtensionDTO]]:
        """Returns the custom extensions, or `None` if they are not set."""
        return self._custom_extensions

    @staticmethod
    def from_definition(
        definition: SemanticModelDefinition,
    ) -> "SemanticModelDefinitionDTO":
        """Convert a Semantic Model definition to its DTO."""
        ai_context = definition.ai_context()
        return SemanticModelDefinitionDTO(
            _ai_context=(
                None if ai_context is None else AIContextDTO.from_ai_context(ai_context)
            ),
            _datasets=convert_list(definition.datasets(), DatasetDTO.from_dataset),
            _relationships=convert_list(
                definition.relationships(), RelationshipDTO.from_relationship
            ),
            _metrics=convert_list(definition.metrics(), MetricDTO.from_metric),
            _custom_extensions=convert_list(
                definition.custom_extensions(),
                CustomExtensionDTO.from_custom_extension,
            ),
        )

    def to_definition(self) -> SemanticModelDefinition:
        """Convert this DTO to a Semantic Model definition.

        Raises:
            IllegalArgumentException: If a definition field is invalid.
        """
        return SemanticModelDefinition(
            datasets=convert_list(self._datasets, DatasetDTO.to_dataset),
            ai_context=(
                None if self._ai_context is None else self._ai_context.to_ai_context()
            ),
            relationships=convert_list(
                self._relationships, RelationshipDTO.to_relationship
            ),
            metrics=convert_list(self._metrics, MetricDTO.to_metric),
            custom_extensions=convert_list(
                self._custom_extensions, CustomExtensionDTO.to_custom_extension
            ),
        )
