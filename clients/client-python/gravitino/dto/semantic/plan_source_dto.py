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

from dataclasses_json import config

from gravitino.api.semantic.plan_source import PlanSource
from gravitino.dto.semantic.json_serdes.semantic_json_mixin import SemanticJsonMixin
from gravitino.name_identifier import NameIdentifier
from gravitino.utils.precondition import Precondition


@dataclass
class PlanSourceDTO(SemanticJsonMixin):
    """Represents a dataset required by a Semantic Model query plan."""

    _dataset: Optional[str] = field(default=None, metadata=config(field_name="dataset"))
    _source: Optional[list[str]] = field(
        default=None, metadata=config(field_name="source")
    )

    def validate(self) -> None:
        """Validates the plan source.

        Raises:
            IllegalArgumentException: If the dataset or source is missing.
        """
        Precondition.check_string_not_empty(
            self._dataset, '"dataset" field is required and cannot be empty'
        )
        Precondition.check_argument(
            self._source is not None and len(self._source) > 0,
            '"source" field is required and cannot be empty',
        )
        for index, level in enumerate(self._source):
            Precondition.check_string_not_empty(
                level, f'"source"[{index}] field cannot be null or empty'
            )

    def to_plan_source(self) -> PlanSource:
        """Converts this DTO to an API plan source."""
        self.validate()
        return PlanSource(self._dataset, NameIdentifier.of(*self._source))

    @staticmethod
    def from_plan_source(source: PlanSource) -> "PlanSourceDTO":
        """Creates a DTO from an API plan source."""
        levels = list(source.source().namespace().levels())
        levels.append(source.source().name())
        return PlanSourceDTO(_dataset=source.dataset(), _source=levels)
