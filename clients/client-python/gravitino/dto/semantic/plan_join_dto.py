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

from gravitino.api.semantic.plan_join import PlanJoin
from gravitino.dto.semantic.json_serdes.semantic_json_mixin import SemanticJsonMixin
from gravitino.dto.semantic.semantic_dto_utils import is_none
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.utils.precondition import Precondition


@dataclass
class PlanJoinDTO(SemanticJsonMixin):
    """Represents a join between two datasets of a Semantic Model query plan."""

    _type: Optional[str] = field(default=None, metadata=config(field_name="type"))
    _relationship: Optional[str] = field(
        default=None, metadata=config(field_name="relationship", exclude=is_none)
    )
    _from_dataset: Optional[str] = field(
        default=None, metadata=config(field_name="fromDataset")
    )
    _from_columns: Optional[list[str]] = field(
        default=None, metadata=config(field_name="fromColumns")
    )
    _to_dataset: Optional[str] = field(
        default=None, metadata=config(field_name="toDataset")
    )
    _to_columns: Optional[list[str]] = field(
        default=None, metadata=config(field_name="toColumns")
    )

    def validate(self) -> None:
        """Validates the plan join.

        Raises:
            IllegalArgumentException: If the join is incomplete or inconsistent.
        """
        Precondition.check_string_not_empty(
            self._type, '"type" field is required and cannot be empty'
        )
        self._parse_type()
        Precondition.check_argument(
            self._relationship is None or self._relationship.strip() != "",
            '"relationship" field cannot be empty when present',
        )
        Precondition.check_string_not_empty(
            self._from_dataset, '"fromDataset" field is required and cannot be empty'
        )
        Precondition.check_string_not_empty(
            self._to_dataset, '"toDataset" field is required and cannot be empty'
        )
        Precondition.check_argument(
            self._from_columns is not None and len(self._from_columns) > 0,
            '"fromColumns" field is required and cannot be empty',
        )
        Precondition.check_argument(
            self._to_columns is not None and len(self._to_columns) > 0,
            '"toColumns" field is required and cannot be empty',
        )
        for index, column in enumerate(self._from_columns):
            Precondition.check_string_not_empty(
                column, f'"fromColumns"[{index}] field cannot be null or empty'
            )
        for index, column in enumerate(self._to_columns):
            Precondition.check_string_not_empty(
                column, f'"toColumns"[{index}] field cannot be null or empty'
            )
        Precondition.check_argument(
            len(self._from_columns) == len(self._to_columns),
            '"fromColumns" and "toColumns" must have the same length',
        )

    def to_plan_join(self) -> PlanJoin:
        """Converts this DTO to an API plan join."""
        self.validate()
        return PlanJoin.of(
            self._parse_type(),
            self._relationship,
            self._from_dataset,
            list(self._from_columns),
            self._to_dataset,
            list(self._to_columns),
        )

    @staticmethod
    def from_plan_join(join: PlanJoin) -> "PlanJoinDTO":
        """Creates a DTO from an API plan join."""
        return PlanJoinDTO(
            _type=join.type().value,
            _relationship=join.relationship(),
            _from_dataset=join.from_dataset(),
            _from_columns=join.from_columns(),
            _to_dataset=join.to_dataset(),
            _to_columns=join.to_columns(),
        )

    def _parse_type(self) -> PlanJoin.Type:
        try:
            return PlanJoin.Type[self._type.upper()]
        except KeyError as e:
            raise IllegalArgumentException(
                f"Unsupported join type '{self._type}'"
            ) from e
