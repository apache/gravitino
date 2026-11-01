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

from gravitino.api.semantic.plan_selection import PlanSelection
from gravitino.dto.semantic.json_serdes.semantic_json_mixin import SemanticJsonMixin
from gravitino.dto.semantic.plan_field_ref_dto import PlanFieldRefDTO
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.utils.precondition import Precondition


@dataclass
class PlanSelectionDTO(SemanticJsonMixin):
    """Represents a selected metric or dimension in a query plan."""

    _name: Optional[str] = field(default=None, metadata=config(field_name="name"))
    _role: Optional[str] = field(default=None, metadata=config(field_name="role"))
    _aggregate: Optional[bool] = field(
        default=None, metadata=config(field_name="aggregate")
    )
    _field: Optional[PlanFieldRefDTO] = field(
        default=None, metadata=config(field_name="field")
    )

    def validate(self) -> None:
        """Validates the selection.

        Raises:
            IllegalArgumentException: If the selection is incomplete or inconsistent.
        """
        Precondition.check_string_not_empty(
            self._name, '"name" field is required and cannot be empty'
        )
        role = self._parse_role()
        Precondition.check_argument(
            self._aggregate is not None, '"aggregate" field is required'
        )
        Precondition.check_argument(
            self._aggregate == (role == PlanSelection.Role.METRIC),
            '"aggregate" must be true exactly for METRIC selections',
        )
        Precondition.check_argument(
            self._field is not None, '"field" field is required'
        )
        self._field.validate()

    def to_plan_selection(self) -> PlanSelection:
        """Converts this DTO to an API plan selection."""
        self.validate()
        return PlanSelection.of(
            self._name,
            self._parse_role(),
            self._aggregate,
            self._field.to_plan_field_ref(),
        )

    @staticmethod
    def from_plan_selection(selection: PlanSelection) -> "PlanSelectionDTO":
        """Creates a DTO from an API plan selection."""
        return PlanSelectionDTO(
            _name=selection.name(),
            _role=selection.role().value,
            _aggregate=selection.aggregate(),
            _field=PlanFieldRefDTO.from_plan_field_ref(selection.field()),
        )

    def _parse_role(self) -> PlanSelection.Role:
        try:
            return PlanSelection.Role[self._role.upper()]
        except (KeyError, AttributeError) as e:
            raise IllegalArgumentException(
                f"Unsupported selection role '{self._role}'"
            ) from e
