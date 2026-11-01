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

from enum import Enum

from gravitino.api.semantic.plan_field_ref import PlanFieldRef
from gravitino.utils.precondition import Precondition


class PlanSelection:
    """A selected metric or dimension in a `SemanticQueryPlan`."""

    class Role(Enum):
        """The role of a selection."""

        METRIC = "METRIC"
        DIMENSION = "DIMENSION"

    def __init__(
        self,
        name: str,
        role: "PlanSelection.Role",
        aggregate: bool,
        field: PlanFieldRef,
    ):
        Precondition.check_string_not_empty(
            name, "selection name must not be null or blank"
        )
        Precondition.check_argument(
            isinstance(role, PlanSelection.Role), "role must not be null"
        )
        Precondition.check_argument(field is not None, "field must not be null")
        Precondition.check_argument(
            aggregate == (role == PlanSelection.Role.METRIC),
            "aggregate must be true exactly for METRIC selections",
        )
        self._name = name
        self._role = role
        self._aggregate = aggregate
        self._field = field

    @staticmethod
    def of(
        name: str,
        role: "PlanSelection.Role",
        aggregate: bool,
        field: PlanFieldRef,
    ) -> "PlanSelection":
        """Create a plan selection."""
        return PlanSelection(name, role, aggregate, field)

    def name(self) -> str:
        """Returns the metric or field name as requested."""
        return self._name

    def role(self) -> "PlanSelection.Role":
        """Returns the selection role."""
        return self._role

    def aggregate(self) -> bool:
        """Returns whether this selection aggregates."""
        return self._aggregate

    def field(self) -> PlanFieldRef:
        """Returns the selected field."""
        return self._field

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PlanSelection):
            return False
        return (
            self._name == other.name()
            and self._role == other.role()
            and self._aggregate == other.aggregate()
            and self._field == other.field()
        )

    def __hash__(self) -> int:
        return hash((self._name, self._role, self._aggregate, self._field))

    def __repr__(self) -> str:
        return (
            f"PlanSelection(name={self._name!r}, role={self._role}, "
            f"aggregate={self._aggregate}, field={self._field!r})"
        )
