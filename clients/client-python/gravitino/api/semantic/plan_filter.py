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

from gravitino.api.semantic.plan_field_ref import PlanFieldRef
from gravitino.api.semantic.semantic_query_filter import SemanticQueryFilter
from gravitino.utils.precondition import Precondition


class PlanFilter:
    """A resolved filter in a `SemanticQueryPlan`."""

    def __init__(
        self,
        field: PlanFieldRef,
        operator: SemanticQueryFilter.Operator,
        value: str,
    ):
        Precondition.check_argument(field is not None, "field must not be null")
        Precondition.check_argument(
            isinstance(operator, SemanticQueryFilter.Operator),
            "operator must not be null",
        )
        Precondition.check_argument(value is not None, "value must not be null")
        self._field = field
        self._operator = operator
        self._value = value

    @staticmethod
    def of(
        field: PlanFieldRef,
        operator: SemanticQueryFilter.Operator,
        value: str,
    ) -> "PlanFilter":
        """Create a plan filter."""
        return PlanFilter(field, operator, value)

    def field(self) -> PlanFieldRef:
        """Returns the filtered field."""
        return self._field

    def operator(self) -> SemanticQueryFilter.Operator:
        """Returns the comparison operator."""
        return self._operator

    def value(self) -> str:
        """Returns the compared value as untyped text."""
        return self._value

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PlanFilter):
            return False
        return (
            self._field == other.field()
            and self._operator == other.operator()
            and self._value == other.value()
        )

    def __hash__(self) -> int:
        return hash((self._field, self._operator, self._value))

    def __repr__(self) -> str:
        return (
            f"PlanFilter(field={self._field!r}, operator={self._operator}, "
            f"value={self._value!r})"
        )
