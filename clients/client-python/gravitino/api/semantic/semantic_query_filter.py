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

from gravitino.utils.precondition import Precondition


class SemanticQueryFilter:
    """An immutable filter applied to a field in a `SemanticQuery`.

    The value is the literal compared with the field. Planners interpret it
    according to the operator and the target dialect.
    """

    class Operator(Enum):
        """The comparison operator of a filter."""

        EQ = "EQ"
        NE = "NE"
        GT = "GT"
        GE = "GE"
        LT = "LT"
        LE = "LE"

    def __init__(
        self, field: str, operator: "SemanticQueryFilter.Operator", value: str
    ):
        Precondition.check_string_not_empty(
            field, "filter field must not be null or empty"
        )
        Precondition.check_argument(
            isinstance(operator, SemanticQueryFilter.Operator),
            "filter operator must not be null",
        )
        Precondition.check_argument(value is not None, "filter value must not be null")
        self._field = field
        self._operator = operator
        self._value = value

    @staticmethod
    def of(
        field: str, operator: "SemanticQueryFilter.Operator", value: str
    ) -> "SemanticQueryFilter":
        """Create a filter.

        Args:
            field (str): The field name to filter on.
            operator (SemanticQueryFilter.Operator): The comparison operator.
            value (str): The literal value to compare with.

        Returns:
            SemanticQueryFilter: The immutable filter.

        Raises:
            IllegalArgumentException: If the field is empty, the operator is not
                an `Operator`, or the value is `None`.
        """
        return SemanticQueryFilter(field, operator, value)

    def field(self) -> str:
        """Returns the field name to filter on."""
        return self._field

    def operator(self) -> "SemanticQueryFilter.Operator":
        """Returns the comparison operator."""
        return self._operator

    def value(self) -> str:
        """Returns the literal value to compare with."""
        return self._value

    def __eq__(self, other) -> bool:
        if not isinstance(other, SemanticQueryFilter):
            return False
        return (
            self._field == other._field
            and self._operator == other._operator
            and self._value == other._value
        )

    def __hash__(self) -> int:
        return hash((self._field, self._operator, self._value))

    def __repr__(self) -> str:
        return (
            f"SemanticQueryFilter(field={self._field!r}, "
            f"operator={self._operator}, value={self._value!r})"
        )
