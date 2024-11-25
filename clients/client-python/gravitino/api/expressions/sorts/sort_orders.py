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

from typing import Optional, List

from gravitino.api.expressions.expression import Expression
from gravitino.api.expressions.sorts.null_ordering import NullOrdering
from gravitino.api.expressions.sorts.sort_direction import SortDirection
from gravitino.api.expressions.sorts.sort_order import SortOrder


class SortImpl(SortOrder):
    """Create a sort order by the given expression with the given sort direction and null ordering."""

    _expression: Expression
    _direction: SortDirection
    _null_ordering: NullOrdering

    def __init__(
        self, expression, direction: SortDirection, null_ordering: NullOrdering
    ):
        self._expression = expression
        self._direction = direction
        self._null_ordering = null_ordering

    def expression(self):
        return self._expression

    def direction(self):
        return self._direction

    def null_ordering(self):
        return self._null_ordering

    def __eq__(self, other):
        if not isinstance(other, SortImpl):
            return False
        return (
            self._expression == other._expression
            and self._direction == other._direction
            and self._null_ordering == other._null_ordering
        )

    def __hash__(self):
        return hash((self._expression, self._direction, self._null_ordering))

    def __str__(self):
        return (
            f"SortImpl(expression={self._expression}, "
            f"direction={self._direction}, "
            f"nullOrdering={self._null_ordering})"
        )


# Helper class to create SortOrder instances
class SortOrders:
    NONE: List[SortOrder] = []

    @staticmethod
    def ascending(expression) -> SortImpl:
        return SortOrders.of(expression, SortDirection.ASCENDING)

    @staticmethod
    def descending(expression) -> SortImpl:
        return SortOrders.of(expression, SortDirection.DESCENDING)

    @staticmethod
    def of(
        expression,
        direction: SortDirection,
        null_ordering: Optional[NullOrdering] = None,
    ) -> SortImpl:
        if null_ordering is None:
            null_ordering = direction.default_null_ordering()
        return SortImpl(expression, direction, null_ordering)
