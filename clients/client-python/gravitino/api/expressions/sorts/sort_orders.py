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
from typing import List

from gravitino.api.expressions.expression import Expression
from gravitino.api.expressions.sorts.null_ordering import NullOrdering
from gravitino.api.expressions.sorts.sort_direction import SortDirection
from gravitino.api.expressions.sorts.sort_order import SortOrder


class SortImpl(SortOrder):

    def __init__(
        self,
        expression: Expression,
        direction: SortDirection,
        null_ordering: NullOrdering,
    ):
        """Initialize the SortImpl object."""
        self._expression = expression
        self._direction = direction
        self._null_ordering = null_ordering

    def expression(self) -> Expression:
        return self._expression

    def direction(self) -> SortDirection:
        return self._direction

    def null_ordering(self) -> NullOrdering:
        return self._null_ordering

    def __eq__(self, other: object) -> bool:
        """Check if two SortImpl instances are equal."""
        if not isinstance(other, SortImpl):
            return False
        return (
            self.expression() == other.expression()
            and self.direction() == other.direction()
            and self.null_ordering() == other.null_ordering()
        )

    def __hash__(self) -> int:
        """Generate a hash for a SortImpl instance."""
        return hash((self.expression(), self.direction(), self.null_ordering()))

    def __str__(self) -> str:
        """Provide a string representation of the SortImpl object."""
        return (
            f"SortImpl(expression={self._expression}, "
            f"direction={self._direction}, null_ordering={self._null_ordering})"
        )


class SortOrders:
    """Helper methods to create SortOrders to pass into Apache Gravitino."""

    # NONE is used to indicate that there is no sort order
    NONE: List[SortOrder] = []

    @staticmethod
    def ascending(expression: Expression) -> SortImpl:
        """Creates a sort order with ascending direction and nulls first."""
        return SortOrders.of(expression, SortDirection.ASCENDING)

    @staticmethod
    def descending(expression: Expression) -> SortImpl:
        """Creates a sort order with descending direction and nulls last."""
        return SortOrders.of(expression, SortDirection.DESCENDING)

    @staticmethod
    def of(
        expression: Expression,
        direction: SortDirection,
        null_ordering: NullOrdering = None,
    ) -> SortImpl:
        """Creates a sort order with the given direction and optionally specified null ordering."""
        if null_ordering is None:
            null_ordering = direction.default_null_ordering()
        return SortImpl(expression, direction, null_ordering)
