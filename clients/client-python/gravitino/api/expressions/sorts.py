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
from abc import ABC, abstractmethod
from typing import Optional


# Enum for NullOrdering
class NullOrdering(Enum):
    NULLS_FIRST = "nulls_first"
    NULLS_LAST = "nulls_last"

    @staticmethod
    def from_string(s: str):
        try:
            return NullOrdering[s.upper()]
        except KeyError as exc:
            raise ValueError(f"Invalid null ordering: {s}") from exc

    def __str__(self):
        return self.value


# Enum for SortDirection
class SortDirection(Enum):
    ASCENDING = "asc"
    DESCENDING = "desc"

    def __init__(self, value):
        self._default_null_ordering = (
            NullOrdering.NULLS_FIRST if value == "asc" else NullOrdering.NULLS_LAST
        )

    def default_null_ordering(self) -> NullOrdering:
        return self._default_null_ordering

    @staticmethod
    def from_string(s: str) -> "SortDirection":
        if s.lower() == "asc":
            return SortDirection.ASCENDING
        if s.lower() == "desc":
            return SortDirection.DESCENDING
        raise ValueError(f"Unexpected sort direction: {s}")

    def __str__(self):
        return self.value


# Abstract base class for SortOrder
class SortOrder(ABC):
    @abstractmethod
    def expression(self):
        pass

    @abstractmethod
    def direction(self):
        pass

    @abstractmethod
    def null_ordering(self):
        pass

    def children(self):
        """
        Returns the child expressions. By default, this is the expression itself in a list.
        """
        return [self.expression()]


# Concrete implementation of SortOrder
class SortImpl(SortOrder):
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
    NONE = []

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

    @staticmethod
    def from_string(
        expression, direction_str: str, null_ordering_str: Optional[str] = None
    ) -> SortImpl:
        direction = SortDirection.from_string(direction_str)
        null_ordering = (
            NullOrdering(null_ordering_str)
            if null_ordering_str
            else direction.default_null_ordering()
        )
        return SortImpl(expression, direction, null_ordering)
