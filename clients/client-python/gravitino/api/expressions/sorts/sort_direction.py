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
from gravitino.api.expressions.sorts.null_ordering import NullOrdering


class SortDirection(Enum):
    """A sort direction used in sorting expressions.
    Each direction has a default null ordering that is implied if no null ordering is specified explicitly.
    """

    ASCENDING = ("asc", NullOrdering.NULLS_FIRST)
    """Ascending sort direction. Nulls appear first. For ascending order, this means nulls appear at the beginning."""

    DESCENDING = ("desc", NullOrdering.NULLS_LAST)
    """Descending sort direction. Nulls appear last. For ascending order, this means nulls appear at the end."""

    def __init__(self, direction: str, default_null_ordering: NullOrdering):
        self._direction = direction
        self._default_null_ordering = default_null_ordering

    def direction(self) -> str:
        return self._direction

    def default_null_ordering(self) -> NullOrdering:
        """
        Returns the default null ordering to use if no null ordering is specified explicitly.

        Returns:
            NullOrdering: The default null ordering.
        """
        return self._default_null_ordering

    def __str__(self) -> str:
        if self == SortDirection.ASCENDING:
            return SortDirection.ASCENDING.direction()
        if self == SortDirection.DESCENDING:
            return SortDirection.DESCENDING.direction()

        raise ValueError(f"Unexpected sort direction: {self}")

    @staticmethod
    def from_string(direction: str):
        """
        Returns the SortDirection from the string representation.

        Args:
            direction: The string representation of the sort direction.

        Returns:
            SortDirection: The corresponding SortDirection.
        """
        direction = direction.lower()
        if direction == SortDirection.ASCENDING.direction():
            return SortDirection.ASCENDING
        if direction == SortDirection.DESCENDING.direction():
            return SortDirection.DESCENDING

        raise ValueError(f"Unexpected sort direction: {direction}")
