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

from typing import ClassVar

from gravitino.api.expressions.expression import Expression
from gravitino.api.expressions.sorts.null_ordering import NullOrdering
from gravitino.api.expressions.sorts.sort_direction import SortDirection
from gravitino.api.expressions.sorts.sort_order import SortOrder
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.expressions.function_arg import FunctionArg


class SortOrderDTO(SortOrder):
    """Data Transfer Object for SortOrder.

    Attributes:
        EMPTY_SORT (list[SortOrderDTO]):
            An empty array of SortOrderDTO.
    """

    EMPTY_SORT: ClassVar[list["SortOrderDTO"]] = []

    def __init__(
        self,
        sort_term: FunctionArg,
        direction: SortDirection,
        null_ordering: NullOrdering,
    ):
        self._sort_term = sort_term
        self._direction = direction
        self._null_ordering = null_ordering

    def sort_term(self) -> FunctionArg:
        """Returns the sort term.

        Returns:
            FunctionArg: The sort term.
        """
        return self._sort_term

    def expression(self) -> Expression:
        return self._sort_term

    def direction(self):
        return self._direction

    def null_ordering(self):
        return self._null_ordering

    def validate(self, columns: list[ColumnDTO]) -> None:
        """Validates the sort order.

        Args:
            columns (list[ColumnDTO]):
                The column DTOs to validate against.

        Raises:
            IllegalArgumentException:
                If the sort order is invalid, this exception is thrown.
        """
        self._sort_term.validate(columns)
