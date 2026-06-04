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


from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import ClassVar, Optional

from gravitino.api.rel.expressions.named_reference import (
    PARTITION_NAME_FIELD,
)
from gravitino.api.rel.expressions.sorts.sort_direction import SortDirection
from gravitino.api.rel.expressions.sorts.sort_orders import SortOrder, SortOrders
from gravitino.utils.precondition import Precondition


@dataclass(frozen=True)
class PartitionRange:
    DEFAULT_COMPARATOR: ClassVar[SortOrder] = SortOrders.of(
        PARTITION_NAME_FIELD, SortDirection.ASCENDING
    )
    _lower_partition_name: Optional[str] = None
    _lower_bound_type: Optional[BoundType] = None
    _upper_partition_name: Optional[str] = None
    _upper_bound_type: Optional[BoundType] = None
    _comparator: SortOrder = DEFAULT_COMPARATOR

    class BoundType(Enum):
        """Enum representing the type of bounds for a partition range."""

        OPEN = "OPEN"  # exclusive
        CLOSED = "CLOSED"  # inclusive

    @classmethod
    def up_to(
        cls,
        upper_partition_name: str,
        upper_bound_type: BoundType,
        comparator: Optional[SortOrder] = None,
    ) -> "PartitionRange":
        """
        Creates a PartitionRange which only has upper bound partition name with a specific comparator type.

        Args:
            upper_partition_name (str): the upper partition name.
            upper_bound_type (BoundType): the type of the upper bound (open or closed).
            comparator (SortOrder): the comparator to use for this range.

        Returns:
            PartitionRange: a PartitionRange with the upper partition name and the specified comparator type.
        """
        Precondition.check_argument(
            upper_partition_name is not None,
            "Upper partition name cannot be null",
        )
        Precondition.check_argument(
            upper_bound_type is not None,
            "Upper bound type cannot be null",
        )
        Precondition.check_argument(
            upper_partition_name.strip() != "",
            "Upper partition name cannot be empty",
        )
        if comparator is None:
            comparator = cls.DEFAULT_COMPARATOR

        return PartitionRange(
            _upper_partition_name=upper_partition_name,
            _upper_bound_type=upper_bound_type,
            _comparator=comparator,
        )

    @classmethod
    def down_to(
        cls,
        lower_partition_name: str,
        lower_bound_type: BoundType,
        comparator: Optional[SortOrder] = None,
    ) -> "PartitionRange":
        """
        Creates a PartitionRange which only has lower bound partition name with a specific comparator type.

        Args:
            lower_partition_name (str): the lower partition name.
            lower_bound_type (BoundType): the type of the lower bound (open or closed).
            comparator (Optional[SortOrder], optional): the comparator to use for this range.

        Returns:
            PartitionRange: a PartitionRange with the lower partition name and the specified comparator type.
        """
        Precondition.check_argument(
            lower_partition_name is not None,
            "Lower partition name cannot be null",
        )
        Precondition.check_argument(
            lower_bound_type is not None,
            "Lower bound type cannot be null",
        )
        Precondition.check_argument(
            lower_partition_name.strip() != "",
            "Lower partition name cannot be empty",
        )
        if comparator is None:
            comparator = cls.DEFAULT_COMPARATOR

        return PartitionRange(
            _lower_partition_name=lower_partition_name,
            _lower_bound_type=lower_bound_type,
            _comparator=comparator,
        )

    @classmethod
    def between(
        cls,
        lower_partition_name: str,
        lower_bound_type: BoundType,
        upper_partition_name: str,
        upper_bound_type: BoundType,
        comparator: Optional[SortOrder] = None,
    ) -> "PartitionRange":
        """
        Creates a PartitionRange which has both lower and upper partition names with a specific comparator type.

        Args:
            lower_partition_name (str): the lower partition name.
            lower_bound_type (BoundType): the type of the lower bound (open or closed).
            upper_partition_name (str):  the upper partition name.
            upper_bound_type (BoundType): the type of the upper bound (open or closed).
            comparator (Optional[SortOrder], optional): the comparator to use for this range.

        Returns:
            PartitionRange: a PartitionRange with both lower and upper partition names
            and the specified comparator type.
        """
        Precondition.check_argument(
            lower_partition_name is not None,
            "Lower partition name cannot be null",
        )
        Precondition.check_argument(
            lower_bound_type is not None,
            "Lower bound type cannot be null",
        )
        Precondition.check_argument(
            lower_partition_name.strip() != "",
            "Lower partition name cannot be empty",
        )

        Precondition.check_argument(
            upper_partition_name is not None,
            "Upper partition name cannot be null",
        )
        Precondition.check_argument(
            upper_bound_type is not None,
            "Upper bound type cannot be null",
        )
        Precondition.check_argument(
            upper_partition_name.strip() != "",
            "Upper partition name cannot be empty",
        )

        if comparator is None:
            comparator = cls.DEFAULT_COMPARATOR

        return PartitionRange(
            _lower_partition_name=lower_partition_name,
            _lower_bound_type=lower_bound_type,
            _upper_partition_name=upper_partition_name,
            _upper_bound_type=upper_bound_type,
            _comparator=comparator,
        )

    def lower_partition_name(self) -> Optional[str]:
        """
        Returns the lower partition name if it exists.

        Returns:
            Optional[str]: an Optional containing the lower partition name if it exists, otherwise None.
        """
        return self._lower_partition_name

    def upper_partition_name(self) -> Optional[str]:
        """
        Returns the upper partition name if it exists.

        Returns:
            Optional[str]: an Optional containing the upper partition name if it exists, otherwise None
        """
        return self._upper_partition_name

    def lower_bound_type(self) -> Optional[BoundType]:
        """
        Returns the type of the lower bound if it exists.

        Returns:
            Optional[BoundType]: an Optional containing the lower bound type if it exists, otherwise None.
        """
        return self._lower_bound_type

    def upper_bound_type(self) -> Optional[BoundType]:
        """
        Returns the type of the upper bound if it exists.

        Returns:
            Optional[BoundType]: an Optional containing the upper bound type if it exists, otherwise None.
        """
        return self._upper_bound_type

    def comparator(self) -> SortOrder:
        """
        Returns the comparator for this partition range.

        Returns:
            SortOrder: the comparator for this partition range.
        """
        return self._comparator


# A PartitionRange that includes all partitions.
ALL_PARTITIONS = PartitionRange(_comparator=PartitionRange.DEFAULT_COMPARATOR)
