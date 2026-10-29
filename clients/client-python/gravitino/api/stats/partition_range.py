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
from typing import ClassVar

from gravitino.utils.precondition import Precondition


@dataclass(frozen=True)
class PartitionRange:
    """A range of partition names using the server's ordering.

    Bounds are explicit; omitted names represent unbounded sides. No comparator
    or date interpretation is applied by the client.
    """

    class BoundType(str, Enum):
        """Whether an endpoint is excluded or included."""

        OPEN = "OPEN"
        CLOSED = "CLOSED"

    lower_partition_name: str | None = None
    lower_bound_type: BoundType | None = None
    upper_partition_name: str | None = None
    upper_bound_type: BoundType | None = None

    ALL_PARTITIONS: ClassVar[PartitionRange]

    def __post_init__(self) -> None:
        for name, bound in (
            (self.lower_partition_name, self.lower_bound_type),
            (self.upper_partition_name, self.upper_bound_type),
        ):
            Precondition.check_argument(
                (name is None) == (bound is None),
                "A partition name and its bound type must be supplied together",
            )
            if name is not None:
                Precondition.check_argument(
                    isinstance(name, str) and bool(name.strip()),
                    "Partition bound name must not be blank",
                )
                Precondition.check_argument(
                    isinstance(bound, self.BoundType),
                    "Partition bound type must be OPEN or CLOSED",
                )

    @classmethod
    def up_to(cls, name: str, bound_type: BoundType) -> PartitionRange:
        """Create an upper-bounded range."""
        Precondition.check_argument(
            name is not None, "Upper partition name is required"
        )
        return cls(upper_partition_name=name, upper_bound_type=bound_type)

    @classmethod
    def down_to(cls, name: str, bound_type: BoundType) -> PartitionRange:
        """Create a lower-bounded range."""
        Precondition.check_argument(
            name is not None, "Lower partition name is required"
        )
        return cls(lower_partition_name=name, lower_bound_type=bound_type)

    @classmethod
    def between(
        cls,
        lower_name: str,
        lower_type: BoundType,
        upper_name: str,
        upper_type: BoundType,
    ) -> PartitionRange:
        """Create a range with both endpoints."""
        Precondition.check_argument(
            lower_name is not None and upper_name is not None,
            "Both partition names are required",
        )
        return cls(lower_name, lower_type, upper_name, upper_type)


PartitionRange.ALL_PARTITIONS = PartitionRange()
