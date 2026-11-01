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
from typing import Optional

from gravitino.api.semantic.semantic_utils import check_non_empty_string_elements
from gravitino.utils.precondition import Precondition


class PlanJoin:
    """A join between two datasets of a `SemanticQueryPlan`.

    The join columns are positional: ``from_columns[i]`` is joined to
    ``to_columns[i]``.
    """

    class Type(Enum):
        """The supported join types."""

        INNER = "INNER"

    def __init__(
        self,
        join_type: "PlanJoin.Type",
        relationship: Optional[str],
        from_dataset: str,
        from_columns: list[str],
        to_dataset: str,
        to_columns: list[str],
    ):
        Precondition.check_argument(
            isinstance(join_type, PlanJoin.Type), "type must not be null"
        )
        Precondition.check_argument(
            relationship is None or relationship.strip() != "",
            "relationship must not be blank when present",
        )
        Precondition.check_string_not_empty(
            from_dataset, "fromDataset must not be null or blank"
        )
        Precondition.check_string_not_empty(
            to_dataset, "toDataset must not be null or blank"
        )
        Precondition.check_argument(
            from_columns is not None and len(from_columns) > 0,
            "fromColumns must not be null or empty",
        )
        Precondition.check_argument(
            to_columns is not None and len(to_columns) > 0,
            "toColumns must not be null or empty",
        )
        check_non_empty_string_elements("fromColumns", from_columns)
        check_non_empty_string_elements("toColumns", to_columns)
        Precondition.check_argument(
            len(from_columns) == len(to_columns),
            "fromColumns and toColumns must have the same length",
        )
        self._type = join_type
        self._relationship = relationship
        self._from_dataset = from_dataset
        self._from_columns = tuple(from_columns)
        self._to_dataset = to_dataset
        self._to_columns = tuple(to_columns)

    @staticmethod
    def of(
        join_type: "PlanJoin.Type",
        relationship: Optional[str],
        from_dataset: str,
        from_columns: list[str],
        to_dataset: str,
        to_columns: list[str],
    ) -> "PlanJoin":
        """Create a plan join."""
        return PlanJoin(
            join_type,
            relationship,
            from_dataset,
            from_columns,
            to_dataset,
            to_columns,
        )

    def type(self) -> "PlanJoin.Type":
        """Returns the join type."""
        return self._type

    def relationship(self) -> Optional[str]:
        """Returns the relationship name, or `None` if the join is implied."""
        return self._relationship

    def from_dataset(self) -> str:
        """Returns the dataset already in scope."""
        return self._from_dataset

    def from_columns(self) -> list[str]:
        """Returns a copy of the join columns of the source dataset."""
        return list(self._from_columns)

    def to_dataset(self) -> str:
        """Returns the dataset this join brings into scope."""
        return self._to_dataset

    def to_columns(self) -> list[str]:
        """Returns a copy of the join columns of the target dataset."""
        return list(self._to_columns)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PlanJoin):
            return False
        return (
            self._type == other.type()
            and self._relationship == other.relationship()
            and self._from_dataset == other.from_dataset()
            and self._from_columns == tuple(other.from_columns())
            and self._to_dataset == other.to_dataset()
            and self._to_columns == tuple(other.to_columns())
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._type,
                self._relationship,
                self._from_dataset,
                self._from_columns,
                self._to_dataset,
                self._to_columns,
            )
        )

    def __repr__(self) -> str:
        return (
            f"PlanJoin(type={self._type}, relationship={self._relationship!r}, "
            f"fromDataset={self._from_dataset!r}, "
            f"fromColumns={list(self._from_columns)!r}, "
            f"toDataset={self._to_dataset!r}, "
            f"toColumns={list(self._to_columns)!r})"
        )
