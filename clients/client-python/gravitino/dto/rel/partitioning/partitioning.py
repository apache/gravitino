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

from abc import abstractmethod
from enum import Enum, unique
from typing import Final, List

from gravitino.api.expressions.expression import Expression
from gravitino.api.expressions.named_reference import NamedReference
from gravitino.api.expressions.transforms.transform import Transform
from gravitino.dto.rel.partition_utils import PartitionUtils
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.utils.precondition import Precondition


class Partitioning(Transform):
    EMPTY_PARTITIONING: Final[List["Partitioning"]] = []
    """An empty array of partitioning."""

    @unique
    class Strategy(str, Enum):
        """Represents a partitioning strategy."""

        IDENTITY = "identity"
        """The identity partitioning strategy."""

        YEAR = "year"
        """The year partitioning strategy."""

        MONTH = "month"
        """The month partitioning strategy."""

        DAY = "day"
        """The day partitioning strategy."""

        HOUR = "hour"
        """The hour partitioning strategy."""

        BUCKET = "bucket"
        """The bucket partitioning strategy."""

        TRUNCATE = "truncate"
        """The truncate partitioning strategy."""

        LIST = "list"
        """The list partitioning strategy."""

        RANGE = "range"
        """The range partitioning strategy."""

        FUNCTION = "function"
        """The function partitioning strategy."""

    @abstractmethod
    def strategy(self) -> Strategy:
        """Returns the name of the partitioning strategy.

        Returns:
            Strategy: The name of the partitioning strategy.
        """
        pass  # pragma: no cover

    @abstractmethod
    def validate(self, columns: List["ColumnDTO"]) -> None:
        """Validates the partitioning columns.

        Args:
            columns (List[ColumnDTO]): The columns to be validated.

        Raises:
            IllegalArgumentException: If the columns are invalid, this exception is thrown.
        """
        pass  # pragma: no cover

    @staticmethod
    def get_by_name(name: str) -> Strategy:
        """Gets the partitioning strategy by name.

        Args:
            name (str): The name of the partitioning strategy.

        Returns:
            Strategy: The partitioning strategy.

        Raises:
            IllegalArgumentException: If the name is invalid, this exception is thrown.
        """

        for strategy in Partitioning.Strategy:
            if strategy.name.lower() == name.lower():
                return strategy
        raise IllegalArgumentException(
            f"Invalid partitioning strategy: {name}. "
            f"Valid values are: {', '.join([str(s) for s in Partitioning.Strategy])}"
        )


class SingleFieldPartitioning(Partitioning):
    """A single field partitioning strategy."""

    def __init__(self, field_name: List[str]):
        Precondition.check_argument(
            field_name is not None and len(field_name) > 0,
            "field_name cannot be null or empty",
        )
        self._field_name = field_name

    def field_name(self) -> List[str]:
        """Returns the field name of the partitioning.

        Returns:
            List[str]: The field name of the partitioning.
        """
        return self._field_name

    def validate(self, columns: List["ColumnDTO"]) -> None:
        """Validates the partitioning columns.

        Args:
            columns (List[ColumnDTO]): The columns to be validated.

        Raises:
            IllegalArgumentException: If the columns are invalid, this exception is thrown.
        """

        PartitionUtils.validate_field_existence(columns, self._field_name)

    def name(self) -> str:
        """Returns the name of the partitioning strategy.

        Returns:
            str: The name of the partitioning strategy.
        """
        return self.strategy().name.lower()

    def arguments(self) -> List[Expression]:
        """Returns the arguments of the partitioning strategy.

        Returns:
            List[Expression]: The arguments of the partitioning strategy.
        """
        return [NamedReference.field(self._field_name)]
