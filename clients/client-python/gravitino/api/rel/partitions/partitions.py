# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from typing import Any, Dict, List, Optional

from gravitino.api.rel.expressions.literals.literal import Literal
from gravitino.api.rel.partitions.identity_partition import IdentityPartition
from gravitino.api.rel.partitions.list_partition import ListPartition
from gravitino.api.rel.partitions.partition import Partition
from gravitino.api.rel.partitions.range_partition import RangePartition


class Partitions:
    """The helper class for partition expressions."""

    EMPTY_PARTITIONS: List[Partition] = []
    """
    An empty array of partitions
    """

    @staticmethod
    def range(
        name: str,
        upper: Literal[Any],
        lower: Literal[Any],
        properties: Optional[Dict[str, str]],
    ) -> RangePartition:
        """
        Creates a range partition.

        Args:
            name: The name of the partition.
            upper: The upper bound of the partition.
            lower: The lower bound of the partition.
            properties: The properties of the partition.

        Returns:
            The created partition.
        """
        return RangePartitionImpl(name, upper, lower, properties)

    @staticmethod
    def list(
        name: str,
        lists: List[List[Literal[Any]]],
        properties: Optional[Dict[str, str]],
    ) -> ListPartition:
        """
        Creates a list partition.

        Args:
            name: The name of the partition.
            lists: The values of the list partition.
            properties: The properties of the partition.

        Returns:
            The created partition.
        """
        return ListPartitionImpl(name, lists, properties or {})

    @staticmethod
    def identity(
        name: Optional[str],
        field_names: List[List[str]],
        values: List[Literal[Any]],
        properties: Optional[Dict[str, str]] = None,
    ) -> IdentityPartition:
        """
        Creates an identity partition.

        The `values` must correspond to the `field_names`.

        Args:
            name: The name of the partition.
            field_names: The field names of the identity partition.
            values: The value of the identity partition.
            properties: The properties of the partition.

        Returns:
            The created partition.
        """
        return IdentityPartitionImpl(name, field_names, values, properties or {})


class RangePartitionImpl(RangePartition):
    """
    Represents a result of range partitioning.
    """

    def __init__(
        self,
        name: str,
        upper: Literal[Any],
        lower: Literal[Any],
        properties: Optional[Dict[str, str]],
    ):
        self._name = name
        self._upper = upper
        self._lower = lower
        self._properties = properties

    def upper(self) -> Literal[Any]:
        """Returns the upper bound of the partition."""
        return self._upper

    def lower(self) -> Literal[Any]:
        """Returns the lower bound of the partition."""
        return self._lower

    def name(self) -> str:
        return self._name

    def properties(self) -> Dict[str, str]:
        return self._properties

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, RangePartitionImpl):
            return False
        return (
            self._name == other._name
            and self._upper == other._upper
            and self._lower == other._lower
            and self._properties == other._properties
        )

    def __hash__(self) -> int:
        return hash(
            (self._name, self._upper, self._lower, frozenset(self._properties.items()))
        )


class ListPartitionImpl(ListPartition):
    def __init__(
        self,
        name: str,
        lists: List[List[Literal[Any]]],
        properties: Optional[Dict[str, str]],
    ):
        self._name = name
        self._lists = lists
        self._properties = properties

    def lists(self) -> List[List[Literal[Any]]]:
        """Returns the values of the list partition."""
        return self._lists

    def name(self) -> str:
        return self._name

    def properties(self) -> Dict[str, str]:
        return self._properties

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ListPartitionImpl):
            return False
        return (
            self._name == other._name
            and self._lists == other._lists
            and self._properties == other._properties
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._name,
                tuple(tuple(sublist) for sublist in self._lists),
                frozenset(self._properties.items()),
            )
        )


class IdentityPartitionImpl(IdentityPartition):
    def __init__(
        self,
        name: str,
        field_names: List[List[str]],
        values: List[Literal[Any]],
        properties: Dict[str, str],
    ):
        self._name = name
        self._field_names = field_names
        self._values = values
        self._properties = properties

    def field_names(self) -> List[List[str]]:
        """Returns the field names of the identity partition."""
        return self._field_names

    def values(self) -> List[Literal[Any]]:
        """Returns the values of the identity partition."""
        return self._values

    def name(self) -> str:
        return self._name

    def properties(self) -> Dict[str, str]:
        return self._properties

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, IdentityPartitionImpl):
            return False
        return (
            self._name == other._name
            and self._field_names == other._field_names
            and self._values == other._values
            and self._properties == other._properties
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._name,
                tuple(tuple(fn) for fn in self._field_names),
                tuple(self._values),
                frozenset(self._properties.items()),
            )
        )
