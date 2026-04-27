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


from typing import Any, TypeVar

from gravitino.api.rel.types.type import Type
from gravitino.api.rel.types.types import Types
from gravitino.api.stats.statistic_value import StatisticValue
from gravitino.utils.precondition import Precondition

T = TypeVar("T")


class StatisticValues:
    """A class representing a collection of statistic values."""

    @staticmethod
    def boolean_value(value: bool) -> "BooleanValue":
        """Creates a statistic value that holds a boolean value.

        Args:
            value: the boolean value to be held by this statistic value

        Returns:
            A BooleanValue instance containing the provided boolean value
        """
        return StatisticValues.BooleanValue(value)

    @staticmethod
    def long_value(value: int) -> "LongValue":
        """Creates a statistic value that holds a long value.

        Args:
            value: the long value to be held by this statistic value

        Returns:
            A LongValue instance containing the provided long value
        """
        return StatisticValues.LongValue(value)

    @staticmethod
    def double_value(value: float) -> "DoubleValue":
        """Creates a statistic value that holds a double value.

        Args:
            value: the double value to be held by this statistic value

        Returns:
            A DoubleValue instance containing the provided double value
        """
        return StatisticValues.DoubleValue(value)

    @staticmethod
    def string_value(value: str) -> "StringValue":
        """Creates a statistic value that holds a string value.

        Args:
            value: the string value to be held by this statistic value

        Returns:
            A StringValue instance containing the provided string value
        """
        return StatisticValues.StringValue(value)

    @staticmethod
    def list_value(value_list: list[StatisticValue[T]]) -> "ListValue[T]":
        """Creates a statistic value that holds a list of other statistic values.

        Args:
            value_list: the list of statistic values to be held by this statistic value

        Returns:
            A ListValue instance containing the provided list of statistic values
        """
        return StatisticValues.ListValue(value_list)

    @staticmethod
    def object_value(value_list: dict[str, StatisticValue[Any]]) -> "ObjectValue[Any]":
        """Creates a statistic value that holds a list of other statistic values.

        Args:
            value_list: the list of statistic values to be held by this statistic value

        Returns:
            A ListValue instance containing the provided list of statistic values
        """
        return StatisticValues.ObjectValue(value_list)

    class BooleanValue(StatisticValue[bool]):
        """A statistic value that holds a Boolean value."""

        def __init__(self, value: bool) -> None:
            self._value = value

        def value(self) -> bool:
            return self._value

        def data_type(self) -> Type:
            return Types.BooleanType.get()

        def __hash__(self) -> int:
            return hash(self._value)

        def __eq__(self, other) -> bool:
            if not isinstance(other, StatisticValues.BooleanValue):
                return False
            return self._value == other._value

    class LongValue(StatisticValue[int]):
        """A statistic value that holds a Long value."""

        def __init__(self, value: int) -> None:
            self._value = value

        def value(self) -> int:
            return self._value

        def data_type(self) -> Type:
            return Types.LongType.get()

        def __hash__(self) -> int:
            return hash(self._value)

        def __eq__(self, other) -> bool:
            if not isinstance(other, StatisticValues.LongValue):
                return False
            return self._value == other._value

    class DoubleValue(StatisticValue[float]):
        """A statistic value that holds a Double value."""

        def __init__(self, value: float) -> None:
            self._value = value

        def value(self) -> float:
            return self._value

        def data_type(self) -> Type:
            return Types.DoubleType.get()

        def __hash__(self) -> int:
            return hash(self._value)

        def __eq__(self, other) -> bool:
            if not isinstance(other, StatisticValues.DoubleValue):
                return False
            return self._value == other._value

    class StringValue(StatisticValue[str]):
        """A statistic value that holds a String value."""

        def __init__(self, value: str) -> None:
            self._value = value

        def value(self) -> str:
            return self._value

        def data_type(self) -> Type:
            return Types.StringType.get()

        def __hash__(self) -> int:
            return hash(self._value)

        def __eq__(self, other) -> bool:
            if not isinstance(other, StatisticValues.StringValue):
                return False
            return self._value == other._value

    class ListValue(StatisticValue[list[StatisticValue[T]]]):
        """A statistic value that holds a List of other statistic values."""

        def __init__(self, value_list: list[StatisticValue[T]]) -> None:
            Precondition.check_argument(
                value_list is not None and len(value_list) > 0,
                "Values cannot be null or empty",
            )
            data_type = value_list[0].data_type()
            Precondition.check_argument(
                all(value.data_type() == data_type for value in value_list),
                "All values in the list must have the same data type",
            )
            self._value_list = value_list

        def value(self) -> list[StatisticValue[T]]:
            return self._value_list

        def data_type(self) -> Type:
            return Types.ListType.nullable(self._value_list[0].data_type())

        def __hash__(self) -> int:
            return hash(tuple(v.value() for v in self._value_list))

        def __eq__(self, other) -> bool:
            if not isinstance(other, StatisticValues.ListValue):
                return False
            return self._value_list == other._value_list

    class ObjectValue(StatisticValue[dict[str, StatisticValue[T]]]):
        """A statistic value that holds a Map of String keys to other statistic values."""

        def __init__(self, value_map: dict[str, StatisticValue[T]]) -> None:
            Precondition.check_argument(
                value_map is not None and len(value_map) > 0,
                "Values cannot be null or empty",
            )
            self._value_map = value_map

        def value(self) -> dict[str, StatisticValue[T]]:
            return self._value_map

        def data_type(self) -> Type:
            return Types.StructType.of(
                Types.StructType.Field.nullable_field(key, value.data_type())
                for key, value in self._value_map.items()
            )

        def __hash__(self) -> int:
            return hash(tuple(v.value() for v in self._value_map.values()))

        def __eq__(self, other) -> bool:
            if not isinstance(other, StatisticValues.ObjectValue):
                return False
            return self._value_map == other._value_map
