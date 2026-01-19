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

"""Factory and concrete implementations for creating statistic values."""

from typing import List, Dict, TypeVar

from gravitino.api.statistics.statistic_value import StatisticValue


T = TypeVar("T")


class StatisticValues:
    """Factory class for creating statistic values."""

    class BooleanValue(StatisticValue[bool]):
        """Boolean statistic value."""

        def __init__(self, value: bool):
            self._value = value

        def value(self) -> bool:
            return self._value

        def data_type(self) -> str:
            return StatisticValue.Type.BOOLEAN

    class LongValue(StatisticValue[int]):
        """Long integer statistic value."""

        def __init__(self, value: int):
            self._value = value

        def value(self) -> int:
            return self._value

        def data_type(self) -> str:
            return StatisticValue.Type.LONG

    class DoubleValue(StatisticValue[float]):
        """Double (float) statistic value."""

        def __init__(self, value: float):
            self._value = value

        def value(self) -> float:
            return self._value

        def data_type(self) -> str:
            return StatisticValue.Type.DOUBLE

    class StringValue(StatisticValue[str]):
        """String statistic value."""

        def __init__(self, value: str):
            self._value = value

        def value(self) -> str:
            return self._value

        def data_type(self) -> str:
            return StatisticValue.Type.STRING

    class ListValue(StatisticValue[List[StatisticValue]]):
        """List statistic value."""

        def __init__(self, values: List[StatisticValue]):
            self._values = values

        def value(self) -> List[StatisticValue]:
            return self._values

        def data_type(self) -> str:
            return StatisticValue.Type.LIST

    class ObjectValue(StatisticValue[Dict[str, StatisticValue]]):
        """Object (map) statistic value."""

        def __init__(self, values: Dict[str, StatisticValue]):
            self._values = values

        def value(self) -> Dict[str, StatisticValue]:
            return self._values

        def data_type(self) -> str:
            return StatisticValue.Type.OBJECT

    @staticmethod
    def of_boolean(value: bool) -> StatisticValue[bool]:
        """Create a boolean statistic value."""
        return StatisticValues.BooleanValue(value)

    @staticmethod
    def of_long(value: int) -> StatisticValue[int]:
        """Create a long statistic value."""
        return StatisticValues.LongValue(value)

    @staticmethod
    def of_double(value: float) -> StatisticValue[float]:
        """Create a double statistic value."""
        return StatisticValues.DoubleValue(value)

    @staticmethod
    def of_string(value: str) -> StatisticValue[str]:
        """Create a string statistic value."""
        return StatisticValues.StringValue(value)

    @staticmethod
    def of_list(values: List[StatisticValue]) -> StatisticValue[List[StatisticValue]]:
        """Create a list statistic value."""
        return StatisticValues.ListValue(values)

    @staticmethod
    def of_object(
        values: Dict[str, StatisticValue]
    ) -> StatisticValue[Dict[str, StatisticValue]]:
        """Create an object statistic value."""
        return StatisticValues.ObjectValue(values)
