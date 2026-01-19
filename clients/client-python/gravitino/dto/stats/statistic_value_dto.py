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

"""Data Transfer Objects for statistic values with JSON serialization support."""

from abc import ABC, abstractmethod
from typing import List, Dict, TypeVar

from gravitino.api.statistics.statistic_value import StatisticValue


T = TypeVar("T")


class StatisticValueDTO(StatisticValue[T], ABC):
    """Abstract base class for statistic value DTOs."""

    @abstractmethod
    def to_json(self) -> dict:
        """Convert to JSON representation."""
        pass

    @classmethod
    def from_json(cls, json_dict: dict) -> "StatisticValueDTO":
        """Create a StatisticValueDTO from JSON representation."""
        data_type = json_dict.get("type")
        value = json_dict.get("value")

        if data_type == StatisticValue.Type.BOOLEAN:
            return BooleanValueDTO(value)
        if data_type == StatisticValue.Type.LONG:
            return LongValueDTO(value)
        if data_type == StatisticValue.Type.DOUBLE:
            return DoubleValueDTO(value)
        if data_type == StatisticValue.Type.STRING:
            return StringValueDTO(value)
        if data_type == StatisticValue.Type.LIST:
            items = [StatisticValueDTO.from_json(item) for item in value]
            return ListValueDTO(items)
        if data_type == StatisticValue.Type.OBJECT:
            obj = {k: StatisticValueDTO.from_json(v) for k, v in value.items()}
            return ObjectValueDTO(obj)
        raise ValueError(f"Unknown statistic value type: {data_type}")


class BooleanValueDTO(StatisticValueDTO[bool]):
    """Boolean statistic value DTO."""

    def __init__(self, value: bool):
        self._value = value

    def value(self) -> bool:
        return self._value

    def data_type(self) -> str:
        return StatisticValue.Type.BOOLEAN

    def to_json(self) -> dict:
        return {"type": self.data_type(), "value": self._value}


class LongValueDTO(StatisticValueDTO[int]):
    """Long statistic value DTO."""

    def __init__(self, value: int):
        self._value = value

    def value(self) -> int:
        return self._value

    def data_type(self) -> str:
        return StatisticValue.Type.LONG

    def to_json(self) -> dict:
        return {"type": self.data_type(), "value": self._value}


class DoubleValueDTO(StatisticValueDTO[float]):
    """Double statistic value DTO."""

    def __init__(self, value: float):
        self._value = value

    def value(self) -> float:
        return self._value

    def data_type(self) -> str:
        return StatisticValue.Type.DOUBLE

    def to_json(self) -> dict:
        return {"type": self.data_type(), "value": self._value}


class StringValueDTO(StatisticValueDTO[str]):
    """String statistic value DTO."""

    def __init__(self, value: str):
        self._value = value

    def value(self) -> str:
        return self._value

    def data_type(self) -> str:
        return StatisticValue.Type.STRING

    def to_json(self) -> dict:
        return {"type": self.data_type(), "value": self._value}


class ListValueDTO(StatisticValueDTO[List[StatisticValueDTO]]):
    """List statistic value DTO."""

    def __init__(self, values: List[StatisticValueDTO]):
        self._values = values

    def value(self) -> List[StatisticValueDTO]:
        return self._values

    def data_type(self) -> str:
        return StatisticValue.Type.LIST

    def to_json(self) -> dict:
        return {
            "type": self.data_type(),
            "value": [item.to_json() for item in self._values],
        }


class ObjectValueDTO(StatisticValueDTO[Dict[str, StatisticValueDTO]]):
    """Object statistic value DTO."""

    def __init__(self, values: Dict[str, StatisticValueDTO]):
        self._values = values

    def value(self) -> Dict[str, StatisticValueDTO]:
        return self._values

    def data_type(self) -> str:
        return StatisticValue.Type.OBJECT

    def to_json(self) -> dict:
        return {
            "type": self.data_type(),
            "value": {k: v.to_json() for k, v in self._values.items()},
        }
