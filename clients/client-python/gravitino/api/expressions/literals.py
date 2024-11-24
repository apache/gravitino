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

from abc import ABC, abstractmethod
from typing import Union
from datetime import date, time, datetime


# Abstract base class for Literal
class Literal(ABC):
    """
    Represents a constant literal value in the expression API.
    """

    @abstractmethod
    def value(self) -> Union[int, float, str, datetime, time, date, bool]:
        """
        Returns the literal value.
        """
        pass

    @abstractmethod
    def data_type(self) -> str:
        """
        Returns the data type of the literal.
        """
        pass

    def children(self):
        """
        Returns the child expressions. By default, this is an empty list.
        """
        return []


# Concrete implementation of Literal
class LiteralImpl(Literal):
    def __init__(
        self, value: Union[int, float, str, datetime, time, date, bool], data_type: str
    ):
        self._value = value
        self._data_type = data_type

    def value(self) -> Union[int, float, str, datetime, time, date, bool]:
        return self._value

    def data_type(self) -> str:
        return self._data_type

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, LiteralImpl):
            return False
        return (self._value == other._value) and (self._data_type == other._data_type)

    def __hash__(self):
        return hash((self._value, self._data_type))

    def __str__(self):
        return f"LiteralImpl(value={self._value}, data_type={self._data_type})"


# Helper class to create literals
class Literals:
    @staticmethod
    def null_literal() -> Literal:
        return LiteralImpl(None, "NullType")

    @staticmethod
    def boolean_literal(value: bool) -> Literal:
        return LiteralImpl(value, "Boolean")

    @staticmethod
    def byte_literal(value: int) -> Literal:
        return LiteralImpl(value, "Byte")

    @staticmethod
    def short_literal(value: int) -> Literal:
        return LiteralImpl(value, "Short")

    @staticmethod
    def integer_literal(value: int) -> Literal:
        return LiteralImpl(value, "Integer")

    @staticmethod
    def long_literal(value: int) -> Literal:
        return LiteralImpl(value, "Long")

    @staticmethod
    def float_literal(value: float) -> Literal:
        return LiteralImpl(value, "Float")

    @staticmethod
    def double_literal(value: float) -> Literal:
        return LiteralImpl(value, "Double")

    @staticmethod
    def decimal_literal(value: float) -> Literal:
        return LiteralImpl(value, "Decimal")

    @staticmethod
    def string_literal(value: str) -> Literal:
        return LiteralImpl(value, "String")

    @staticmethod
    def varchar_literal(length: int, value: str) -> Literal:
        return LiteralImpl(value, f"Varchar({length})")

    @staticmethod
    def date_literal(value: date) -> Literal:
        return LiteralImpl(value, "Date")

    @staticmethod
    def time_literal(value: time) -> Literal:
        return LiteralImpl(value, "Time")

    @staticmethod
    def timestamp_literal(value: datetime) -> Literal:
        return LiteralImpl(value, "Timestamp")

    @staticmethod
    def timestamp_literal_from_string(value: str) -> Literal:
        return Literals.timestamp_literal(datetime.fromisoformat(value))
