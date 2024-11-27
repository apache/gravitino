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

from decimal import Decimal
from typing import Union
from datetime import date, time, datetime

from gravitino.api.expressions.literals.literal import Literal
from gravitino.api.types import Types


class LiteralImpl(Literal):
    """Creates a literal with the given type value."""

    _value: Union[int, float, Decimal, str, date, time, datetime, bool, None]
    _data_type: Union[
        Types.IntegerType,
        Types.LongType,
        Types.FloatType,
        Types.DoubleType,
        Types.DecimalType,
        Types.StringType,
        Types.DateType,
        Types.TimeType,
        Types.TimestampType,
        Types.BooleanType,
        Types.NullType,
    ]

    def __init__(
        self,
        value: Union[int, float, str, datetime, time, date, bool, Decimal, None],
        data_type: str,
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


class Literals:
    """The helper class to create literals to pass into Apache Gravitino."""

    NULL = LiteralImpl(None, "NullType")

    @staticmethod
    def of(value, data_type) -> Literal:
        return LiteralImpl(value, data_type)

    @staticmethod
    def boolean_literal(value: bool) -> Literal:
        return LiteralImpl(value, "Boolean")

    @staticmethod
    def byte_literal(value: int) -> Literal:
        return LiteralImpl(value, "Byte")

    @staticmethod
    def unsigned_byte_literal(value: int) -> Literal:
        return LiteralImpl(value, "Unsigned Byte")

    @staticmethod
    def short_literal(value: int) -> Literal:
        return LiteralImpl(value, "Short")

    @staticmethod
    def unsigned_short_literal(value: int) -> Literal:
        return LiteralImpl(value, "Unsigned Short")

    @staticmethod
    def integer_literal(value: int) -> Literal:
        return LiteralImpl(value, "Integer")

    @staticmethod
    def unsigned_integer_literal(value: int) -> Literal:
        return LiteralImpl(value, "Unsigned Integer")

    @staticmethod
    def long_literal(value: int) -> Literal:
        return LiteralImpl(value, "Long")

    @staticmethod
    def unsigned_long_literal(value: Decimal) -> Literal:
        return LiteralImpl(value, "Unsigned Long")

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

    @staticmethod
    def string_literal(value: str) -> Literal:
        return LiteralImpl(value, "String")

    @staticmethod
    def varchar_literal(length: int, value: str) -> Literal:
        return LiteralImpl(value, f"Varchar({length})")
