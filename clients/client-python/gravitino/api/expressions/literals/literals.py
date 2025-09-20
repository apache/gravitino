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
import decimal
from datetime import date, datetime, time
from typing import TypeVar

from gravitino.api.expressions.literals.literal import Literal
from gravitino.api.rel.types.type import Type
from gravitino.api.rel.types.types import Types

T = TypeVar("T")


class LiteralImpl(Literal[T]):
    """Creates a literal with the given type value."""

    _value: T
    _data_type: Type

    def __init__(self, value: T, data_type: Type):
        self._value = value
        self._data_type = data_type

    def value(self) -> T:
        return self._value

    def data_type(self) -> Type:
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

    NULL = LiteralImpl(None, Types.NullType.get())

    @staticmethod
    def of(value: T, data_type: Type) -> Literal[T]:
        return LiteralImpl(value, data_type)

    @staticmethod
    def boolean_literal(value: bool) -> LiteralImpl[bool]:
        return LiteralImpl(value, Types.BooleanType.get())

    @staticmethod
    def byte_literal(value: str) -> LiteralImpl[str]:
        return LiteralImpl(value, Types.ByteType.get())

    @staticmethod
    def unsigned_byte_literal(value: str) -> LiteralImpl[str]:
        return LiteralImpl(value, Types.ByteType.unsigned())

    @staticmethod
    def short_literal(value: int) -> LiteralImpl[int]:
        return LiteralImpl(value, Types.ShortType.get())

    @staticmethod
    def unsigned_short_literal(value: int) -> LiteralImpl[int]:
        return LiteralImpl(value, Types.ShortType.unsigned())

    @staticmethod
    def integer_literal(value: int) -> LiteralImpl[int]:
        return LiteralImpl(value, Types.IntegerType.get())

    @staticmethod
    def unsigned_integer_literal(value: int) -> LiteralImpl[int]:
        return LiteralImpl(value, Types.IntegerType.unsigned())

    @staticmethod
    def long_literal(value: int) -> LiteralImpl[int]:
        return LiteralImpl(value, Types.LongType.get())

    @staticmethod
    def unsigned_long_literal(value: int) -> LiteralImpl[int]:
        return LiteralImpl(value, Types.LongType.unsigned())

    @staticmethod
    def float_literal(value: float) -> LiteralImpl[float]:
        return LiteralImpl(value, Types.FloatType.get())

    @staticmethod
    def double_literal(value: float) -> LiteralImpl[float]:
        return LiteralImpl(value, Types.DoubleType.get())

    @staticmethod
    def decimal_literal(value: decimal.Decimal) -> LiteralImpl[decimal.Decimal]:
        precision: int = len(value.as_tuple().digits)
        scale: int = -value.as_tuple().exponent
        return LiteralImpl(value, Types.DecimalType.of(max(precision, scale), scale))

    @staticmethod
    def date_literal(value: date) -> Literal[date]:
        return LiteralImpl(value, Types.DateType.get())

    @staticmethod
    def time_literal(value: time) -> Literal[time]:
        return Literals.of(value, Types.TimeType.get())

    @staticmethod
    def timestamp_literal(value: datetime) -> Literal[datetime]:
        return Literals.of(value, Types.TimestampType.without_time_zone())

    @staticmethod
    def timestamp_literal_from_string(value: str) -> Literal[datetime]:
        return Literals.timestamp_literal(datetime.fromisoformat(value))

    @staticmethod
    def string_literal(value: str) -> Literal[str]:
        return LiteralImpl(value, Types.StringType.get())

    @staticmethod
    def varchar_literal(length: int, value: str) -> Literal[str]:
        return LiteralImpl(value, Types.VarCharType.of(length))
