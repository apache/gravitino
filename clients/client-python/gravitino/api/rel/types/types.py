"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

from gravitino.utils import check_argument
from gravitino.api.rel.types.type import (
    Name,
    Type,
    DateTimeType,
    PrimitiveType,
    IntegralType,
    ComplexType,
    FractionType,
)


class Types:
    """The helper class for types."""

    class NullType(Type):
        """The data type representing `NULL` values."""

        _instance = None

        @staticmethod
        def get() -> "Types.NullType":
            """Returns the singleton instance of the null type."""
            if Types.NullType._instance is None:
                Types.NullType._instance = Types.NullType()
            return Types.NullType._instance

        def name(self) -> str:
            return Name.NULL

        def simple_string(self) -> str:
            return "null"

    class BooleanType(PrimitiveType):
        """The boolean type in Gravitino."""

        _instance = None

        @staticmethod
        def get() -> "Types.BooleanType":
            """Returns the singleton instance of the boolean type."""
            if Types.BooleanType._instance is None:
                Types.BooleanType._instance = Types.BooleanType()
            return Types.BooleanType._instance

        def name(self) -> str:
            return Name.BOOLEAN

        def simple_string(self) -> str:
            return "boolean"

    class ByteType(IntegralType):
        """The byte type in Gravitino."""

        _instance = None
        _unsigned_instance = None

        @staticmethod
        def get() -> "Types.ByteType":
            """Returns the singleton instance of the byte type."""
            if Types.ByteType._instance is None:
                Types.ByteType._instance = Types.ByteType(True)
            return Types.ByteType._instance

        @staticmethod
        def unsigned() -> "Types.ByteType":
            """Returns the singleton instance of the unsigned byte type."""
            if Types.ByteType._unsigned_instance is None:
                Types.ByteType._unsigned_instance = Types.ByteType(False)
            return Types.ByteType._unsigned_instance

        def name(self) -> str:
            return Name.BYTE

        def simple_string(self) -> str:
            return "byte" if self.signed() else "byte unsigned"

    class ShortType(IntegralType):
        """The short type in Gravitino."""

        _instance = None
        _unsigned_instance = None

        @staticmethod
        def get() -> "Types.ShortType":
            """Returns the singleton instance of the short type."""
            if Types.ShortType._instance is None:
                Types.ShortType._instance = Types.ShortType(True)
            return Types.ShortType._instance

        @staticmethod
        def unsigned() -> "Types.ShortType":
            """Returns the singleton instance of the unsigned short type."""
            if Types.ShortType._unsigned_instance is None:
                Types.ShortType._unsigned_instance = Types.ShortType(False)
            return Types.ShortType._unsigned_instance

        def name(self) -> str:
            return Name.SHORT

        def simple_string(self) -> str:
            return "short" if self.signed() else "short unsigned"

    class IntegerType(IntegralType):
        """The integer type in Gravitino."""

        _instance = None
        _unsigned_instance = None

        @staticmethod
        def get() -> "Types.IntegerType":
            """Returns the singleton instance of the integer type."""
            if Types.IntegerType._instance is None:
                Types.IntegerType._instance = Types.IntegerType(True)
            return Types.IntegerType._instance

        @staticmethod
        def unsigned() -> "Types.IntegerType":
            """Returns the singleton instance of the unsigned integer type."""
            if Types.IntegerType._unsigned_instance is None:
                Types.IntegerType._unsigned_instance = Types.IntegerType(False)
            return Types.IntegerType._unsigned_instance

        def name(self) -> str:
            return Name.INTEGER

        def simple_string(self) -> str:
            return "integer" if self.signed() else "integer unsigned"

    class LongType(IntegralType):
        """The long type in Gravitino."""

        _instance = None
        _unsigned_instance = None

        @staticmethod
        def get() -> "Types.LongType":
            """Returns the singleton instance of the long type."""
            if Types.LongType._instance is None:
                Types.LongType._instance = Types.LongType(True)
            return Types.LongType._instance

        @staticmethod
        def unsigned() -> "Types.LongType":
            """Returns the singleton instance of the unsigned long type."""
            if Types.LongType._unsigned_instance is None:
                Types.LongType._unsigned_instance = Types.LongType(False)
            return Types.LongType._unsigned_instance

        def name(self) -> str:
            return Name.LONG

        def simple_string(self) -> str:
            return "long" if self.signed() else "long unsigned"

    class FloatType(FractionType):
        """The float type in Gravitino."""

        _instance = None

        @staticmethod
        def get() -> "Types.FloatType":
            """Returns the singleton instance of the float type."""
            if Types.FloatType._instance is None:
                Types.FloatType._instance = Types.FloatType()
            return Types.FloatType._instance

        def name(self) -> str:
            return Name.FLOAT

        def simple_string(self) -> str:
            return "float"

    class DoubleType(FractionType):
        """The double type in Gravitino."""

        _instance = None

        @staticmethod
        def get() -> "Types.DoubleType":
            """Returns the singleton instance of the double type."""
            if Types.DoubleType._instance is None:
                Types.DoubleType._instance = Types.DoubleType()
            return Types.DoubleType._instance

        def name(self) -> str:
            return Name.DOUBLE

        def simple_string(self) -> str:
            return "double"

    class DecimalType(FractionType):
        """The decimal type in Gravitino."""

        def __init__(self, precision: int, scale: int):
            """
            Args:
                precision: The precision of the decimal type.
                scale: The scale of the decimal type.

            Returns:
                The decimal type.
            """
            self._precision = precision
            self._scale = scale

        def __eq__(self, other: object) -> bool:
            if self == other:
                return True
            if not isinstance(other, Types.DecimalType):
                return False
            return self._precision == other._precision and self._scale == other._scale

        def __hash__(self) -> int:
            return hash((self._precision, self._scale))

        def check_precision_scale(self, precision: int, scale: int) -> None:
            check_argument(
                precision <= 38,
                f"Decimals with precision larger than 38 are not supported: {precision}",
            )
            check_argument(
                scale <= precision,
                f"Scale cannot be larger than precision: {scale} > {precision}",
            )

        def precision(self) -> int:
            """Returns the precision of the decimal type."""
            return self._precision

        def scale(self) -> int:
            """Returns the scale of the decimal type."""
            return self._scale

        def name(self) -> str:
            return Name.DECIMAL

        def simple_string(self) -> str:
            return f"decimal({self._precision},{self._scale})"

    class DateType(DateTimeType):
        """The date type in Gravitino."""

        _instance = None

        @staticmethod
        def get() -> "Types.DateType":
            if Types.DateType._instance is None:
                Types.DateType._instance = Types.DateType()
            return Types.DateType._instance

        def name(self) -> str:
            return Name.DATE

        def simple_string(self) -> str:
            return "date"

    class TimeType(DateTimeType):
        """The time type in Gravitino."""

        _instance = None

        @staticmethod
        def get() -> "Types.TimeType":
            if Types.TimeType._instance is None:
                Types.TimeType._instance = Types.TimeType()
            return Types.TimeType._instance

        def name(self) -> str:
            return Name.TIME

        def simple_string(self) -> str:
            return "time"

    class TimestampType(DateTimeType):
        """The timestamp type in Gravitino."""

        _instance_without_time_zone = None
        _instance_with_time_zone = None

        @staticmethod
        def with_time_zone() -> "Types.TimestampType":
            """Returns the singleton instance of the timestamp type with time zone."""
            if Types.TimestampType._instance_with_time_zone is None:
                Types.TimestampType._instance_with_time_zone = Types.TimestampType(True)
            return Types.TimestampType._instance_with_time_zone

        @staticmethod
        def without_time_zone() -> "Types.TimestampType":
            """Returns the singleton instance of the timestamp type without time zone."""
            if Types.TimestampType._instance_without_time_zone is None:
                Types.TimestampType._instance_without_time_zone = Types.TimestampType(
                    False
                )
            return Types.TimestampType._instance_without_time_zone

        def __init__(self, with_time_zone: bool):
            self._with_time_zone = with_time_zone

        def has_time_zone(self) -> bool:
            """Returns True if the timestamp type has time zone, False otherwise."""
            return self._with_time_zone

        def name(self) -> str:
            return Name.TIMESTAMP

        def simple_string(self) -> str:
            return "timestamp_tz" if self._with_time_zone else "timestamp"

    class IntervalYearType(DateTimeType):
        """The interval year type in Gravitino."""

        _instance = None

        @staticmethod
        def get() -> "Types.IntervalYearType":
            """Returns the singleton instance of the interval year type."""
            if Types.IntervalYearType._instance is None:
                Types.IntervalYearType._instance = Types.IntervalYearType()
            return Types.IntervalYearType._instance

        def name(self) -> str:
            return Name.INTERVAL_YEAR

        def simple_string(self) -> str:
            return "interval_year"

    class IntervalDayType(DateTimeType):
        """The interval day type in Gravitino."""

        _instance = None

        @staticmethod
        def get() -> "Types.IntervalDayType":
            """Returns the singleton instance of the interval day type."""
            if Types.IntervalDayType._instance is None:
                Types.IntervalDayType._instance = Types.IntervalDayType()
            return Types.IntervalDayType._instance

        def name(self) -> str:
            return Name.INTERVAL_DAY

        def simple_string(self) -> str:
            return "interval_day"

    class StringType(PrimitiveType):
        """The string type in Gravitino."""

        _instance = None

        @staticmethod
        def get() -> "Types.StringType":
            if Types.StringType._instance is None:
                Types.StringType._instance = Types.StringType()
            return Types.StringType._instance

        def name(self) -> str:
            return Name.STRING

        def simple_string(self) -> str:
            return "string"

    class UUIDType(PrimitiveType):
        _instance = None

        @staticmethod
        def get() -> "Types.UUIDType":
            if Types.UUIDType._instance is None:
                Types.UUIDType._instance = Types.UUIDType()
            return Types.UUIDType._instance

        def name(self) -> str:
            return Name.UUID

        def simple_string(self) -> str:
            return "uuid"

    class FixedType(PrimitiveType):
        """Fixed-length byte array type in Gravitino."""

        def __init__(self, length: int):
            self._length = length

        @staticmethod
        def of(length: int) -> "Types.FixedType":
            """Returns an instance of FixedType with the given length."""
            return Types.FixedType(length)

        def length(self) -> int:
            """Returns the length of the fixed type."""
            return self._length

        def name(self) -> str:
            return Name.FIXED

        def simple_string(self) -> str:
            return f"fixed({self._length})"

        def __eq__(self, other: object) -> bool:
            if self == other:
                return True
            if not isinstance(other, Types.FixedType):
                return False
            return self._length == other._length

        def __hash__(self) -> int:
            return hash(self._length)

    class VarCharType(PrimitiveType):
        """The varchar type in Gravitino."""

        def __init__(self, length: int):
            """
            Args:
                length: The length of the varchar type.

            Returns:
                The varchar type.
            """
            self._length = length

        @staticmethod
        def of(length: int) -> "Types.VarCharType":
            """Returns an instance of VarCharType with the given length."""
            return Types.VarCharType(length)

        def length(self) -> int:
            """Returns the length of the varchar type."""
            return self._length

        def name(self) -> str:
            return Name.VARCHAR

        def simple_string(self) -> str:
            return f"varchar({self._length})"

        def __eq__(self, other: object) -> bool:
            if self == other:
                return True
            if not isinstance(other, Types.VarCharType):
                return False
            return self._length == other._length

        def __hash__(self) -> int:
            return hash(self._length)

    class VarCharType(PrimitiveType):
        """The varchar type in Gravitino."""

        def __init__(self, length: int):
            self._length = length

        @staticmethod
        def of(length: int) -> "Types.VarCharType":
            """Returns an instance of VarCharType with the given length."""
            return Types.VarCharType(length)

        def length(self) -> int:
            """Returns the length of the varchar type."""
            return self._length

        def name(self) -> str:
            return Name.VARCHAR

        def simple_string(self) -> str:
            return f"varchar({self._length})"

        def __eq__(self, other: object) -> bool:
            if self == other:
                return True
            if not isinstance(other, Types.VarCharType):
                return False
            return self._length == other._length

        def __hash__(self) -> int:
            return hash(self._length)

    class BinaryType(PrimitiveType):
        """The binary type in Gravitino."""

        _instance = None

        @staticmethod
        def get() -> "Types.BinaryType":
            """Returns the singleton instance of BinaryType."""
            if Types.BinaryType._instance is None:
                Types.BinaryType._instance = Types.BinaryType()
            return Types.BinaryType._instance

        def name(self) -> str:
            return Name.BINARY

        def simple_string(self) -> str:
            return "binary"

    class UnparsedType(Type):
        """Represents a type that is not parsed yet."""

        def __init__(self, unparsed_type: str):
            """
            Args:
                unparsed_type: The unparsed type.

            Returns:
                The unparsed type
            """
            self._unparsed_type = unparsed_type

        @staticmethod
        def of(unparsed_type: str) -> "Types.UnparsedType":
            """Creates a new UnparsedType with the given unparsed type."""
            return Types.UnparsedType(unparsed_type)

        def unparsed_type(self) -> str:
            """Returns the unparsed type as a string."""
            return self._unparsed_type

        def name(self) -> str:
            return Name.UNPARSED

        def simple_string(self) -> str:
            return f"unparsed({self._unparsed_type})"

        def __eq__(self, other: object) -> bool:
            if self == other:
                return True
            if not isinstance(other, Types.UnparsedType):
                return False
            return self._unparsed_type == other._unparsed_type

        def __hash__(self) -> int:
            return hash(self._unparsed_type)

        def __str__(self) -> str:
            return self._unparsed_type

    class ExternalType(Type):
        def __init__(self, catalog_string: str):
            self._catalog_string = catalog_string

        def catalog_string(self) -> str:
            return self._catalog_string

        def name(self) -> str:
            return Name.EXTERNAL

        def simple_string(self) -> str:
            return f"external({self._catalog_string})"

        def equals(self, other: object) -> bool:
            if self == other:
                return True
            if not isinstance(other, Types.ExternalType):
                return False
            return self._catalog_string == other._catalog_string

        def __hash__(self) -> int:
            return hash(self._catalog_string)

    class StructType(ComplexType):
        # TODO: to be implemented
        pass

    class ListType(ComplexType):
        # TODO: to be implemented
        pass

    class MapType(ComplexType):
        # TODO: to be implemented
        pass

    class UnionType(ComplexType):
        # TODO: to be implemented
        pass

    @staticmethod
    def allow_auto_increment(data_type: Type) -> bool:
        return isinstance(data_type, Types.IntegerType) or isinstance(
            data_type, Types.LongType
        )
