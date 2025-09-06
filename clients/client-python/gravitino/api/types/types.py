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
# pylint: disable=C0302
from __future__ import annotations

from typing import List

from .type import (
    ComplexType,
    DateTimeType,
    FractionType,
    IntegralType,
    IntervalType,
    Name,
    PrimitiveType,
    Type,
)


class Types:
    """The helper class for Type. It contains all built-in types and provides utility methods."""

    class NullType(Type):
        """The data type representing `NULL` values."""

        _instance: Types.NullType = None

        def __new__(cls):
            if cls._instance is None:
                cls._instance = super(Types.NullType, cls).__new__(cls)
            return cls._instance

        @classmethod
        def get(cls) -> Types.NullType:
            return cls()

        def name(self) -> Name:
            return Name.NULL

        def simple_string(self) -> str:
            return "null"

    class BooleanType(PrimitiveType):
        """The boolean type in Gravitino."""

        _instance: Types.BooleanType = None

        def __new__(cls):
            if cls._instance is None:
                cls._instance = super(Types.BooleanType, cls).__new__(cls)
            return cls._instance

        @classmethod
        def get(cls) -> Types.BooleanType:
            return cls()

        def name(self) -> Name:
            return Name.BOOLEAN

        def simple_string(self) -> str:
            return "boolean"

    class ByteType(IntegralType):
        """The byte type in Gravitino."""

        _instance: Types.ByteType = None
        _unsigned_instance: Types.ByteType = None

        def __new__(cls, signed: bool = True):
            if signed:
                if cls._instance is None:
                    cls._instance = super(Types.ByteType, cls).__new__(cls)
                    cls._instance.__init__(signed)
                return cls._instance
            if cls._unsigned_instance is None:
                cls._unsigned_instance = super(Types.ByteType, cls).__new__(cls)
                cls._unsigned_instance.__init__(signed)
            return cls._unsigned_instance

        @classmethod
        def get(cls) -> Types.ByteType:
            return cls(True)

        @classmethod
        def unsigned(cls) -> Types.ByteType:
            return cls(False)

        def name(self) -> Name:
            return Name.BYTE

        def simple_string(self) -> str:
            return "byte" if self.signed() else "byte unsigned"

    class ShortType(IntegralType):
        _instance: Types.ShortType = None
        _unsigned_instance: Types.ShortType = None

        def __new__(cls, signed=True):
            if signed:
                if cls._instance is None:
                    cls._instance = super(Types.ShortType, cls).__new__(cls)
                    cls._instance.__init__(signed)
                return cls._instance
            if cls._unsigned_instance is None:
                cls._unsigned_instance = super(Types.ShortType, cls).__new__(cls)
                cls._unsigned_instance.__init__(signed)
            return cls._unsigned_instance

        @classmethod
        def get(cls) -> Types.ShortType:
            return cls(True)

        @classmethod
        def unsigned(cls) -> Types.ShortType:
            return cls(False)

        def name(self) -> Name:
            return Name.SHORT

        def simple_string(self) -> str:
            return "short" if self.signed() else "short unsigned"

    class IntegerType(IntegralType):
        _instance: Types.IntegerType = None
        _unsigned_instance: Types.IntegerType = None

        def __new__(cls, signed=True):
            if signed:
                if cls._instance is None:
                    cls._instance = super(Types.IntegerType, cls).__new__(cls)
                    cls._instance.__init__(signed)
                return cls._instance
            if cls._unsigned_instance is None:
                cls._unsigned_instance = super(Types.IntegerType, cls).__new__(cls)
                cls._unsigned_instance.__init__(signed)
            return cls._unsigned_instance

        @classmethod
        def get(cls) -> Types.IntegerType:
            return cls(True)

        @classmethod
        def unsigned(cls):
            return cls(False)

        def name(self) -> Name:
            return Name.INTEGER

        def simple_string(self) -> str:
            return "integer" if self.signed() else "integer unsigned"

    class LongType(IntegralType):
        _instance: Types.LongType = None
        _unsigned_instance: Types.LongType = None

        def __new__(cls, signed=True):
            if signed:
                if cls._instance is None:
                    cls._instance = super(Types.LongType, cls).__new__(cls)
                    cls._instance.__init__(signed)
                return cls._instance
            if cls._unsigned_instance is None:
                cls._unsigned_instance = super(Types.LongType, cls).__new__(cls)
                cls._unsigned_instance.__init__(signed)
            return cls._unsigned_instance

        @classmethod
        def get(cls) -> Types.LongType:
            return cls(True)

        @classmethod
        def unsigned(cls):
            return cls(False)

        def name(self) -> Name:
            return Name.LONG

        def simple_string(self) -> str:
            return "long" if self.signed() else "long unsigned"

    class FloatType(FractionType):
        _instance: Types.FloatType = None

        def __new__(cls):
            if cls._instance is None:
                cls._instance = super(Types.FloatType, cls).__new__(cls)
                cls._instance.__init__()
            return cls._instance

        @classmethod
        def get(cls) -> Types.FloatType:
            return cls()

        def name(self) -> Name:
            return Name.FLOAT

        def simple_string(self) -> str:
            return "float"

    class DoubleType(FractionType):
        _instance: Types.DoubleType = None

        def __new__(cls):
            if cls._instance is None:
                cls._instance = super(Types.DoubleType, cls).__new__(cls)
                cls._instance.__init__()
            return cls._instance

        @classmethod
        def get(cls) -> Types.DoubleType:
            return cls()

        def name(self) -> Name:
            return Name.DOUBLE

        def simple_string(self) -> str:
            return "double"

    class DecimalType(FractionType):
        """The decimal type in Gravitino."""

        MAX_PRECISION = 38
        _precision: int
        _scale: int

        def __init__(self, precision: int, scale: int):
            """
            Args:
                precision: The precision of the decimal type.
                scale: The scale of the decimal type.
            """
            super().__init__()
            self.check_precision_scale(precision, scale)
            self._precision = precision
            self._scale = scale

        @staticmethod
        def check_precision_scale(precision: int, scale: int):
            """
            Ensures the precision and scale values are within valid range.

            Args:
                precision: The precision of the decimal.
                scale: The scale of the decimal.
            """
            if not 1 <= precision <= Types.DecimalType.MAX_PRECISION:
                raise ValueError(
                    f"Decimal precision must be in range [1, 38]: {precision}"
                )
            if not 0 <= scale <= precision:
                raise ValueError(
                    f"Decimal scale must be in range [0, precision ({precision})]: {scale}"
                )

        @classmethod
        def of(cls, precision: int, scale: int) -> Types.DecimalType:
            return cls(precision, scale)

        def name(self) -> Name:
            return Name.DECIMAL

        def precision(self) -> int:
            return self._precision

        def scale(self) -> int:
            return self._scale

        def simple_string(self) -> str:
            return f"decimal({self._precision},{self._scale})"

        def __eq__(self, other):
            """
            Compares two DecimalType objects for equality.

            Args:
                other: The other DecimalType to compare with.

            Returns:
                True if both objects have the same precision and scale, False otherwise.
            """
            if not isinstance(other, Types.DecimalType):
                return False
            return self._precision == other._precision and self._scale == other._scale

        def __hash__(self):
            return hash((self._precision, self._scale))

    class DateType(DateTimeType):
        """The date time type in Gravitino."""

        _instance: Types.DateType = None

        def __new__(cls):
            if cls._instance is None:
                cls._instance = super(Types.DateType, cls).__new__(cls)
                cls._instance.__init__()
            return cls._instance

        @classmethod
        def get(cls) -> Types.DateType:
            return cls()

        def name(self) -> Name:
            return Name.DATE

        def simple_string(self) -> str:
            return "date"

    class TimeType(DateTimeType):
        _instance: Types.TimeType = None
        _precision: int

        def __new__(cls, precision: int = None):
            if precision is None:
                # Forward compatibility: Use singleton when there is no precision parameter
                if cls._instance is None:
                    cls._instance = super(Types.TimeType, cls).__new__(cls)
                    cls._instance.__init__()
                return cls._instance
            # Create a new instance when the precision parameter is present
            return super(Types.TimeType, cls).__new__(cls)

        def __init__(self, precision: int = None):
            if hasattr(self, "_initialized"):
                return
            super().__init__()
            self._precision = (
                precision if precision is not None else self.DATE_TIME_PRECISION_NOT_SET
            )
            self._initialized = True

        @classmethod
        def get(cls) -> Types.TimeType:
            """Returns the default TimeType instance (without precision)"""
            return cls()

        @classmethod
        def of(cls, precision: int) -> Types.TimeType:
            """Create a TimeType instance with the specified precision"""
            if not (
                cls.MIN_ALLOWED_PRECISION <= precision <= cls.MAX_ALLOWED_PRECISION
            ):
                raise ValueError(
                    f"precision must be in range "
                    f"[{cls.MIN_ALLOWED_PRECISION}, {cls.MAX_ALLOWED_PRECISION}]: "
                    f"precision: {precision}"
                )
            return cls(precision)

        def precision(self) -> int:
            """Returns the precision of the time type"""
            return self._precision

        def has_precision_set(self) -> bool:
            """Returns whether the time type has precision set"""
            return self._precision != self.DATE_TIME_PRECISION_NOT_SET

        def name(self) -> Name:
            return Name.TIME

        def simple_string(self) -> str:
            return f"time({self._precision})" if self.has_precision_set() else "time"

        def __eq__(self, other):
            if not isinstance(other, Types.TimeType):
                return False
            return self._precision == other._precision

        def __hash__(self):
            return hash(self._precision)

    class TimestampType(DateTimeType):
        _instance_with_tz: Types.TimestampType = None
        _instance_without_tz: Types.TimestampType = None
        _with_time_zone: bool
        _precision: int

        def __new__(cls, with_time_zone: bool, precision: int = None):
            if precision is None:
                # Use singleton when there is no precision parameter
                if with_time_zone:
                    if cls._instance_with_tz is None:
                        cls._instance_with_tz = super(Types.TimestampType, cls).__new__(
                            cls
                        )
                        cls._instance_with_tz.__init__(with_time_zone)
                    return cls._instance_with_tz
                if cls._instance_without_tz is None:
                    cls._instance_without_tz = super(Types.TimestampType, cls).__new__(
                        cls
                    )
                    cls._instance_without_tz.__init__(with_time_zone)
                return cls._instance_without_tz
            # Create a new instance when the precision parameter is present
            return super(Types.TimestampType, cls).__new__(cls)

        def __init__(self, with_time_zone: bool, precision: int = None):
            if hasattr(self, "_initialized"):
                return
            super().__init__()
            self._with_time_zone = with_time_zone
            self._precision = (
                precision if precision is not None else self.DATE_TIME_PRECISION_NOT_SET
            )
            self._initialized = True

        @classmethod
        def with_time_zone(cls, precision: int = None) -> Types.TimestampType:
            """Create TimestampType with Timezone"""
            if precision is not None:
                if not (
                    cls.MIN_ALLOWED_PRECISION <= precision <= cls.MAX_ALLOWED_PRECISION
                ):
                    raise ValueError(
                        f"precision must be in range "
                        f"[{cls.MIN_ALLOWED_PRECISION}, {cls.MAX_ALLOWED_PRECISION}]: "
                        f"precision: {precision}"
                    )
                return cls(True, precision)
            return cls(True)

        @classmethod
        def without_time_zone(cls, precision: int = None) -> Types.TimestampType:
            """Create TimestampType without Timezone"""
            if precision is not None:
                if not (
                    cls.MIN_ALLOWED_PRECISION <= precision <= cls.MAX_ALLOWED_PRECISION
                ):
                    raise ValueError(
                        f"precision must be in range "
                        f"[{cls.MIN_ALLOWED_PRECISION}, {cls.MAX_ALLOWED_PRECISION}]: "
                        f"precision: {precision}"
                    )
                return cls(False, precision)
            return cls(False)

        def has_time_zone(self) -> bool:
            """Returns whether the timestamp type has a timezone"""
            return self._with_time_zone

        def precision(self) -> int:
            """Returns the precision of the timestamp type"""
            return self._precision

        def has_precision_set(self) -> bool:
            """Returns whether the precision is set for the timestamp type"""
            return self._precision != self.DATE_TIME_PRECISION_NOT_SET

        def name(self) -> Name:
            return Name.TIMESTAMP

        def simple_string(self) -> str:
            if self.has_precision_set():
                return (
                    f"timestamp_tz({self._precision})"
                    if self._with_time_zone
                    else f"timestamp({self._precision})"
                )
            return "timestamp_tz" if self._with_time_zone else "timestamp"

        def __eq__(self, other):
            if not isinstance(other, Types.TimestampType):
                return False
            return (
                self._with_time_zone == other._with_time_zone
                and self._precision == other._precision
            )

        def __hash__(self):
            return hash((self._with_time_zone, self._precision))

    class IntervalYearType(IntervalType):
        """The interval year type in Gravitino."""

        _instance: Types.IntervalYearType = None

        def __new__(cls):
            if cls._instance is None:
                cls._instance = super(Types.IntervalYearType, cls).__new__(cls)
                cls._instance.__init__()
            return cls._instance

        @classmethod
        def get(cls) -> Types.IntervalYearType:
            return cls()

        def name(self) -> Name:
            return Name.INTERVAL_YEAR

        def simple_string(self) -> str:
            return "interval_year"

    class IntervalDayType(IntervalType):
        """The interval day type in Gravitino."""

        _instance: Types.IntervalDayType = None

        def __new__(cls):
            if cls._instance is None:
                cls._instance = super(Types.IntervalDayType, cls).__new__(cls)
                cls._instance.__init__()
            return cls._instance

        @classmethod
        def get(cls) -> Types.IntervalDayType:
            return cls()

        def name(self) -> Name:
            return Name.INTERVAL_DAY

        def simple_string(self) -> str:
            return "interval_day"

    class StringType(PrimitiveType):
        """The string type in Gravitino, equivalent to varchar(MAX),
        which the MAX is determined by the underlying catalog."""

        _instance: Types.StringType = None

        def __new__(cls):
            if cls._instance is None:
                cls._instance = super(Types.StringType, cls).__new__(cls)
                cls._instance.__init__()
            return cls._instance

        @classmethod
        def get(cls) -> Types.StringType:
            return cls()

        def name(self) -> Name:
            return Name.STRING

        def simple_string(self) -> str:
            return "string"

    class UUIDType(PrimitiveType):
        """The uuid type in Gravitino."""

        _instance: Types.UUIDType = None

        def __new__(cls):
            if cls._instance is None:
                cls._instance = super(Types.UUIDType, cls).__new__(cls)
                cls._instance.__init__()
            return cls._instance

        @classmethod
        def get(cls) -> Types.UUIDType:
            return cls()

        def name(self) -> Name:
            return Name.UUID

        def simple_string(self) -> str:
            return "uuid"

    class FixedType(PrimitiveType):
        """Fixed-length byte array type, if you want to use variable-length
        byte array, use BinaryType instead."""

        _length: int

        def __init__(self, length: int):
            """
            Initializes the FixedType with the given length.

            Args:
                length: The length of the fixed type.
            """
            self._length = length

        @classmethod
        def of(cls, length: int) -> Types.FixedType:
            """
            Args:
                length: The length of the fixed type.

            Returns:
                 A FixedType instance with the given length.
            """
            return cls(length)

        def name(self) -> Name:
            return Name.FIXED

        def length(self) -> int:
            return self._length

        def simple_string(self) -> str:
            return f"fixed({self._length})"

        def __eq__(self, other):
            """
            Compares two FixedType objects for equality.

            Args:
                other: The other FixedType object to compare with.

            Returns:
                True if both FixedType objects have the same length, False otherwise.
            """
            if not isinstance(other, Types.FixedType):
                return False
            return self._length == other._length

        def __hash__(self):
            return hash(self._length)

    class VarCharType(PrimitiveType):
        """The varchar type in Gravitino."""

        _length: int

        def __init__(self, length: int):
            self._length = length

        @classmethod
        def of(cls, length: int) -> Types.VarCharType:
            return cls(length)

        def name(self) -> Name:
            return Name.VARCHAR

        def length(self) -> int:
            return self._length

        def simple_string(self) -> str:
            return f"varchar({self._length})"

        def __eq__(self, other):
            """
            Compares two VarCharType objects for equality.

            Args:
                other: The other VarCharType object to compare with.

            Returns:
                True if both VarCharType objects have the same length, False otherwise.
            """
            if isinstance(other, Types.VarCharType):
                return self._length == other._length
            return False

        def __hash__(self):
            return hash(self._length)

    class FixedCharType(PrimitiveType):
        """The fixed char type in Gravitino."""

        _length: int

        def __init__(self, length: int):
            self._length = length

        @classmethod
        def of(cls, length: int) -> Types.FixedCharType:
            return cls(length)

        def name(self) -> Name:
            return Name.FIXEDCHAR

        def length(self) -> int:
            return self._length

        def simple_string(self) -> str:
            return f"char({self._length})"

        def __eq__(self, other):
            if not isinstance(other, Types.FixedCharType):
                return False
            return self._length == other._length

        def __hash__(self):
            return hash(self._length)

    class BinaryType(PrimitiveType):
        _instance: Types.BinaryType = None

        def __new__(cls):
            if cls._instance is None:
                cls._instance = super(Types.BinaryType, cls).__new__(cls)
                cls._instance.__init__()
            return cls._instance

        @classmethod
        def get(cls) -> Types.BinaryType:
            return cls()

        def name(self) -> Name:
            return Name.BINARY

        def simple_string(self) -> str:
            return "binary"

    class StructType(ComplexType):
        """The struct type in Gravitino."""

        _fields: List[Field]

        def __init__(self, fields: List[Field]):
            if not fields or len(fields) == 0:
                raise ValueError("fields cannot be null or empty")
            self._fields = fields

        @classmethod
        def of(cls, *fields) -> Types.StructType:
            """
            Args:
                fields: The fields of the struct type.

            Returns:
                A StructType instance with the given fields.
            """
            return cls(list(fields))

        def fields(self) -> List[Field]:
            return self._fields

        def name(self) -> Name:
            return Name.STRUCT

        def simple_string(self) -> str:
            return (
                f"struct<{', '.join(field.simple_string() for field in self._fields)}>"
            )

        def __eq__(self, other):
            """
            Compares two StructType objects for equality.

            Args:
                other: The other StructType object to compare with.

            Returns:
                True if both StructType objects have the same fields, False otherwise.
            """
            if isinstance(other, Types.StructType):
                return self._fields == other._fields
            return False

        def __hash__(self):
            return hash(tuple(self._fields))

        class Field:
            _name: str
            _type: Type
            _nullable: bool
            _comment: str

            def __init__(
                self, name: str, field_type: Type, nullable: bool, comment: str
            ) -> None:
                """
                Initializes the Field with the given name, type, nullable flag, and comment.

                Args:
                    name: The name of the field.
                    field_type: The type of the field.
                    nullable: Whether the field is nullable.
                    comment: The comment of the field (optional).
                """
                if name is None:
                    raise ValueError("name cannot be null")
                if type is None:
                    raise ValueError("type cannot be null")
                self._name = name
                self._type = field_type
                self._nullable = nullable
                self._comment = comment

            @classmethod
            def not_null_field(
                cls, name: str, field_type: Type, comment: str = None
            ) -> Types.StructType.Field:
                """
                Args:
                    name: The name of the field.
                    field_type: The type of the field.
                    comment: The comment of the field (optional).
                """
                return cls(name, field_type, False, comment)

            @classmethod
            def nullable_field(
                cls, name: str, field_type: Type, comment: str = None
            ) -> Types.StructType.Field:
                """
                Args:
                    name: The name of the field.
                    field_type: The type of the field.
                    comment: The comment of the field (optional).

                Returns:
                    A nullable Field instance with the given name, field_type, and comment.
                """
                return cls(name, field_type, True, comment)

            def name(self):
                return self._name

            def type(self):
                return self._type

            def nullable(self):
                return self._nullable

            def comment(self):
                return self._comment

            def __eq__(self, other):
                """
                Compares two Field objects for equality.

                Args:
                    other: The other Field object to compare with.

                Returns:
                    True if both Field objects have the same attributes, False otherwise.
                """
                if isinstance(other, Types.StructType.Field):
                    return (
                        self._name == other._name
                        and self._type == other._type
                        and self._nullable == other._nullable
                        and self._comment == other._comment
                    )
                return False

            def __hash__(self):
                return hash((self._name, self._type, self._nullable))

            def simple_string(self) -> str:
                nullable_str = "NULL" if self._nullable else "NOT NULL"
                comment_str = f" COMMENT '{self._comment}'" if self._comment else ""
                return f"{self._name}: {self._type.simple_string()} {nullable_str}{comment_str}"

    class ListType(ComplexType):
        """The list type in Gravitino."""

        _element_type: Type
        _element_nullable: bool

        def __init__(self, element_type: Type, element_nullable: bool):
            """
            Create a new ListType with the given element type and the type is nullable.

            Args:
                element_type: The element type of the list.
                element_nullable: Whether the element of the list is nullable.
            """
            if element_type is None:
                raise ValueError("element_type cannot be null")
            self._element_type = element_type
            self._element_nullable = element_nullable

        @classmethod
        def nullable(cls, element_type: Type) -> Types.ListType:
            """
            Create a new ListType with the given element type and the type is nullable.

            Args:
                element_type: The element type of the list.

            Returns:
                A new ListType instance.
            """
            return cls.of(element_type, True)

        @classmethod
        def not_null(cls, element_type: Type) -> Types.ListType:
            """
            Create a new ListType with the given element type.

            Args:
                element_type: The element type of the list.

            Returns:
                A new ListType instance.
            """
            return cls.of(element_type, False)

        @classmethod
        def of(cls, element_type: Type, element_nullable: bool) -> Types.ListType:
            """
            Create a new ListType with the given element type and whether the element is nullable.

            Args:
                element_type: The element type of the list.
                element_nullable: Whether the element of the list is nullable.

            Returns
                A new ListType instance.
            """
            return cls(element_type, element_nullable)

        def element_type(self) -> Type:
            return self._element_type

        def element_nullable(self) -> bool:
            return self._element_nullable

        def name(self) -> Name:
            return Name.LIST

        def simple_string(self) -> str:
            return (
                f"list<{self._element_type.simple_string()}>"
                if self._element_nullable
                else f"list<{self._element_type.simple_string()}, NOT NULL>"
            )

        def __eq__(self, other):
            if isinstance(other, Types.ListType):
                return (
                    self._element_nullable == other.element_nullable()
                    and self._element_type == other.element_type()
                )
            return False

        def __hash__(self):
            return hash((self._element_type, self._element_nullable))

    class MapType(ComplexType):
        """The map type in Gravitino."""

        _key_type: Type
        _value_type: Type
        _value_nullable: bool

        def __init__(self, key_type: Type, value_type: Type, value_nullable: bool):
            """
            Create a new MapType with the given key type, value type and the value is nullable.

            Args:
                key_type: The key type of the map.
                value_type: The value type of the map.
                value_nullable: Whether the value of the map is nullable.
            """
            self._key_type = key_type
            self._value_type = value_type
            self._value_nullable = value_nullable

        @classmethod
        def value_nullable(cls, key_type: Type, value_type: Type) -> Types.MapType:
            """
            Create a new MapType with the given key type, value type, and the value is nullable.

            Args:
                key_type: The key type of the map.
                value_type: The value type of the map.

            Returns:
                A new MapType instance.
            """
            return cls.of(key_type, value_type, True)

        @classmethod
        def value_not_null(cls, key_type: Type, value_type: Type) -> Types.MapType:
            """
            Create a new MapType with the given key type, value type, and the value is not nullable.

            Args:
                key_type: The key type of the map.
                value_type: The value type of the map.

            Returns:
                A new MapType instance.
            """
            return cls.of(key_type, value_type, False)

        @classmethod
        def of(
            cls, key_type: Type, value_type: Type, value_nullable: bool
        ) -> Types.MapType:
            """
            Create a new MapType with the given key type, value type, and whether the value is nullable.

            Args:
                key_type: The key type of the map.
                value_type: The value type of the map.
                value_nullable: Whether the value of the map is nullable.

            Returns:
                A new MapType instance.
            """
            return cls(key_type, value_type, value_nullable)

        def key_type(self) -> Type:
            return self._key_type

        def value_type(self) -> Type:
            return self._value_type

        def is_value_nullable(self) -> bool:
            return self._value_nullable

        def name(self) -> Name:
            return Name.MAP

        def simple_string(self) -> str:
            return f"map<{self._key_type.simple_string()}, {self._value_type.simple_string()}>"

        def __eq__(self, other):
            """
            Compares two MapType objects for equality.

            Args:
                other The other MapType object to compare with.

            Returns:
                True if both MapType objects have the same key type, value type, and nullability, False otherwise.
            """
            if isinstance(other, Types.MapType):
                return (
                    self._value_nullable == other._value_nullable
                    and self._key_type == other._key_type
                    and self._value_type == other._value_type
                )
            return False

        def __hash__(self):
            return hash((self._key_type, self._value_type, self._value_nullable))

    class UnionType(ComplexType):
        """The union type in Gravitino."""

        _types: list[Type]

        def __init__(self, types: list[Type]):
            """
            Create a new UnionType with the given types.

            Args:
                types The types of the union.
            """
            self._types = types

        @classmethod
        def of(cls, *types: Type) -> Types.UnionType:
            """
            Create a new UnionType with the given types.

            Args:
                types: The types of the union.

            Returns:
                A new UnionType instance.
            """
            return Types.UnionType(list(types))

        def types(self) -> list:
            return self._types

        def name(self) -> Name:
            return Name.UNION

        def simple_string(self) -> str:
            return f"union<{', '.join(t.simple_string() for t in self._types)}>"

        def __eq__(self, other):
            """
            Compares two UnionType objects for equality.

            Args:
                other The other UnionType object to compare with.

            Returns:
                True if both UnionType objects have the same types, False otherwise.
            """
            if isinstance(other, Types.UnionType):
                return self._types == other.types()
            return False

        def __hash__(self):
            return hash(tuple(self._types))

    class UnparsedType(Type):
        """Represents a type that is not parsed yet. The parsed type is represented by other types of types."""

        _unparsed_type: str

        def __init__(self, unparsed_type: str):
            """
            Initializes an unparsed_type instance.

            Args:
                unparsed_type: The unparsed type as a string.
            """
            self._unparsed_type = unparsed_type

        @classmethod
        def of(cls, unparsed_type: str) -> Types.UnparsedType:
            """
            Creates a new unparsed_type with the given unparsed type.

            Args:
                unparsed_type The unparsed type.

            Returns
                A new unparsed_type instance.
            """
            return cls(unparsed_type)

        def unparsed_type(self) -> str:
            return self._unparsed_type

        def name(self) -> Name:
            return Name.UNPARSED

        def simple_string(self) -> str:
            return f"unparsed({self._unparsed_type})"

        def __eq__(self, other):
            """
            Compares two unparsed_type objects for equality.

            Args:
                other: The other unparsed_type object to compare with.

            Returns:
                True if both unparsed_type objects have the same unparsed type string, False otherwise.
            """
            if isinstance(other, Types.UnparsedType):
                return self._unparsed_type == other.unparsed_type()
            return False

        def __hash__(self):
            return hash(self._unparsed_type)

        def __str__(self):
            return self._unparsed_type

    class ExternalType(Type):
        """Represents a type that is defined in an external catalog."""

        _catalog_string: str

        def __init__(self, catalog_string: str):
            """
            Initializes an ExternalType instance.

            Args:
                catalog_string The string representation of this type in the catalog.
            """
            self._catalog_string = catalog_string

        @classmethod
        def of(cls, catalog_string: str) -> Types.ExternalType:
            """
            Creates a new ExternalType with the given catalog string.

            Args:
                catalog_string The string representation of this type in the catalog.

            Returns:
                A new ExternalType instance.
            """
            return cls(catalog_string)

        def catalog_string(self) -> str:
            return self._catalog_string

        def name(self) -> Name:
            return Name.EXTERNAL

        def simple_string(self) -> str:
            return f"external({self._catalog_string})"

        def __eq__(self, other):
            """
            Compares two ExternalType objects for equality.

            Args:
                other: The other ExternalType object to compare with.

            Returns:
                True if both ExternalType objects have the same catalog string, False otherwise.
            """
            if not isinstance(other, Types.ExternalType):
                return False
            return self._catalog_string == other._catalog_string

        def __hash__(self):
            return hash(self._catalog_string)

        def __str__(self):
            return self.simple_string()

    @staticmethod
    def allow_auto_increment(data_type: Type) -> bool:
        """
        Checks if the given data type is allowed to be an auto-increment column.

        Args:
            data_type The data type to check.

        Returns:
            True if the given data type is allowed to be an auto-increment column, False otherwise.
        """
        return isinstance(data_type, (Types.IntegerType, Types.LongType))
