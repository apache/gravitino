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
from .type import (
    Type,
    Name,
    PrimitiveType,
    IntegralType,
    FractionType,
    DateTimeType,
    IntervalType,
    ComplexType,
)


class Types:

    class NullType(Type):
        _instance = None

        @classmethod
        def get(cls):
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def name(self) -> Name:
            return Name.NULL

        def simple_string(self) -> str:
            return "null"

    class BooleanType(PrimitiveType):
        _instance = None

        @classmethod
        def get(cls):
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def name(self) -> Name:
            return Name.BOOLEAN

        def simple_string(self) -> str:
            return "boolean"

    class ByteType(IntegralType):
        _instance = None
        _unsigned_instance = None

        def __init__(self, signed: bool):
            super().__init__(signed)
            self._signed = signed

        @classmethod
        def get(cls):
            if cls._instance is None:
                cls._instance = cls(True)
            return cls._instance

        @classmethod
        def unsigned(cls):
            if cls._unsigned_instance is None:
                cls._unsigned_instance = cls(False)
            return cls._unsigned_instance

        def name(self) -> Name:
            return Name.BYTE

        def simple_string(self) -> str:
            return "byte" if self.signed() else "byte unsigned"

    class ShortType(IntegralType):
        _instance = None
        _unsigned_instance = None

        def __init__(self, signed: bool):
            super().__init__(signed)
            self._signed = signed

        @classmethod
        def get(cls):
            if cls._instance is None:
                cls._instance = cls(True)
            return cls._instance

        @classmethod
        def unsigned(cls):
            if cls._unsigned_instance is None:
                cls._unsigned_instance = cls(False)
            return cls._unsigned_instance

        def name(self) -> Name:
            return Name.SHORT

        def simple_string(self) -> str:
            return "short" if self.signed() else "short unsigned"

    class IntegerType(IntegralType):
        _instance = None
        _unsigned_instance = None

        def __init__(self, signed: bool):
            super().__init__(signed)
            self._signed = signed

        @classmethod
        def get(cls):
            if cls._instance is None:
                cls._instance = cls(True)
            return cls._instance

        @classmethod
        def unsigned(cls):
            if cls._unsigned_instance is None:
                cls._unsigned_instance = cls(False)
            return cls._unsigned_instance

        def name(self) -> Name:
            return Name.INTEGER

        def simple_string(self) -> str:
            return "integer" if self.signed() else "integer unsigned"

    class LongType(IntegralType):
        _instance = None
        _unsigned_instance = None

        def __init__(self, signed: bool):
            super().__init__(signed)
            self._signed = signed

        @classmethod
        def get(cls):
            if cls._instance is None:
                cls._instance = cls(True)
            return cls._instance

        @classmethod
        def unsigned(cls):
            if cls._unsigned_instance is None:
                cls._unsigned_instance = cls(False)
            return cls._unsigned_instance

        def name(self) -> Name:
            return Name.LONG

        def simple_string(self) -> str:
            return "long" if self.signed() else "long unsigned"

    class FloatType(FractionType):
        _instance = None

        @classmethod
        def get(cls):
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def name(self) -> Name:
            return Name.FLOAT

        def simple_string(self) -> str:
            return "float"

    class DoubleType(FractionType):
        _instance = None

        @classmethod
        def get(cls):
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def name(self) -> Name:
            return Name.DOUBLE

        def simple_string(self) -> str:
            return "double"

    class DecimalType(FractionType):
        @classmethod
        def of(cls, precision: int, scale: int):
            return cls(precision, scale)

        def __init__(self, precision: int, scale: int):
            """
            @param precision: The precision of the decimal type.
            @param scale: The scale of the decimal type.
            """
            super().__init__()
            self.check_precision_scale(precision, scale)
            self._precision = precision
            self._scale = scale

        @staticmethod
        def check_precision_scale(precision: int, scale: int):
            """
            Ensures the precision and scale values are within valid range.
            @param precision: The precision of the decimal.
            @param scale: The scale of the decimal.
            """
            if not 1 <= precision <= 38:
                raise ValueError(
                    f"Decimal precision must be in range [1, 38]: {precision}"
                )
            if not 0 <= scale <= precision:
                raise ValueError(
                    f"Decimal scale must be in range [0, precision ({precision})]: {scale}"
                )

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

            @param other: The other DecimalType to compare with.
            @return: True if both objects have the same precision and scale, False otherwise.
            """
            if not isinstance(other, Types.DecimalType):
                return False
            return self._precision == other._precision and self._scale == other._scale

        def __hash__(self):
            return hash((self._precision, self._scale))

    class DateType(DateTimeType):
        _instance = None

        @classmethod
        def get(cls):
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def name(self) -> Name:
            return Name.DATE

        def simple_string(self) -> str:
            return "date"

    class TimeType(DateTimeType):
        _instance = None

        @classmethod
        def get(cls):
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def name(self) -> Name:
            return Name.TIME

        def simple_string(self) -> str:
            return "time"

    class TimestampType(DateTimeType):
        _instance_with_tz = None
        _instance_without_tz = None

        @classmethod
        def with_time_zone(cls):
            if cls._instance_with_tz is None:
                cls._instance_with_tz = cls(True)
            return cls._instance_with_tz

        @classmethod
        def without_time_zone(cls):
            if cls._instance_without_tz is None:
                cls._instance_without_tz = cls(False)
            return cls._instance_without_tz

        def __init__(self, with_time_zone: bool):
            self._with_time_zone = with_time_zone

        def has_time_zone(self) -> bool:
            return self._with_time_zone

        def name(self) -> Name:
            return Name.TIMESTAMP

        def simple_string(self) -> str:
            return "timestamp_tz" if self._with_time_zone else "timestamp"

    class IntervalYearType(IntervalType):
        _instance = None

        @classmethod
        def get(cls):
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def name(self) -> Name:
            return Name.INTERVAL_YEAR

        def simple_string(self) -> str:
            return "interval_year"

    class IntervalDayType(IntervalType):
        _instance = None

        @classmethod
        def get(cls):
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def name(self) -> Name:
            return Name.INTERVAL_DAY

        def simple_string(self) -> str:
            return "interval_day"

    class StringType(PrimitiveType):
        _instance = None

        @classmethod
        def get(cls):
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def name(self) -> Name:
            return Name.STRING

        def simple_string(self) -> str:
            return "string"

    class UUIDType(PrimitiveType):
        _instance = None

        @classmethod
        def get(cls):
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def name(self) -> Name:
            return Name.UUID

        def simple_string(self) -> str:
            return "uuid"

    class FixedType(PrimitiveType):
        def __init__(self, length: int):
            """
            Initializes the FixedType with the given length.

            @param length: The length of the fixed type.
            """
            self._length = length

        @classmethod
        def of(cls, length: int):
            """
            @param length: The length of the fixed type.
            @return: A FixedType instance with the given length.
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

            @param other: The other FixedType object to compare with.
            @return: True if both FixedType objects have the same length, False otherwise.
            """
            if not isinstance(other, Types.FixedType):
                return False
            return self._length == other._length

        def __hash__(self):
            return hash(self._length)

    class VarCharType(PrimitiveType):
        def __init__(self, length: int):
            """
            Initializes the VarCharType with the given length.

            @param length: The length of the varchar type.
            """
            self._length = length

        @classmethod
        def of(cls, length: int):
            """
            @param length: The length of the varchar type.
            @return: A VarCharType instance with the given length.
            """
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

            @param other: The other VarCharType object to compare with.
            @return: True if both VarCharType objects have the same length, False otherwise.
            """
            if not isinstance(other, Types.VarCharType):
                return False
            return self._length == other._length

        def __hash__(self):
            return hash(self._length)

    class FixedCharType(PrimitiveType):
        def __init__(self, length: int):
            """
            Initializes the FixedCharType with the given length.

            @param length: The length of the fixed char type.
            """
            self._length = length

        @classmethod
        def of(cls, length: int):
            """
            @param length: The length of the fixed char type.
            @return: A FixedCharType instance with the given length.
            """
            return cls(length)

        def name(self) -> Name:
            return Name.FIXEDCHAR

        def length(self) -> int:
            return self._length

        def simple_string(self) -> str:
            return f"char({self._length})"

        def __eq__(self, other):
            """
            Compares two FixedCharType objects for equality.

            @param other: The other FixedCharType object to compare with.
            @return: True if both FixedCharType objects have the same length, False otherwise.
            """
            if not isinstance(other, Types.FixedCharType):
                return False
            return self._length == other._length

        def __hash__(self):
            return hash(self._length)

    class BinaryType(PrimitiveType):
        _instance = None

        @classmethod
        def get(cls):
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def name(self) -> Name:
            return Name.BINARY

        def simple_string(self) -> str:
            return "binary"

    class StructType(ComplexType):
        def __init__(self, fields):
            """
            Initializes the StructType with the given fields.

            @param fields: The fields of the struct type.
            """
            if not fields or len(fields) == 0:
                raise ValueError("fields cannot be null or empty")
            self._fields = fields

        @classmethod
        def of(cls, *fields):
            """
            @param fields: The fields of the struct type.
            @return: A StructType instance with the given fields.
            """
            return cls(fields)

        def fields(self):
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

            @param other: The other StructType object to compare with.
            @return: True if both StructType objects have the same fields, False otherwise.
            """
            if not isinstance(other, Types.StructType):
                return False
            return self._fields == other._fields

        def __hash__(self):
            return hash(tuple(self._fields))

        class Field:
            def __init__(self, name, field_type, nullable, comment=None):
                """
                Initializes the Field with the given name, type, nullable flag, and comment.

                @param name: The name of the field.
                @param field_type: The type of the field.
                @param nullable: Whether the field is nullable.
                @param comment: The comment of the field (optional).
                """
                if name is None:
                    raise ValueError("name cannot be null")
                if type is None:
                    raise ValueError("type cannot be null")
                self._name = name
                self._field_type = field_type
                self._nullable = nullable
                self._comment = comment

            @classmethod
            def not_null_field(cls, name, field_type, comment=None):
                """
                @param name: The name of the field.
                @param field_type: The type of the field.
                @param comment: The comment of the field.
                @return: A NOT NULL Field instance with the given name, field_type, and comment.
                """
                return cls(name, field_type, False, comment)

            @classmethod
            def nullable_field(cls, name, field_type, comment=None):
                """
                @param name: The name of the field.
                @param field_type: The type of the field.
                @param comment: The comment of the field.
                @return: A nullable Field instance with the given name, field_type, and comment.
                """
                return cls(name, field_type, True, comment)

            def name(self):
                return self._name

            def field_type(self):
                return self._field_type

            def nullable(self):
                return self._nullable

            def comment(self):
                return self._comment

            def __eq__(self, other):
                """
                Compares two Field objects for equality.

                @param other: The other Field object to compare with.
                @return: True if both Field objects have the same attributes, False otherwise.
                """
                if not isinstance(other, Types.StructType.Field):
                    return False
                return (
                    self._name == other._name
                    and self._field_type == other._field_type
                    and self._nullable == other._nullable
                    and self._comment == other._comment
                )

            def __hash__(self):
                return hash((self._name, self._field_type, self._nullable))

            def simple_string(self) -> str:
                nullable_str = "NULL" if self._nullable else "NOT NULL"
                comment_str = f" COMMENT '{self._comment}'" if self._comment else ""
                return f"{self._name}: {self._field_type.simple_string()} {nullable_str}{comment_str}"

    class ListType(ComplexType):
        def __init__(self, element_type: Type, element_nullable: bool):
            """
            Create a new ListType with the given element type and the type is nullable.

            @param element_type: The element type of the list.
            @param element_nullable: Whether the element of the list is nullable.
            """
            if element_type is None:
                raise ValueError("element_type cannot be null")
            self._element_type = element_type
            self._element_nullable = element_nullable

        @classmethod
        def nullable(cls, element_type: Type):
            """
            Create a new ListType with the given element type and the type is nullable.

            @param element_type: The element type of the list.
            @return: A new ListType instance.
            """
            return cls.of(element_type, True)

        @classmethod
        def not_null(cls, element_type: Type):
            """
            Create a new ListType with the given element type.

            @param element_type: The element type of the list.
            @return: A new ListType instance.
            """
            return cls.of(element_type, False)

        @classmethod
        def of(cls, element_type: Type, element_nullable: bool):
            """
            Create a new ListType with the given element type and whether the element is nullable.

            @param element_type: The element type of the list.
            @param element_nullable: Whether the element of the list is nullable.
            @return: A new ListType instance.
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
            """
            Compares two ListType objects for equality.

            @param other: The other ListType object to compare with.
            @return: True if both ListType objects have the same element type and nullability, False otherwise.
            """
            if not isinstance(other, Types.ListType):
                return False
            return (
                self._element_nullable == other._element_nullable
                and self._element_type == other._element_type
            )

        def __hash__(self):
            return hash((self._element_type, self._element_nullable))

    class MapType(ComplexType):
        def __init__(self, key_type: Type, value_type: Type, value_nullable: bool):
            """
            Create a new MapType with the given key type, value type and the value is nullable.

            @param key_type: The key type of the map.
            @param value_type: The value type of the map.
            @param value_nullable: Whether the value of the map is nullable.
            """
            self._key_type = key_type
            self._value_type = value_type
            self._value_nullable = value_nullable

        @classmethod
        def value_nullable(cls, key_type: Type, value_type: Type):
            """
            Create a new MapType with the given key type, value type, and the value is nullable.

            @param key_type: The key type of the map.
            @param value_type: The value type of the map.
            @return: A new MapType instance.
            """
            return cls.of(key_type, value_type, True)

        @classmethod
        def value_not_null(cls, key_type: Type, value_type: Type):
            """
            Create a new MapType with the given key type, value type, and the value is not nullable.

            @param key_type: The key type of the map.
            @param value_type: The value type of the map.
            @return: A new MapType instance.
            """
            return cls.of(key_type, value_type, False)

        @classmethod
        def of(cls, key_type: Type, value_type: Type, value_nullable: bool):
            """
            Create a new MapType with the given key type, value type, and whether the value is nullable.

            @param key_type: The key type of the map.
            @param value_type: The value type of the map.
            @param value_nullable: Whether the value of the map is nullable.
            @return: A new MapType instance.
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

            @param other: The other MapType object to compare with.
            @return: True if both MapType objects have the same key type, value type, and nullability, False otherwise.
            """
            if not isinstance(other, Types.MapType):
                return False
            return (
                self._value_nullable == other._value_nullable
                and self._key_type == other._key_type
                and self._value_type == other._value_type
            )

        def __hash__(self):
            return hash((self._key_type, self._value_type, self._value_nullable))

    class UnionType(ComplexType):
        def __init__(self, types: list):
            """
            Create a new UnionType with the given types.

            @param types: The types of the union.
            """
            self._types = types

        @classmethod
        def of(cls, *types: Type):
            """
            Create a new UnionType with the given types.

            @param types: The types of the union.
            @return: A new UnionType instance.
            """
            return cls(types)

        def types(self) -> list:
            return self._types

        def name(self) -> Name:
            return Name.UNION

        def simple_string(self) -> str:
            return f"union<{', '.join(t.simple_string() for t in self._types)}>"

        def __eq__(self, other):
            """
            Compares two UnionType objects for equality.

            @param other: The other UnionType object to compare with.
            @return: True if both UnionType objects have the same types, False otherwise.
            """
            if not isinstance(other, Types.UnionType):
                return False
            return self._types == other._types

        def __hash__(self):
            return hash(tuple(self._types))

    class UnparsedType(Type):
        def __init__(self, unparsed_type: str):
            """
            Initializes an unparsed_type instance.

            @param unparsed_type: The unparsed type as a string.
            """
            self._unparsed_type = unparsed_type

        @classmethod
        def of(cls, unparsed_type: str):
            """
            Creates a new unparsed_type with the given unparsed type.

            @param unparsed_type: The unparsed type.
            @return: A new unparsed_type instance.
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

            @param other: The other unparsed_type object to compare with.
            @return: True if both unparsed_type objects have the same unparsed type string, False otherwise.
            """
            if not isinstance(other, Types.unparsed_type):
                return False
            return self._unparsed_type == other._unparsed_type

        def __hash__(self):
            return hash(self._unparsed_type)

        def __str__(self):
            return self._unparsed_type

    class ExternalType(Type):
        def __init__(self, catalog_string: str):
            """
            Initializes an ExternalType instance.

            @param catalog_string: The string representation of this type in the catalog.
            """
            self._catalog_string = catalog_string

        @classmethod
        def of(cls, catalog_string: str):
            """
            Creates a new ExternalType with the given catalog string.

            @param catalog_string: The string representation of this type in the catalog.
            @return: A new ExternalType instance.
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

            @param other: The other ExternalType object to compare with.
            @return: True if both ExternalType objects have the same catalog string, False otherwise.
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

        @param data_type: The data type to check.
        @return: True if the given data type is allowed to be an auto-increment column, False otherwise.
        """
        return isinstance(data_type, (Types.IntegerType, Types.LongType))
