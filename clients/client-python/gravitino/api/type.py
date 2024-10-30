from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional

class Name(Enum):
    """
    The root type name of this type, representing all data types supported.
    """

    BOOLEAN = "BOOLEAN"
    """ The boolean type. """

    BYTE = "BYTE"
    """ The byte type. """

    SHORT = "SHORT"
    """ The short type. """

    INTEGER = "INTEGER"
    """ The integer type. """

    LONG = "LONG"
    """ The long type. """

    FLOAT = "FLOAT"
    """ The float type. """

    DOUBLE = "DOUBLE"
    """ The double type. """

    DECIMAL = "DECIMAL"
    """ The decimal type. """

    DATE = "DATE"
    """ The date type. """

    TIME = "TIME"
    """ The time type. """

    TIMESTAMP = "TIMESTAMP"
    """ The timestamp type. """

    INTERVAL_YEAR = "INTERVAL_YEAR"
    """ The interval year type. """

    INTERVAL_DAY = "INTERVAL_DAY"
    """ The interval day type. """

    STRING = "STRING"
    """ The string type. """

    VARCHAR = "VARCHAR"
    """ The varchar type. """

    FIXEDCHAR = "FIXEDCHAR"
    """ The char type with fixed length. """

    UUID = "UUID"
    """ The UUID type. """

    FIXED = "FIXED"
    """ The binary type with fixed length. """

    BINARY = "BINARY"
    """ The binary type with variable length. The length is specified in the type itself. """

    STRUCT = "STRUCT"
    """
    The struct type.
    A struct type is a complex type that contains a set of named fields, each with a type, 
    and optionally a comment.
    """

    LIST = "LIST"
    """
    The list type.
    A list type is a complex type that contains a set of elements, each with the same type.
    """

    MAP = "MAP"
    """
    The map type.
    A map type is a complex type that contains a set of key-value pairs, each with a key type 
    and a value type.
    """

    UNION = "UNION"
    """
    The union type.
    A union type is a complex type that contains a set of types.
    """

    NULL = "NULL"
    """ The null type. A null type represents a value that is null. """

    UNPARSED = "UNPARSED"
    """ The unparsed type. An unparsed type represents an unresolvable type. """

    EXTERNAL = "EXTERNAL"
    """ The external type. An external type represents a type that is not supported. """

# Define the Type interface (abstract base class)
class Type(ABC):
    @abstractmethod
    def name(self) -> Name:
        """ Returns the generic name of the type. """
        pass

    @abstractmethod
    def simpleString(self) -> str:
        """ Returns a readable string representation of the type. """
        pass

# Define base classes
class PrimitiveType(Type, ABC):
    """ Base class for all primitive types. """
    pass

class NumericType(PrimitiveType, ABC):
    """ Base class for all numeric types. """
    pass

class DateTimeType(PrimitiveType, ABC):
    """ Base class for all date/time types. """
    pass

class IntervalType(PrimitiveType, ABC):
    """ Base class for all interval types. """
    pass

class ComplexType(Type, ABC):
    """ Base class for all complex types, including struct, list, map, and union. """
    pass

# Define IntegralType class
class IntegralType(NumericType, ABC):
    def __init__(self, signed: bool):
        self._signed = signed

    def signed(self) -> bool:
        """ Returns True if the integer type is signed, False otherwise. """
        return self._signed

# Define FractionType class
class FractionType(NumericType, ABC):
    """ Base class for all fractional types. """
    pass

""" The helper class for Type. """
class Types:

    """ The data type representing `NULL` values. """
    class NullType(Type):
        _instance = None

        @classmethod
        def get(cls):
            """
            @return The singleton instance of NullType.
            """
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def name(self) -> Name:
            """
            @return The name of the Null type.
            """
            return Name.NULL

        def simpleString(self) -> str:
            """
            @return A readable string representation of the Null type.
            """
            return "null"

    """ The boolean type in Gravitino. """
    class BooleanType(PrimitiveType):
        _instance = None

        @classmethod
        def get(cls):
            """
            @return The singleton instance of BooleanType.
            """
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def name(self) -> Name:
            """
            @return The name of the Boolean type.
            """
            return Name.BOOLEAN

        def simpleString(self) -> str:
            """
            @return A readable string representation of the Boolean type.
            """
            return "boolean"

    """ The byte type in Gravitino. """
    class ByteType(IntegralType):
        _instance = None
        _unsigned_instance = None

        @classmethod
        def get(cls):
            """
            @return The singleton instance of ByteType.
            """
            if cls._instance is None:
                cls._instance = cls(True)
            return cls._instance

        @classmethod
        def unsigned(cls):
            """
            @return The singleton instance of unsigned ByteType.
            """
            if cls._unsigned_instance is None:
                cls._unsigned_instance = cls(False)
            return cls._unsigned_instance

        def __init__(self, signed: bool):
            """
            @param signed: True if the byte type is signed, False otherwise.
            """
            super().__init__(signed)

        def name(self) -> Name:
            """
            @return The name of the Byte type.
            """
            return Name.BYTE

        def simpleString(self) -> str:
            """
            @return A readable string representa
            """
            return "byte" if self.signed() else "byte unsigned"

    """ The short type in Gravitino. """
    class ShortType(IntegralType):
        _instance = None
        _unsigned_instance = None

        @classmethod
        def get(cls):
            """
            @return The singleton instance of ShortType.
            """
            if cls._instance is None:
                cls._instance = cls(True)
            return cls._instance

        @classmethod
        def unsigned(cls):
            """
            @return The singleton instance of unsigned ShortType.
            """
            if cls._unsigned_instance is None:
                cls._unsigned_instance = cls(False)
            return cls._unsigned_instance

        def __init__(self, signed: bool):
            """
            @param signed: True if the short type is signed, False otherwise.
            """
            super().__init__(signed)

        def name(self) -> Name:
            """
            @return The name of the Short type.
            """
            return Name.SHORT

        def simpleString(self) -> str:
            """
            @return A readable string representation of the Short type.
            """
            return "short" if self.signed() else "short unsigned"

    """ The integer type in Gravitino. """
    class IntegerType(IntegralType):
        _instance = None
        _unsigned_instance = None

        @classmethod
        def get(cls):
            """
            @return The singleton instance of IntegerType.
            """
            if cls._instance is None:
                cls._instance = cls(True)
            return cls._instance

        @classmethod
        def unsigned(cls):
            """
            @return The singleton instance of unsigned IntegerType.
            """
            if cls._unsigned_instance is None:
                cls._unsigned_instance = cls(False)
            return cls._unsigned_instance

        def __init__(self, signed: bool):
            """
            @param signed: True if the integer type is signed, False otherwise.
            """
            super().__init__(signed)

        def name(self) -> Name:
            """
            @return The name of the Integer type.
            """
            return Name.INTEGER

        def simpleString(self) -> str:
            """
            @return A readable string representation of the Integer type.
            """
            return "integer" if self.signed() else "integer unsigned"

    """ The long type in Gravitino. """
    class LongType(IntegralType):
        _instance = None
        _unsigned_instance = None

        @classmethod
        def get(cls):
            """
            @return The singleton instance of LongType.
            """
            if cls._instance is None:
                cls._instance = cls(True)
            return cls._instance

        @classmethod
        def unsigned(cls):
            """
            @return The singleton instance of unsigned LongType.
            """
            if cls._unsigned_instance is None:
                cls._unsigned_instance = cls(False)
            return cls._unsigned_instance

        def __init__(self, signed: bool):
            """
            @param signed: True if the long type is signed, False otherwise.
            """
            super().__init__(signed)

        def name(self) -> Name:
            """
            @return The name of the Long type.
            """
            return Name.LONG

        def simpleString(self) -> str:
            """
            @return A readable string representation of the Long type.
            """
            return "long" if self.signed() else "long unsigned"

    """ The float type in Gravitino. """
    class FloatType(FractionType):
        _instance = None

        @classmethod
        def get(cls):
            """
            @return The singleton instance of FloatType.
            """
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def name(self) -> Name:
            """
            @return The name of the Float type.
            """
            return Name.FLOAT

        def simpleString(self) -> str:
            """
            @return A readable string representation of the Float type.
            """
            return "float"

    """ The double type in Gravitino. """
    class DoubleType(FractionType):
        _instance = None

        @classmethod
        def get(cls):
            """
            @return The singleton instance of DoubleType.
            """
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def name(self) -> Name:
            """
            @return The name of the Double type.
            """
            return Name.DOUBLE

        def simpleString(self) -> str:
            """
            @return A readable string representation of the Double type.
            """
            return "double"

    """ The decimal type in Gravitino. """
    class DecimalType(FractionType):
        @classmethod
        def of(cls, precision: int, scale: int):
            """
            @param precision: The precision of the decimal type.
            @param scale: The scale of the decimal type.
            @return A DecimalType with the given precision and scale.
            """
            return cls(precision, scale)

        def __init__(self, precision: int, scale: int):
            """
            @param precision: The precision of the decimal type.
            @param scale: The scale of the decimal type.
            """
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
            if not (1 <= precision <= 38):
                raise ValueError(f"Decimal precision must be in range [1, 38]: {precision}")
            if not (0 <= scale <= precision):
                raise ValueError(f"Decimal scale must be in range [0, precision ({precision})]: {scale}")

        def name(self) -> Name:
            """
            @return The name of the Decimal type.
            """
            return Name.DECIMAL

        def precision(self) -> int:
            """
            @return The precision of the decimal type.
            """
            return self._precision

        def scale(self) -> int:
            """
            @return The scale of the decimal type.
            """
            return self._scale

        def simpleString(self) -> str:
            """
            @return A readable string representation of the Decimal type.
            """
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
            """
            @return: A hash code for the DecimalType based on its precision and scale.
            """
            return hash((self._precision, self._scale))

    """ The date time type in Gravitino. """
    class DateType(DateTimeType):
        _instance = None

        @classmethod
        def get(cls):
            """
            @return The singleton instance of DateType.
            """
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def name(self) -> Name:
            """
            @return The name of the Date type.
            """
            return Name.DATE

        def simpleString(self) -> str:
            """
            @return A readable string representation of the Date type.
            """
            return "date"

    """ The time type in Gravitino. """
    class TimeType(DateTimeType):
        _instance = None

        @classmethod
        def get(cls):
            """
            @return The singleton instance of TimeType.
            """
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def name(self) -> Name:
            """
            @return The name of the Time type.
            """
            return Name.TIME

        def simpleString(self) -> str:
            """
            @return A readable string representation of the Time type.
            """
            return "time"

    """ The timestamp type in Gravitino. """
    class TimestampType(DateTimeType):
        _instance_with_tz = None
        _instance_without_tz = None

        @classmethod
        def withTimeZone(cls):
            """
            @return A TimestampType with time zone.
            """
            if cls._instance_with_tz is None:
                cls._instance_with_tz = cls(True)
            return cls._instance_with_tz

        @classmethod
        def withoutTimeZone(cls):
            """
            @return A TimestampType without time zone.
            """
            if cls._instance_without_tz is None:
                cls._instance_without_tz = cls(False)
            return cls._instance_without_tz

        def __init__(self, with_time_zone: bool):
            """
            @param with_time_zone: True if the timestamp type has a time zone, False otherwise.
            """
            self._with_time_zone = with_time_zone

        def hasTimeZone(self) -> bool:
            """
            @return True if the timestamp type has a time zone, False otherwise.
            """
            return self._with_time_zone

        def name(self) -> Name:
            """
            @return The name of the Timestamp type.
            """
            return Name.TIMESTAMP

        def simpleString(self) -> str:
            """
            @return A readable string representation of the Timestamp type.
            """
            return "timestamp_tz" if self._with_time_zone else "timestamp"

    """ The interval year type in Gravitino. """
    class IntervalYearType(IntervalType):
        _instance = None

        @classmethod
        def get(cls):
            """
            @return The singleton instance of IntervalYearType.
            """
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def name(self) -> Name:
            """
            @return The name of the IntervalYear type.
            """
            return Name.INTERVAL_YEAR

        def simpleString(self) -> str:
            """
            @return A readable string representation of the IntervalYear type.
            """
            return "interval_year"
        
    """ The interval day type in Gravitino. """
    class IntervalDayType(IntervalType):
        _instance = None

        @classmethod
        def get(cls):
            """
            @return: The singleton instance of IntervalDayType.
            """
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def name(self) -> Name:
            """
            @return: The name of the interval day type.
            """
            return Name.INTERVAL_DAY

        def simpleString(self) -> str:
            """
            @return: A readable string representation of the interval day type.
            """
            return "interval_day"

    """ The string type in Gravitino, equivalent to varchar(MAX), where the MAX is determined by the underlying catalog. """
    class StringType(PrimitiveType):
        _instance = None

        @classmethod
        def get(cls):
            """
            @return: The singleton instance of StringType.
            """
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def name(self) -> Name:
            """
            @return: The name of the string type.
            """
            return Name.STRING

        def simpleString(self) -> str:
            """
            @return: A readable string representation of the string type.
            """
            return "string"

    """ The UUID type in Gravitino. """
    class UUIDType(PrimitiveType):
        _instance = None

        @classmethod
        def get(cls):
            """
            @return: The singleton instance of UUIDType.
            """
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def name(self) -> Name:
            """
            @return: The name of the UUID type.
            """
            return Name.UUID

        def simpleString(self) -> str:
            """
            @return: A readable string representation of the UUID type.
            """
            return "uuid"

    """ Fixed-length byte array type. Use BinaryType for variable-length byte arrays. """
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
            """
            @return: The name of the fixed type.
            """
            return Name.FIXED

        def length(self) -> int:
            """
            @return: The length of the fixed type.
            """
            return self._length

        def simpleString(self) -> str:
            """
            @return: A readable string representation of the fixed type.
            """
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
            """
            @return: A hash code for the FixedType based on its length.
            """
            return hash(self._length)

    """ The varchar type in Gravitino. """
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
            """
            @return: The name of the varchar type.
            """
            return Name.VARCHAR

        def length(self) -> int:
            """
            @return: The length of the varchar type.
            """
            return self._length

        def simpleString(self) -> str:
            """
            @return: A readable string representation of the varchar type.
            """
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
            """
            @return: A hash code for the VarCharType based on its length.
            """
            return hash(self._length)

    """ The fixed char type in Gravitino. """
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
            """
            @return: The name of the fixed char type.
            """
            return Name.FIXEDCHAR

        def length(self) -> int:
            """
            @return: The length of the fixed char type.
            """
            return self._length

        def simpleString(self) -> str:
            """
            @return: A readable string representation of the fixed char type.
            """
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
            """
            @return: A hash code for the FixedCharType based on its length.
            """
            return hash(self._length)

    """ The binary type in Gravitino. """
    class BinaryType(PrimitiveType):
        _instance = None

        @classmethod
        def get(cls):
            """
            @return: The singleton instance of BinaryType.
            """
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def name(self) -> Name:
            """
            @return: The name of the binary type.
            """
            return Name.BINARY

        def simpleString(self) -> str:
            """
            @return: A readable string representation of the binary type.
            """
            return "binary"

    """ The struct type in Gravitino. Note, this type is not supported in the current version of Gravitino."""
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
            """
            @return: The fields of the struct type.
            """
            return self._fields

        def name(self) -> Name:
            """
            @return: The name of the struct type.
            """
            return Name.STRUCT

        def simpleString(self) -> str:
            """
            @return: A readable string representation of the struct type.
            """
            return f"struct<{', '.join(field.simpleString() for field in self._fields)}>"

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
            """
            @return: A hash code for the StructType based on its fields.
            """
            return hash(tuple(self._fields))

        """ A field of a struct type. """
        class Field:
            def __init__(self, name, type, nullable, comment=None):
                """
                Initializes the Field with the given name, type, nullable flag, and comment.

                @param name: The name of the field.
                @param type: The type of the field.
                @param nullable: Whether the field is nullable.
                @param comment: The comment of the field (optional).
                """
                if name is None:
                    raise ValueError("name cannot be null")
                if type is None:
                    raise ValueError("type cannot be null")
                self._name = name
                self._type = type
                self._nullable = nullable
                self._comment = comment

            @classmethod
            def notNullField(cls, name, type, comment=None):
                """
                @param name: The name of the field.
                @param type: The type of the field.
                @param comment: The comment of the field.
                @return: A NOT NULL Field instance with the given name, type, and comment.
                """
                return cls(name, type, False, comment)

            @classmethod
            def nullableField(cls, name, type, comment=None):
                """
                @param name: The name of the field.
                @param type: The type of the field.
                @param comment: The comment of the field.
                @return: A nullable Field instance with the given name, type, and comment.
                """
                return cls(name, type, True, comment)

            def name(self):
                """
                @return: The name of the field.
                """
                return self._name

            def type(self):
                """
                @return: The type of the field.
                """
                return self._type

            def nullable(self):
                """
                @return: Whether the field is nullable.
                """
                return self._nullable

            def comment(self):
                """
                @return: The comment of the field, or None if not set.
                """
                return self._comment

            def __eq__(self, other):
                """
                Compares two Field objects for equality.

                @param other: The other Field object to compare with.
                @return: True if both Field objects have the same attributes, False otherwise.
                """
                if not isinstance(other, Types.StructType.Field):
                    return False
                return (self._name == other._name and self._type == other._type and
                        self._nullable == other._nullable and self._comment == other._comment)

            def __hash__(self):
                """
                @return: A hash code for the Field based on its attributes.
                """
                return hash((self._name, self._type, self._nullable))

            def simpleString(self) -> str:
                """
                @return: The simple string representation of the field.
                """
                nullable_str = "NULL" if self._nullable else "NOT NULL"
                comment_str = f" COMMENT '{self._comment}'" if self._comment else ""
                return f"{self._name}: {self._type.simpleString()} {nullable_str}{comment_str}"

    """ A list type. Note, this type is not supported in the current version of Gravitino. """
    class ListType(ComplexType):
        def __init__(self, elementType: Type, elementNullable: bool):
            """
            Create a new ListType with the given element type and the type is nullable.

            @param elementType: The element type of the list.
            @param elementNullable: Whether the element of the list is nullable.
            """
            if elementType is None:
                raise ValueError("elementType cannot be null")
            self._elementType = elementType
            self._elementNullable = elementNullable

        @classmethod
        def nullable(cls, elementType: Type):
            """
            Create a new ListType with the given element type and the type is nullable.

            @param elementType: The element type of the list.
            @return: A new ListType instance.
            """
            return cls.of(elementType, True)

        @classmethod
        def notNull(cls, elementType: Type):
            """
            Create a new ListType with the given element type.

            @param elementType: The element type of the list.
            @return: A new ListType instance.
            """
            return cls.of(elementType, False)

        @classmethod
        def of(cls, elementType: Type, elementNullable: bool):
            """
            Create a new ListType with the given element type and whether the element is nullable.

            @param elementType: The element type of the list.
            @param elementNullable: Whether the element of the list is nullable.
            @return: A new ListType instance.
            """
            return cls(elementType, elementNullable)

        def elementType(self) -> Type:
            """
            @return: The element type of the list.
            """
            return self._elementType

        def elementNullable(self) -> bool:
            """
            @return: Whether the element of the list is nullable.
            """
            return self._elementNullable

        def name(self) -> Name:
            """
            @return: The name of the list type.
            """
            return Name.LIST

        def simpleString(self) -> str:
            """
            @return: A readable string representation of the list type.
            """
            return f"list<{self._elementType.simpleString()}>" if self._elementNullable else f"list<{self._elementType.simpleString()}, NOT NULL>"

        def __eq__(self, other):
            """
            Compares two ListType objects for equality.

            @param other: The other ListType object to compare with.
            @return: True if both ListType objects have the same element type and nullability, False otherwise.
            """
            if not isinstance(other, Types.ListType):
                return False
            return self._elementNullable == other._elementNullable and self._elementType == other._elementType

        def __hash__(self):
            """
            @return: A hash code for the ListType based on its element type and nullability.
            """
            return hash((self._elementType, self._elementNullable))

    """ The map type in Gravitino. Note, this type is not supported in the current version of Gravitino. """
    class MapType(ComplexType):
        def __init__(self, keyType: Type, valueType: Type, valueNullable: bool):
            """
            Create a new MapType with the given key type, value type and the value is nullable.

            @param keyType: The key type of the map.
            @param valueType: The value type of the map.
            @param valueNullable: Whether the value of the map is nullable.
            """
            self._keyType = keyType
            self._valueType = valueType
            self._valueNullable = valueNullable

        @classmethod
        def valueNullable(cls, keyType: Type, valueType: Type):
            """
            Create a new MapType with the given key type, value type, and the value is nullable.

            @param keyType: The key type of the map.
            @param valueType: The value type of the map.
            @return: A new MapType instance.
            """
            return cls.of(keyType, valueType, True)

        @classmethod
        def valueNotNull(cls, keyType: Type, valueType: Type):
            """
            Create a new MapType with the given key type, value type, and the value is not nullable.

            @param keyType: The key type of the map.
            @param valueType: The value type of the map.
            @return: A new MapType instance.
            """
            return cls.of(keyType, valueType, False)

        @classmethod
        def of(cls, keyType: Type, valueType: Type, valueNullable: bool):
            """
            Create a new MapType with the given key type, value type, and whether the value is nullable.

            @param keyType: The key type of the map.
            @param valueType: The value type of the map.
            @param valueNullable: Whether the value of the map is nullable.
            @return: A new MapType instance.
            """
            return cls(keyType, valueType, valueNullable)

        def keyType(self) -> Type:
            """
            @return: The key type of the map.
            """
            return self._keyType

        def valueType(self) -> Type:
            """
            @return: The value type of the map.
            """
            return self._valueType

        def valueNullable(self) -> bool:
            """
            @return: Whether the value of the map is nullable.
            """
            return self._valueNullable

        def name(self) -> Name:
            """
            @return: The name of the map type.
            """
            return Name.MAP

        def simpleString(self) -> str:
            """
            @return: A readable string representation of the map type.
            """
            return f"map<{self._keyType.simpleString()}, {self._valueType.simpleString()}>"

        def __eq__(self, other):
            """
            Compares two MapType objects for equality.

            @param other: The other MapType object to compare with.
            @return: True if both MapType objects have the same key type, value type, and nullability, False otherwise.
            """
            if not isinstance(other, Types.MapType):
                return False
            return self._valueNullable == other._valueNullable and self._keyType == other._keyType and self._valueType == other._valueType

        def __hash__(self):
            """
            @return: A hash code for the MapType based on its key type, value type, and nullability.
            """
            return hash((self._keyType, self._valueType, self._valueNullable))

    """ The union type in Gravitino. Note, this type is not supported in the current version of Gravitino. """
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
            """
            @return: The types of the union.
            """
            return self._types

        def name(self) -> Name:
            """
            @return: The name of the union type.
            """
            return Name.UNION

        def simpleString(self) -> str:
            """
            @return: A readable string representation of the union type.
            """
            return f"union<{', '.join(t.simpleString() for t in self._types)}>"

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
            """
            @return: A hash code for the UnionType based on its types.
            """
            return hash(tuple(self._types))

    """  Represents a type that is not parsed yet. The parsed type is represented by other types of Types. """
    class UnparsedType(Type):
        def __init__(self, unparsedType: str):
            """
            Initializes an UnparsedType instance.

            @param unparsedType: The unparsed type as a string.
            """
            self._unparsedType = unparsedType

        @classmethod
        def of(cls, unparsedType: str):
            """
            Creates a new UnparsedType with the given unparsed type.

            @param unparsedType: The unparsed type.
            @return: A new UnparsedType instance.
            """
            return cls(unparsedType)

        def unparsedType(self) -> str:
            """
            @return: The unparsed type as a string.
            """
            return self._unparsedType

        def name(self) -> Name:
            """
            @return: The name of the unparsed type.
            """
            return Name.UNPARSED

        def simpleString(self) -> str:
            """
            @return: A readable string representation of the unparsed type.
            """
            return f"unparsed({self._unparsedType})"

        def __eq__(self, other):
            """
            Compares two UnparsedType objects for equality.

            @param other: The other UnparsedType object to compare with.
            @return: True if both UnparsedType objects have the same unparsed type string, False otherwise.
            """
            if not isinstance(other, Types.UnparsedType):
                return False
            return self._unparsedType == other._unparsedType

        def __hash__(self):
            """
            @return: A hash code for the UnparsedType based on its unparsed type string.
            """
            return hash(self._unparsedType)

        def __str__(self):
            """
            @return: The unparsed type string representation.
            """
            return self._unparsedType

    """ Represents a type that is defined in an external catalog. """
    class ExternalType(Type):
        def __init__(self, catalogString: str):
            """
            Initializes an ExternalType instance.

            @param catalogString: The string representation of this type in the catalog.
            """
            self._catalogString = catalogString

        @classmethod
        def of(cls, catalogString: str):
            """
            Creates a new ExternalType with the given catalog string.

            @param catalogString: The string representation of this type in the catalog.
            @return: A new ExternalType instance.
            """
            return cls(catalogString)

        def catalogString(self) -> str:
            """
            @return: The string representation of this type in external catalog.
            """
            return self._catalogString

        def name(self) -> Name:
            """
            @return: The name of the external type.
            """
            return Name.EXTERNAL

        def simpleString(self) -> str:
            """
            @return: A readable string representation of the external type.
            """
            return f"external({self._catalogString})"

        def __eq__(self, other):
            """
            Compares two ExternalType objects for equality.

            @param other: The other ExternalType object to compare with.
            @return: True if both ExternalType objects have the same catalog string, False otherwise.
            """
            if not isinstance(other, Types.ExternalType):
                return False
            return self._catalogString == other._catalogString

        def __hash__(self):
            """
            @return: A hash code for the ExternalType based on its catalog string.
            """
            return hash(self._catalogString)

        def __str__(self):
            """
            @return: The string representation of the external type.
            """
            return self.simpleString()

    @staticmethod
    def allowAutoIncrement(dataType: Type) -> bool:
        """
        Checks if the given data type is allowed to be an auto-increment column.

        @param dataType: The data type to check.
        @return: True if the given data type is allowed to be an auto-increment column, False otherwise.
        """
        return isinstance(dataType, (Types.IntegerType, Types.LongType))

