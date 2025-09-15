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
from enum import Enum


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
        """Returns the generic name of the type."""
        pass

    @abstractmethod
    def simple_string(self) -> str:
        """Returns a readable string representation of the type."""
        pass


# Define base classes
class PrimitiveType(Type, ABC):
    """Base class for all primitive types."""

    pass


class NumericType(PrimitiveType, ABC):
    """Base class for all numeric types."""

    pass


class DateTimeType(PrimitiveType, ABC):
    """Base class for all date/time types."""

    # Indicates that precision for the date/time type was not explicitly set by the user.
    # The value should be converted to the catalog's default precision.
    DATE_TIME_PRECISION_NOT_SET = -1

    # Represents the minimum precision range for timestamp, time and other date/time types.
    # The minimum precision is 0, which means second-level precision.
    MIN_ALLOWED_PRECISION = 0

    # Represents the maximum precision allowed for timestamp, time and other date/time types.
    # The maximum precision is 12, which means picosecond-level precision.
    MAX_ALLOWED_PRECISION = 12


class IntervalType(PrimitiveType, ABC):
    """Base class for all interval types."""

    pass


class ComplexType(Type, ABC):
    """Base class for all complex types, including struct, list, map, and union."""

    pass


class IntegralType(NumericType, ABC):
    """Base class for all integral types."""

    _signed: bool

    def __init__(self, signed: bool):
        self._signed = signed

    def signed(self) -> bool:
        """Returns True if the integer type is signed, False otherwise."""
        return self._signed


class FractionType(NumericType, ABC):
    """Base class for all fractional types."""

    pass
