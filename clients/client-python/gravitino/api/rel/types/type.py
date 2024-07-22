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

from abc import ABC, abstractmethod
from enum import Enum

class Name(Enum):
    BOOLEAN = "BOOLEAN"
    BYTE = "BYTE"
    SHORT = "SHORT"
    INTEGER = "INTEGER"
    LONG = "LONG"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    DECIMAL = "DECIMAL"
    DATE = "DATE"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    INTERVAL_YEAR = "INTERVAL_YEAR"
    INTERVAL_DAY = "INTERVAL_DAY"
    STRING = "STRING"
    VARCHAR = "VARCHAR"
    FIXEDCHAR = "FIXEDCHAR"
    UUID = "UUID"
    FIXED = "FIXED"
    BINARY = "BINARY"
    STRUCT = "STRUCT"
    LIST = "LIST"
    MAP = "MAP"
    UNION = "UNION"
    NULL = "NULL"
    UNPARSED = "UNPARSED"
    EXTERNAL = "EXTERNAL"

class Type(ABC):
    @abstractmethod
    def name(self):
        pass

    @abstractmethod
    def simple_string(self):
        pass

class PrimitiveType(Type, ABC):
    pass

class NumericType(PrimitiveType, ABC):
    pass

class DateTimeType(PrimitiveType, ABC):
    pass

class IntervalType(PrimitiveType, ABC):
    pass

class ComplexType(Type, ABC):
    pass

class IntegralType(NumericType):
    def __init__(self, signed: bool):
        self._signed = signed

    def signed(self) -> bool:
        return self._signed

class FractionType(NumericType, ABC):
    pass
