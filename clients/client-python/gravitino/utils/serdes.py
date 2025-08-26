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

import re
from collections.abc import Mapping
from types import MappingProxyType
from typing import Final, Pattern, Set

from gravitino.api.types.types import Name, Types


class SerdesUtilsBase:
    EXPRESSION_TYPE: Final[str] = "type"
    DATA_TYPE: Final[str] = "dataType"
    LITERAL_VALUE: Final[str] = "value"
    FIELD_NAME: Final[str] = "fieldName"
    FUNCTION_NAME: Final[str] = "funcName"
    FUNCTION_ARGS: Final[str] = "funcArgs"
    UNPARSED_EXPRESSION: Final[str] = "unparsedExpression"
    TYPE: Final[str] = "type"
    STRUCT: Final[str] = "struct"
    FIELDS: Final[str] = "fields"
    STRUCT_FIELD_NAME: Final[str] = "name"
    STRUCT_FIELD_NULLABLE: Final[str] = "nullable"
    STRUCT_FIELD_COMMENT: Final[str] = "comment"
    LIST: Final[str] = "list"
    LIST_ELEMENT_TYPE: Final[str] = "elementType"
    LIST_ELEMENT_NULLABLE: Final[str] = "containsNull"
    MAP: Final[str] = "map"
    MAP_KEY_TYPE: Final[str] = "keyType"
    MAP_VALUE_TYPE: Final[str] = "valueType"
    MAP_VALUE_NULLABLE: Final[str] = "valueContainsNull"
    UNION: Final[str] = "union"
    UNION_TYPES: Final[str] = "types"
    UNPARSED: Final[str] = "unparsed"
    UNPARSED_TYPE: Final[str] = "unparsedType"
    EXTERNAL: Final[str] = "external"
    CATALOG_STRING: Final[str] = "catalogString"

    PARTITION_TYPE: Final[str] = "type"
    PARTITION_NAME: Final[str] = "name"
    PARTITION_PROPERTIES: Final[str] = "properties"
    FIELD_NAMES: Final[str] = "fieldNames"
    IDENTITY_PARTITION_VALUES: Final[str] = "values"
    LIST_PARTITION_LISTS: Final[str] = "lists"
    RANGE_PARTITION_UPPER: Final[str] = "upper"
    RANGE_PARTITION_LOWER: Final[str] = "lower"

    NON_PRIMITIVE_TYPES: Final[Set[Name]] = {
        Name.STRUCT,
        Name.LIST,
        Name.MAP,
        Name.UNION,
        Name.UNPARSED,
        Name.EXTERNAL,
    }
    PRIMITIVE_AND_NULL_TYPES: Final[Set[Name]] = set(list(Name)) - NON_PRIMITIVE_TYPES

    DECIMAL_PATTERN: Final[Pattern[str]] = re.compile(
        r"decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)"
    )
    FIXED_PATTERN: Final[Pattern[str]] = re.compile(r"fixed\(\s*(\d+)\s*\)")
    FIXEDCHAR_PATTERN: Final[Pattern[str]] = re.compile(r"char\(\s*(\d+)\s*\)")
    VARCHAR_PATTERN: Final[Pattern[str]] = re.compile(r"varchar\(\s*(\d+)\s*\)")
    TYPES: Final[Mapping] = MappingProxyType(
        {
            type_instance.simple_string(): type_instance
            for type_instance in (
                Types.NullType.get(),
                Types.BooleanType.get(),
                Types.ByteType.get(),
                Types.ByteType.unsigned(),
                Types.IntegerType.get(),
                Types.IntegerType.unsigned(),
                Types.ShortType.get(),
                Types.ShortType.unsigned(),
                Types.LongType.get(),
                Types.LongType.unsigned(),
                Types.FloatType.get(),
                Types.DoubleType.get(),
                Types.DateType.get(),
                Types.TimeType.get(),
                Types.TimestampType.with_time_zone(),
                Types.TimestampType.without_time_zone(),
                Types.IntervalYearType.get(),
                Types.IntervalDayType.get(),
                Types.StringType.get(),
                Types.UUIDType.get(),
            )
        }
    )
