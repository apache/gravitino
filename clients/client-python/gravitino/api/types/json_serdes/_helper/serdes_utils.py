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
from types import MappingProxyType
from typing import Any, ClassVar, Dict, Mapping, Pattern, Set, Union, overload

from gravitino.api.types.type import Name, Type
from gravitino.api.types.types import Types


class SerdesUtils:
    EXPRESSION_TYPE: ClassVar[str] = "type"
    DATA_TYPE: ClassVar[str] = "dataType"
    LITERAL_VALUE: ClassVar[str] = "value"
    FIELD_NAME: ClassVar[str] = "fieldName"
    FUNCTION_NAME: ClassVar[str] = "funcName"
    FUNCTION_ARGS: ClassVar[str] = "funcArgs"
    UNPARSED_EXPRESSION: ClassVar[str] = "unparsedExpression"
    TYPE: ClassVar[str] = "type"
    STRUCT: ClassVar[str] = "struct"
    FIELDS: ClassVar[str] = "fields"
    STRUCT_FIELD_NAME: ClassVar[str] = "name"
    STRUCT_FIELD_NULLABLE: ClassVar[str] = "nullable"
    STRUCT_FIELD_COMMENT: ClassVar[str] = "comment"
    LIST: ClassVar[str] = "list"
    LIST_ELEMENT_TYPE: ClassVar[str] = "elementType"
    LIST_ELEMENT_NULLABLE: ClassVar[str] = "containsNull"
    MAP: ClassVar[str] = "map"
    MAP_KEY_TYPE: ClassVar[str] = "keyType"
    MAP_VALUE_TYPE: ClassVar[str] = "valueType"
    MAP_VALUE_NULLABLE: ClassVar[str] = "valueContainsNull"
    UNION: ClassVar[str] = "union"
    UNION_TYPES: ClassVar[str] = "types"
    UNPARSED: ClassVar[str] = "unparsed"
    UNPARSED_TYPE: ClassVar[str] = "unparsedType"
    EXTERNAL: ClassVar[str] = "external"
    CATALOG_STRING: ClassVar[str] = "catalogString"

    NON_PRIMITIVE_TYPES: ClassVar[Set[Name]] = {
        Name.STRUCT,
        Name.LIST,
        Name.MAP,
        Name.UNION,
        Name.UNPARSED,
        Name.EXTERNAL,
    }
    PRIMITIVE_AND_NULL_TYPES: ClassVar[Set[Name]] = (
        set(list(Name)) - NON_PRIMITIVE_TYPES
    )

    DECIMAL_PATTERN: ClassVar[Pattern[str]] = re.compile(
        r"decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)"
    )
    FIXED_PATTERN: ClassVar[Pattern[str]] = re.compile(r"fixed\(\s*(\d+)\s*\)")
    FIXEDCHAR_PATTERN: ClassVar[Pattern[str]] = re.compile(r"char\(\s*(\d+)\s*\)")
    VARCHAR_PATTERN: ClassVar[Pattern[str]] = re.compile(r"varchar\(\s*(\d+)\s*\)")
    TYPES: ClassVar[Mapping] = MappingProxyType(
        {
            type_instance.simple_string(): type_instance
            for type_instance in {
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
            }
        }
    )

    @classmethod
    def write_data_type(cls, data_type: Type) -> Union[str, Dict[str, Any]]:
        """Write Gravitino Type to JSON data. Used for Gravitino Type JSON Serialization.

        Args:
            data_type (Any): The Gravitino Type.

        Returns:
            Union[str, Dict[str, Any]]: The serialized data.
        """

        type_name = data_type.name()
        if type_name in cls.PRIMITIVE_AND_NULL_TYPES:
            return data_type.simple_string()

        if type_name is Name.STRUCT:
            return cls.write_struct_type(data_type)

        if type_name is Name.LIST:
            return cls.write_list_type(data_type)

        if type_name is Name.MAP:
            return cls.write_map_type(data_type)

        if type_name is Name.UNION:
            return cls.write_union_type(data_type)

        if type_name is Name.EXTERNAL:
            return cls.write_external_type(data_type)

        if type_name is Name.UNPARSED:
            return cls.write_unparsed_type(data_type)

        return cls.write_unparsed_type(data_type.simple_string())

    @classmethod
    def write_struct_type(cls, struct_type: Types.StructType) -> Dict[str, Any]:
        struct_data = {
            cls.TYPE: cls.STRUCT,
            cls.FIELDS: [
                cls.write_struct_field(field) for field in struct_type.fields()
            ],
        }
        return struct_data

    @classmethod
    def write_struct_field(cls, struct_field: Types.StructType.Field) -> Dict[str, Any]:
        struct_field_data = {
            cls.STRUCT_FIELD_NAME: struct_field.name(),
            cls.TYPE: cls.write_data_type(struct_field.type()),
            cls.STRUCT_FIELD_NULLABLE: struct_field.nullable(),
        }
        comment = struct_field.comment()
        if comment is not None:
            struct_field_data.update({cls.STRUCT_FIELD_COMMENT: comment})
        return struct_field_data

    @classmethod
    def write_list_type(cls, list_type: Types.ListType) -> Dict[str, Any]:
        list_data = {
            cls.TYPE: cls.LIST,
            cls.LIST_ELEMENT_TYPE: cls.write_data_type(list_type.element_type()),
            cls.LIST_ELEMENT_NULLABLE: list_type.element_nullable(),
        }
        return list_data

    @classmethod
    def write_map_type(cls, map_type: Types.MapType) -> Dict[str, Any]:
        map_data = {
            cls.TYPE: cls.MAP,
            cls.MAP_VALUE_NULLABLE: map_type.is_value_nullable(),
            cls.MAP_KEY_TYPE: cls.write_data_type(data_type=map_type.key_type()),
            cls.MAP_VALUE_TYPE: cls.write_data_type(data_type=map_type.value_type()),
        }
        return map_data

    @classmethod
    def write_union_type(cls, union_type: Types.UnionType) -> Dict[str, Any]:
        union_data = {
            cls.TYPE: cls.UNION,
            cls.UNION_TYPES: [
                cls.write_data_type(data_type=type_) for type_ in union_type.types()
            ],
        }
        return union_data

    @classmethod
    def write_external_type(cls, external_type: Types.ExternalType) -> Dict[str, str]:
        external_data = {
            cls.TYPE: cls.EXTERNAL,
            cls.CATALOG_STRING: external_type.catalog_string(),
        }
        return external_data

    @classmethod
    @overload
    def write_unparsed_type(cls, unparsed_type: str) -> Dict[str, str]: ...

    @classmethod
    @overload
    def write_unparsed_type(cls, unparsed_type: Type) -> Dict[str, str]: ...

    @classmethod
    def write_unparsed_type(cls, unparsed_type) -> Dict[str, str]:
        return {
            cls.TYPE: cls.UNPARSED,
            cls.UNPARSED_TYPE: (
                unparsed_type.unparsed_type()
                if isinstance(unparsed_type, Types.UnparsedType)
                else unparsed_type
            ),
        }
