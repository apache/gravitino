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

import json
from typing import Any, Dict, Union, overload

from dataclasses_json.core import Json

from gravitino.api.types.type import Name, Type
from gravitino.api.types.types import Types
from gravitino.utils.precondition import Precondition
from gravitino.utils.serdes import SerdesUtilsBase


class SerdesUtils(SerdesUtilsBase):
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

    @classmethod
    def read_data_type(cls, type_data: Json) -> Type:
        """Read Gravitino Type from JSON data. Used for Gravitino Type JSON Deserialization.

        Args:
            type_data (Json): The serialized data.

        Returns:
            Type: The Gravitino Type.
        """
        if type_data is None or isinstance(type_data, (dict, str)):
            Precondition.check_argument(
                type_data is not None and len(type_data) > 0,
                f"Cannot parse type from invalid JSON: {type_data}",
            )

        if isinstance(type_data, str):
            return cls.from_primitive_type_string(type_data.lower())

        if isinstance(type_data, dict) and type_data.get(cls.TYPE):
            type_str = type_data[cls.TYPE]
            if cls.STRUCT == type_str:
                return cls.read_struct_type(type_data)
            if cls.LIST == type_str:
                return cls.read_list_type(type_data)
            if cls.MAP == type_str:
                return cls.read_map_type(type_data)
            if cls.UNION == type_str:
                return cls.read_union_type(type_data)
            if cls.UNPARSED == type_str:
                return cls.read_unparsed_type(type_data)
            if cls.EXTERNAL == type_str:
                return cls.read_external_type(type_data)

        return Types.UnparsedType.of(unparsed_type=json.dumps(type_data))

    @classmethod
    def from_primitive_type_string(cls, type_string: str) -> Type:
        type_instance = cls.TYPES.get(type_string)
        if type_instance is not None:
            return type_instance

        decimal_matched = cls.DECIMAL_PATTERN.match(type_string)
        if decimal_matched:
            return Types.DecimalType.of(
                precision=int(decimal_matched.group(1)),
                scale=int(decimal_matched.group(2)),
            )

        fixed_matched = cls.FIXED_PATTERN.match(type_string)
        if fixed_matched:
            return Types.FixedType.of(length=int(fixed_matched.group(1)))

        fixedchar_matched = cls.FIXEDCHAR_PATTERN.match(type_string)
        if fixedchar_matched:
            return Types.FixedCharType.of(length=int(fixedchar_matched.group(1)))

        varchar_matched = cls.VARCHAR_PATTERN.match(type_string)
        if varchar_matched:
            return Types.VarCharType.of(length=int(varchar_matched.group(1)))

        return Types.UnparsedType.of(type_string)

    @classmethod
    def read_struct_type(cls, struct_data: Dict[str, Any]) -> Types.StructType:
        fields = struct_data.get(cls.FIELDS)
        Precondition.check_argument(
            fields is not None and isinstance(fields, list),
            f"Cannot parse struct fields from non-array: {fields}",
        )
        field_list = [cls.read_struct_field(field) for field in struct_data[cls.FIELDS]]
        return Types.StructType.of(*field_list)

    @classmethod
    def read_struct_field(cls, field_data: Dict[str, Any]) -> Types.StructType.Field:
        Precondition.check_argument(
            field_data is not None and isinstance(field_data, dict),
            f"Cannot parse struct field from invalid JSON: {field_data}",
        )
        Precondition.check_argument(
            field_data.get(cls.STRUCT_FIELD_NAME) is not None,
            f"Cannot parse struct field from missing name: {field_data}",
        )
        Precondition.check_argument(
            field_data.get(cls.TYPE) is not None,
            f"Cannot parse struct field from missing type: {field_data}",
        )

        name = field_data[cls.STRUCT_FIELD_NAME]
        field_type = cls.read_data_type(field_data[cls.TYPE])
        nullable = field_data.get(cls.STRUCT_FIELD_NULLABLE, True)
        comment = field_data.get(cls.STRUCT_FIELD_COMMENT, "")

        return Types.StructType.Field(
            name=name, field_type=field_type, nullable=nullable, comment=comment
        )

    @classmethod
    def read_list_type(cls, list_data: Dict[str, Any]) -> Types.ListType:
        Precondition.check_argument(
            list_data.get(cls.LIST_ELEMENT_TYPE) is not None,
            f"Cannot parse list type from missing element type: {list_data}",
        )
        element_type = cls.read_data_type(list_data[cls.LIST_ELEMENT_TYPE])
        nullable = list_data.get(cls.LIST_ELEMENT_NULLABLE, True)
        return Types.ListType.of(element_type=element_type, element_nullable=nullable)

    @classmethod
    def read_map_type(cls, map_data: Dict[str, Any]) -> Types.MapType:
        Precondition.check_argument(
            map_data.get(cls.MAP_KEY_TYPE) is not None,
            f"Cannot parse map type from missing key type: {map_data}",
        )
        Precondition.check_argument(
            map_data.get(cls.MAP_VALUE_TYPE) is not None,
            f"Cannot parse map type from missing value type: {map_data}",
        )
        key_type = cls.read_data_type(map_data[cls.MAP_KEY_TYPE])
        value_type = cls.read_data_type(map_data[cls.MAP_VALUE_TYPE])
        nullable = map_data.get(cls.MAP_VALUE_NULLABLE, True)
        return Types.MapType.of(key_type, value_type, nullable)

    @classmethod
    def read_union_type(cls, union_data: Dict[str, Any]) -> Types.UnionType:
        Precondition.check_argument(
            union_data.get(cls.UNION_TYPES) is not None,
            f"Cannot parse union type from missing types: {union_data}",
        )
        types = union_data.get(cls.UNION_TYPES)
        Precondition.check_argument(
            types is not None and isinstance(types, list),
            f"Cannot parse union types from non-array: {types}",
        )

        union_types = [cls.read_data_type(type_data) for type_data in types]
        return Types.UnionType.of(*union_types)

    @classmethod
    def read_unparsed_type(cls, data: Dict[str, Any]) -> Types.UnparsedType:
        Precondition.check_argument(
            data.get(cls.UNPARSED_TYPE) is not None,
            f"Cannot parse unparsed type from missing unparsed type: {data}",
        )

        return Types.UnparsedType.of(data[cls.UNPARSED_TYPE])

    @classmethod
    def read_external_type(cls, external_data: Dict[str, Any]) -> Types.ExternalType:
        Precondition.check_argument(
            external_data.get(cls.CATALOG_STRING) is not None,
            f"Cannot parse external type from missing catalogString: {external_data}",
        )
        return Types.ExternalType.of(external_data[cls.CATALOG_STRING])
