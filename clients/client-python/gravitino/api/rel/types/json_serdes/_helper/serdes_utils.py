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
from typing import Any, Dict, Optional, Union, overload

from dataclasses_json.core import Json

from gravitino.api.rel.expressions.expression import Expression
from gravitino.api.rel.indexes.index import Index
from gravitino.api.rel.table_change import After, Default, First, TableChange
from gravitino.api.rel.types.type import Name, Type
from gravitino.api.rel.types.types import Types
from gravitino.dto.rel.expressions.field_reference_dto import FieldReferenceDTO
from gravitino.dto.rel.expressions.func_expression_dto import FuncExpressionDTO
from gravitino.dto.rel.expressions.function_arg import FunctionArg
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO
from gravitino.dto.rel.expressions.unparsed_expression_dto import UnparsedExpressionDTO
from gravitino.dto.rel.indexes.index_dto import IndexDTO
from gravitino.utils.precondition import Precondition
from gravitino.utils.serdes import SerdesUtilsBase


class SerdesUtils(SerdesUtilsBase):
    POSITION_FIRST = "first"
    POSITION_AFTER = "after"
    POSITION_DEFAULT = "default"

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

        time_matched = cls.TIME_PATTERN.match(type_string)
        if time_matched:
            return Types.TimeType.of(precision=int(time_matched.group(1)))

        timestamp_matched = cls.TIMESTAMP_PATTERN.match(type_string)
        if timestamp_matched:
            return Types.TimestampType.without_time_zone(
                precision=int(timestamp_matched.group(1))
            )

        timestamp_tz_matched = cls.TIMESTAMP_TZ_PATTERN.match(type_string)
        if timestamp_tz_matched:
            return Types.TimestampType.with_time_zone(
                precision=int(timestamp_tz_matched.group(1))
            )

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

    @classmethod
    def column_default_value_encoder(cls, value: Expression) -> Union[str, None]:
        if value is None or value == Expression.EMPTY_EXPRESSION:
            return None

        return cls.write_function_arg(value)

    @classmethod
    def write_function_arg(cls, arg: FunctionArg) -> Any:
        if arg.arg_type() == FunctionArg.ArgType.LITERAL:
            return {
                cls.TYPE: arg.arg_type().value.lower(),
                cls.DATA_TYPE: cls.write_data_type(arg.data_type()),
                cls.LITERAL_VALUE: arg.value(),
            }

        if arg.arg_type() == FunctionArg.ArgType.FIELD:
            return {
                cls.TYPE: arg.arg_type().value.lower(),
                cls.FIELD_NAME: arg.field_name(),
            }

        if arg.arg_type() == FunctionArg.ArgType.FUNCTION:
            return {
                cls.TYPE: arg.arg_type().value.lower(),
                cls.FUNCTION_NAME: arg.function_name(),
                cls.FUNCTION_ARGS: [
                    cls.write_function_arg(child) for child in arg.args()
                ],
            }

        if arg.arg_type() == FunctionArg.ArgType.UNPARSED:
            return {
                cls.TYPE: arg.arg_type().value.lower(),
                cls.UNPARSED_EXPRESSION: arg.unparsed_expression(),
            }

        raise ValueError(f"Unknown function argument type: {arg.arg_type()}")

    @classmethod
    def column_default_value_decoder(cls, value: dict) -> Expression:
        if value is None:
            return Expression.EMPTY_EXPRESSION

        return cls.read_function_arg(value)

    @classmethod
    def read_function_arg(cls, value: dict[str, Any]) -> Expression:
        obj_type = value.get(cls.EXPRESSION_TYPE)
        if obj_type is None:
            raise ValueError(f"Cannot parse function arg from missing type: {value}")

        arg_type = FunctionArg.ArgType[obj_type.upper()]

        if arg_type == FunctionArg.ArgType.LITERAL:
            if cls.DATA_TYPE not in value:
                raise ValueError(
                    f"Cannot parse literal arg from missing data type: {value}"
                )

            if cls.LITERAL_VALUE not in value:
                raise ValueError(
                    f"Cannot parse literal arg from missing literal value: {value}"
                )

            data_type = cls.read_data_type(value[cls.DATA_TYPE])
            data_value = value[cls.LITERAL_VALUE]

            return (
                LiteralDTO.builder()
                .with_data_type(data_type)
                .with_value(data_value)
                .build()
            )

        if arg_type == FunctionArg.ArgType.FIELD:
            if cls.FIELD_NAME not in value:
                raise ValueError(
                    f"Cannot parse field arg from missing field name: {value}"
                )

            field_name = value[cls.FIELD_NAME]
            return FieldReferenceDTO.builder().with_field_name(field_name).build()

        if arg_type == FunctionArg.ArgType.FUNCTION:
            if cls.FUNCTION_NAME not in value:
                raise ValueError(
                    f"Cannot parse function arg from missing function name: {value}"
                )

            if cls.FUNCTION_ARGS not in value:
                raise ValueError(
                    f"Cannot parse function arg from missing function args: {value}"
                )

            function_name = value[cls.FUNCTION_NAME]
            function_args = value[cls.FUNCTION_ARGS]

            return (
                FuncExpressionDTO.builder()
                .with_function_name(function_name)
                .with_function_args(function_args)
                .build()
            )

        if arg_type == FunctionArg.ArgType.UNPARSED:
            if cls.UNPARSED_EXPRESSION not in value or not isinstance(
                value[cls.UNPARSED_EXPRESSION], str
            ):
                raise ValueError(
                    f"Cannot parse unparsed expression from missing string field unparsedExpression: {value}"
                )

            return (
                UnparsedExpressionDTO.builder()
                .with_unparsed_expression(value.get(cls.UNPARSED_EXPRESSION))
                .build()
            )

        raise ValueError(f"Unknown function argument type:  {arg_type}")

    @classmethod
    def table_index_encoder(cls, index: Index) -> Optional[dict]:
        if index is None:
            return None

        if index.name() is not None:
            data = {
                cls.INDEX_TYPE: str(index.type().name).upper(),
                cls.INDEX_NAME: index.name(),
                cls.INDEX_FIELD_NAMES: index.field_names(),
            }

        else:
            data = {
                cls.INDEX_TYPE: str(index.type().name).upper(),
                cls.INDEX_FIELD_NAMES: index.field_names(),
            }

        return data

    @classmethod
    def table_index_decoder(cls, data: dict) -> Optional[Index]:
        if data is None:
            return None

        builder = IndexDTO.builder()
        index_type_str = data.get("indexType")
        name = data.get("name")
        field_names = data.get("fieldNames")

        if index_type_str is None or field_names is None:
            raise ValueError(
                "Invalid index data: 'indexType' and 'fieldNames' are required"
            )
        builder.with_index_type(Index.IndexType[index_type_str.upper()])
        builder.with_field_names(field_names)

        if name is not None:
            builder.with_name(name)

        return builder.build()

    @classmethod
    def column_position_encoder(
        cls,
        value: TableChange.ColumnPosition,
    ) -> Union[str, dict[str, str]]:
        if isinstance(value, First):
            return cls.POSITION_FIRST
        if isinstance(value, After):
            return {
                cls.POSITION_AFTER: value.get_column(),
            }

        if isinstance(value, Default):
            return cls.POSITION_DEFAULT

        raise ValueError(f"Unknown column position: {value}")

    @classmethod
    def column_position_decoder(
        cls,
        value: Union[str, dict[str, str]],
    ) -> Union[First, After, Default]:
        if isinstance(value, str):
            if value == cls.POSITION_FIRST or value == cls.POSITION_FIRST.upper():
                return TableChange.ColumnPosition.first()

            if value == cls.POSITION_DEFAULT or value == cls.POSITION_DEFAULT.upper():
                return TableChange.ColumnPosition.default_pos()

        if isinstance(value, dict):
            after_column = value.get(cls.POSITION_AFTER)
            if after_column:
                return TableChange.ColumnPosition.after(after_column)

        raise ValueError(f"Unknown json column position: {value}")
