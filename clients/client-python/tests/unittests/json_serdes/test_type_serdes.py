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

import random
import unittest
from itertools import combinations, product

from gravitino.api.rel.expressions.expression import Expression
from gravitino.api.rel.indexes.indexes import Indexes
from gravitino.api.rel.table_change import After, Default, First
from gravitino.api.rel.types.json_serdes import TypeSerdes
from gravitino.api.rel.types.json_serdes._helper.serdes_utils import SerdesUtils
from gravitino.api.rel.types.type import PrimitiveType
from gravitino.api.rel.types.types import Types
from gravitino.dto.rel.expressions.field_reference_dto import FieldReferenceDTO
from gravitino.dto.rel.expressions.func_expression_dto import FuncExpressionDTO
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO
from gravitino.dto.rel.expressions.unparsed_expression_dto import UnparsedExpressionDTO
from gravitino.dto.rel.indexes.index_dto import Index
from gravitino.exceptions.base import IllegalArgumentException


class MockType(PrimitiveType):
    def __init__(self, name: str):
        self._name = name

    def name(self) -> str:
        return self._name

    def simple_string(self):
        return "mock_type"


class TestTypeSerdes(unittest.TestCase):
    def setUp(self):
        self._primitive_and_none_types = {
            **SerdesUtils.TYPES,
            **{
                "decimal(10,2)": Types.DecimalType.of(10, 2),
                "fixed(10)": Types.FixedType.of(10),
                "char(10)": Types.FixedCharType.of(10),
                "varchar(10)": Types.VarCharType.of(10),
                "time(6)": Types.TimeType.of(6),
                "time(3)": Types.TimeType.of(3),
                "timestamp(6)": Types.TimestampType.without_time_zone(6),
                "timestamp(0)": Types.TimestampType.without_time_zone(0),
                "timestamp_tz(6)": Types.TimestampType.with_time_zone(6),
                "timestamp_tz(3)": Types.TimestampType.with_time_zone(3),
            },
        }

    def test_serialize_primitive_and_none_type(self):
        for simple_string, type_ in self._primitive_and_none_types.items():
            self.assertEqual(TypeSerdes.serialize(data_type=type_), simple_string)

    def test_serialize_struct_type_of_primitive_and_none_types(self):
        types = self._primitive_and_none_types.values()
        fields = [
            Types.StructType.Field.not_null_field(
                name=f"field_{field_idx}",
                field_type=type_,
                comment=f"comment {field_idx}" if field_idx % 2 == 0 else None,
            )
            for type_, field_idx in zip(types, range(len(types)))
        ]

        struct_type = Types.StructType.of(*fields)
        serialized_result = TypeSerdes.serialize(struct_type)
        serialized_fields = serialized_result.get(SerdesUtils.FIELDS)

        self.assertEqual(serialized_result.get(SerdesUtils.TYPE), SerdesUtils.STRUCT)
        for field, serialized_field in zip(fields, serialized_fields):
            self.assertEqual(
                serialized_field.get(SerdesUtils.STRUCT_FIELD_NAME), field.name()
            )
            self.assertEqual(
                serialized_field.get(SerdesUtils.TYPE),
                SerdesUtils.write_data_type(field.type()),
            )
            self.assertEqual(
                serialized_field.get(SerdesUtils.STRUCT_FIELD_NULLABLE),
                field.nullable(),
            )
            self.assertEqual(
                serialized_field.get(SerdesUtils.STRUCT_FIELD_COMMENT), field.comment()
            )

    def test_serialize_list_type_of_primitive_and_none_types(self):
        for simple_string, type_ in self._primitive_and_none_types.items():
            list_type = Types.ListType.of(element_type=type_, element_nullable=False)
            serialized_result = TypeSerdes.serialize(list_type)
            self.assertEqual(serialized_result.get(SerdesUtils.TYPE), SerdesUtils.LIST)
            self.assertEqual(
                serialized_result.get(SerdesUtils.LIST_ELEMENT_TYPE), simple_string
            )
            self.assertEqual(
                serialized_result.get(SerdesUtils.LIST_ELEMENT_NULLABLE), False
            )

    def test_serialize_map_type_of_primitive_and_none_types(self):
        types = self._primitive_and_none_types.values()
        for key_type, value_type in product(types, types):
            map_type = Types.MapType.of(
                key_type=key_type, value_type=value_type, value_nullable=False
            )
            serialized_result = TypeSerdes.serialize(map_type)
            self.assertEqual(serialized_result.get(SerdesUtils.TYPE), SerdesUtils.MAP)
            self.assertEqual(
                serialized_result.get(SerdesUtils.MAP_KEY_TYPE),
                key_type.simple_string(),
            )
            self.assertEqual(
                serialized_result.get(SerdesUtils.MAP_VALUE_TYPE),
                value_type.simple_string(),
            )
            self.assertEqual(
                serialized_result.get(SerdesUtils.MAP_VALUE_NULLABLE), False
            )

    def test_serialize_union_type_of_primitive_and_none_types(self):
        types = self._primitive_and_none_types.values()
        for types in combinations(types, 2):
            union_type = Types.UnionType.of(*types)
            serialized_result = TypeSerdes.serialize(union_type)
            self.assertEqual(serialized_result.get(SerdesUtils.TYPE), SerdesUtils.UNION)
            self.assertListEqual(
                serialized_result.get(SerdesUtils.UNION_TYPES),
                [type_.simple_string() for type_ in types],
            )

    def test_serialize_external_type(self):
        external_type = Types.ExternalType.of(catalog_string="catalog_string")
        serialized_result = TypeSerdes.serialize(external_type)
        self.assertEqual(serialized_result.get(SerdesUtils.TYPE), SerdesUtils.EXTERNAL)
        self.assertEqual(
            serialized_result.get(SerdesUtils.CATALOG_STRING), "catalog_string"
        )

    def test_write_unparsed_type(self):
        unparsed_type = Types.UnparsedType.of(unparsed_type="unparsed_type")
        serialized_result = TypeSerdes.serialize(unparsed_type)
        self.assertEqual(serialized_result.get(SerdesUtils.TYPE), SerdesUtils.UNPARSED)
        self.assertEqual(
            serialized_result.get(SerdesUtils.UNPARSED_TYPE), "unparsed_type"
        )

        mock_type = MockType(name="mock")
        result = TypeSerdes.serialize(mock_type)
        self.assertEqual(result.get(SerdesUtils.TYPE), SerdesUtils.UNPARSED)
        self.assertEqual(
            result.get(SerdesUtils.UNPARSED_TYPE), mock_type.simple_string()
        )

    def test_deserialize_primitive_and_none_type(self):
        for simple_string, type_ in self._primitive_and_none_types.items():
            self.assertEqual(TypeSerdes.deserialize(data=simple_string), type_)

    def test_deserialize_invalid_primitive_and_non_type(self):
        invalid_data = ["", {}, None]
        for data in invalid_data:
            self.assertRaises(
                IllegalArgumentException,
                TypeSerdes.deserialize,
                data=data,
            )

    def test_deserialize_primitive_and_non_type_unparsed(self):
        unparsed_data = [
            int(random.random() * 10),
            random.random(),
            "invalid_type",
            {"invalid_key": "value"},
            list(range(10)),
            True,
        ]
        for data in unparsed_data:
            result = TypeSerdes.deserialize(data=data)
            self.assertIsInstance(result, Types.UnparsedType)

    def test_deserialize_struct_type(self):
        types = self._primitive_and_none_types.values()
        fields = [
            Types.StructType.Field.not_null_field(
                name=f"field_{field_idx}",
                field_type=type_,
                comment=f"comment {field_idx}" if field_idx % 2 == 0 else "",
            )
            for type_, field_idx in zip(types, range(len(types)))
        ]

        struct_type = Types.StructType.of(*fields)
        serialized_result = TypeSerdes.serialize(struct_type)
        deserialized_result = TypeSerdes.deserialize(data=serialized_result)
        self.assertEqual(deserialized_result, struct_type)

    def test_deserialize_struct_type_invalid_fields(self):
        message_prefix = "Cannot parse struct fields? from"
        field_data = {"type": SerdesUtils.STRUCT}
        invalid_data = (
            {**field_data, **{"fields": "non-array-fields"}},
            {**field_data, **{"fields": ["invalid_field"]}},
            {**field_data, **{"fields": [{"invalid_name": "value"}]}},
            {
                **field_data,
                **{"fields": [{"name": "valid_field_name", "invalid_type": "value"}]},
            },
        )
        messages = (
            f"{message_prefix} non-array",
            f"{message_prefix} invalid JSON",
            f"{message_prefix} missing name",
            f"{message_prefix} missing type",
        )
        for data, message in zip(invalid_data, messages):
            self.assertRaisesRegex(
                IllegalArgumentException,
                message,
                TypeSerdes.deserialize,
                data=data,
            )

    def test_deserialize_list_type(self):
        types = self._primitive_and_none_types.values()
        for type_ in types:
            list_type = Types.ListType.of(element_type=type_, element_nullable=False)
            serialized_result = TypeSerdes.serialize(list_type)
            deserialized_result = TypeSerdes.deserialize(data=serialized_result)
            self.assertEqual(
                list_type.simple_string(), deserialized_result.simple_string()
            )

    def test_deserialize_list_type_invalid_data(self):
        list_data = {"type": "list", "invalid_element_type": "value"}
        self.assertRaisesRegex(
            IllegalArgumentException,
            "Cannot parse list type from missing element type",
            TypeSerdes.deserialize,
            data=list_data,
        )

    def test_deserialize_map_type(self):
        types = self._primitive_and_none_types.values()
        for key_type, value_type in product(types, types):
            map_type = Types.MapType.of(
                key_type=key_type, value_type=value_type, value_nullable=False
            )
            serialized_result = TypeSerdes.serialize(map_type)
            deserialized_result = TypeSerdes.deserialize(data=serialized_result)
            self.assertEqual(
                map_type.simple_string(), deserialized_result.simple_string()
            )

    def test_deserialize_map_type_invalid_data(self):
        invalid_map_data = (
            {"type": "map", "invalid_key_type": "value"},
            {
                "type": "map",
                "keyType": "valid_key",
                "invalid_value_type": "invalid_value",
            },
        )
        for data in invalid_map_data:
            self.assertRaisesRegex(
                IllegalArgumentException,
                "Cannot parse map type from missing (key|value) type",
                TypeSerdes.deserialize,
                data=data,
            )

    def test_deserialize_union_type(self):
        types = self._primitive_and_none_types.values()
        for types in combinations(types, 2):
            union_type = Types.UnionType.of(*types)
            serialized_result = TypeSerdes.serialize(union_type)
            deserialized_result = TypeSerdes.deserialize(data=serialized_result)
            self.assertEqual(
                union_type.simple_string(), deserialized_result.simple_string()
            )

    def test_deserialize_union_type_invalid_data(self):
        invalid_union_data = (
            {"type": "union", "invalid_types": "invalid_types"},
            {"type": "union", "types": "invalid_types_value"},
        )
        for data in invalid_union_data:
            self.assertRaisesRegex(
                IllegalArgumentException,
                "Cannot parse union types? from (?:non-array|missing types)",
                TypeSerdes.deserialize,
                data=data,
            )

    def test_deserialize_unparsed_type(self):
        unparsed_type = Types.UnparsedType.of(unparsed_type="unparsed_type")
        serialized_result = TypeSerdes.serialize(unparsed_type)
        deserialized_result = TypeSerdes.deserialize(data=serialized_result)
        self.assertEqual(
            unparsed_type.simple_string(), deserialized_result.simple_string()
        )

    def test_deserialize_unparsed_type_invalid_data(self):
        invalid_data = {"type": "unparsed"}
        self.assertRaisesRegex(
            IllegalArgumentException,
            "Cannot parse unparsed type from missing unparsed type",
            TypeSerdes.deserialize,
            data=invalid_data,
        )

    def test_deserialize_external_type(self):
        external_type = Types.ExternalType.of(catalog_string="catalog_string")
        serialized_result = TypeSerdes.serialize(external_type)
        deserialized_result = TypeSerdes.deserialize(data=serialized_result)
        self.assertEqual(
            external_type.simple_string(), deserialized_result.simple_string()
        )

    def test_deserialize_external_type_invalid_data(self):
        invalid_data = {"type": "external"}
        self.assertRaisesRegex(
            IllegalArgumentException,
            "Cannot parse external type from missing catalogString",
            TypeSerdes.deserialize,
            data=invalid_data,
        )

    def test_time_type_precision_serialization(self):
        """Test the serialization and deserialization of time type precision"""
        # Test TimeType with precision
        time_with_precision = Types.TimeType.of(6)
        serialized = TypeSerdes.serialize(time_with_precision)
        self.assertEqual(serialized, "time(6)")

        deserialized = TypeSerdes.deserialize(serialized)
        self.assertEqual(deserialized, time_with_precision)
        self.assertTrue(deserialized.has_precision_set())
        self.assertEqual(deserialized.precision(), 6)

        # Test TimestampType with precision (without timezone)
        timestamp_with_precision = Types.TimestampType.without_time_zone(3)
        serialized = TypeSerdes.serialize(timestamp_with_precision)
        self.assertEqual(serialized, "timestamp(3)")

        deserialized = TypeSerdes.deserialize(serialized)
        self.assertEqual(deserialized, timestamp_with_precision)
        self.assertTrue(deserialized.has_precision_set())
        self.assertEqual(deserialized.precision(), 3)
        self.assertFalse(deserialized.has_time_zone())

        # Test TimestampType with precision (with timezone)
        timestamp_tz_with_precision = Types.TimestampType.with_time_zone(9)
        serialized = TypeSerdes.serialize(timestamp_tz_with_precision)
        self.assertEqual(serialized, "timestamp_tz(9)")

        deserialized = TypeSerdes.deserialize(serialized)
        self.assertEqual(deserialized, timestamp_tz_with_precision)
        self.assertTrue(deserialized.has_precision_set())
        self.assertEqual(deserialized.precision(), 9)
        self.assertTrue(deserialized.has_time_zone())

    def test_backward_compatibility(self):
        """Test forward compatibility - Time types without precision should work properly"""
        # Test TimeType without precision
        time_without_precision = Types.TimeType.get()
        serialized = TypeSerdes.serialize(time_without_precision)
        self.assertEqual(serialized, "time")

        deserialized = TypeSerdes.deserialize(serialized)
        self.assertEqual(deserialized, time_without_precision)
        self.assertFalse(deserialized.has_precision_set())

        # Test TimestampType without precision (without timezone)
        timestamp_without_precision = Types.TimestampType.without_time_zone()
        serialized = TypeSerdes.serialize(timestamp_without_precision)
        self.assertEqual(serialized, "timestamp")

        deserialized = TypeSerdes.deserialize(serialized)
        self.assertEqual(deserialized, timestamp_without_precision)
        self.assertFalse(deserialized.has_precision_set())
        self.assertFalse(deserialized.has_time_zone())

        # Test TimestampType without precision (with timezone)
        timestamp_tz_without_precision = Types.TimestampType.with_time_zone()
        serialized = TypeSerdes.serialize(timestamp_tz_without_precision)
        self.assertEqual(serialized, "timestamp_tz")

        deserialized = TypeSerdes.deserialize(serialized)
        self.assertEqual(deserialized, timestamp_tz_without_precision)
        self.assertFalse(deserialized.has_precision_set())
        self.assertTrue(deserialized.has_time_zone())

    def test_column_default_value_encoder_none(self) -> None:
        self.assertIsNone(SerdesUtils.column_default_value_encoder(None))
        self.assertIsNone(
            SerdesUtils.column_default_value_encoder(Expression.EMPTY_EXPRESSION)
        )

    def test_column_default_value_encoder_with_literal(self) -> None:
        literal = (
            LiteralDTO.builder()
            .with_data_type(Types.DateType.get())
            .with_value("2023-04-01")
            .build()
        )

        serialized = SerdesUtils.column_default_value_encoder(literal)
        expected = {
            "type": "literal",
            "dataType": "date",
            "value": "2023-04-01",
        }
        self.assertEqual(expected, serialized)

    def test_column_default_value_encoder_with_field(self) -> None:
        field = (
            FieldReferenceDTO.builder().with_field_name(["field1", "field2"]).build()
        )

        serialized = SerdesUtils.column_default_value_encoder(field)
        expected = {
            "type": "field",
            "fieldName": ["field1", "field2"],
        }
        self.assertEqual(expected, serialized)

    def test_column_default_value_encoder_with_function(self) -> None:
        arg1 = FieldReferenceDTO.builder().with_field_name(["dt"]).build()
        arg2 = (
            LiteralDTO.builder()
            .with_data_type(Types.StringType.get())
            .with_value("Asia/Shanghai")
            .build()
        )
        to_date_func = (
            FuncExpressionDTO.builder()
            .with_function_name("toDate")
            .with_function_args([arg1, arg2])
            .build()
        )

        serialized = SerdesUtils.column_default_value_encoder(to_date_func)
        expected = {
            "type": "function",
            "funcName": "toDate",
            "funcArgs": [
                {
                    "type": "field",
                    "fieldName": ["dt"],
                },
                {
                    "type": "literal",
                    "dataType": "string",
                    "value": "Asia/Shanghai",
                },
            ],
        }
        self.assertEqual(expected, serialized)

    def test_column_default_value_encoder_with_unparsed(self) -> None:
        unparsed = (
            UnparsedExpressionDTO.builder().with_unparsed_expression("customer").build()
        )
        serialized = SerdesUtils.column_default_value_encoder(unparsed)
        expected = {
            "type": "unparsed",
            "unparsedExpression": "customer",
        }
        self.assertEqual(expected, serialized)

    def test_column_default_value_decoder_with_none(self) -> None:
        self.assertEqual(
            Expression.EMPTY_EXPRESSION, SerdesUtils.column_default_value_decoder(None)
        )

    def test_column_default_value_decoder_with_literal(self) -> None:
        json_str = {
            "type": "literal",
            "dataType": "string",
            "value": "Asia/Shanghai",
        }
        expr: LiteralDTO = SerdesUtils.column_default_value_decoder(json_str)
        self.assertEqual("Asia/Shanghai", expr.value())
        self.assertEqual(expr.data_type(), Types.StringType.get())

    def test_column_default_value_decoder_with_field(self) -> None:
        json_str = {
            "type": "field",
            "fieldName": ["field1", "field2"],
        }
        expr: FieldReferenceDTO = SerdesUtils.column_default_value_decoder(json_str)
        self.assertEqual(expr.field_name(), ["field1", "field2"])

    def test_column_default_value_decoder_with_function(self) -> None:
        json_str = {
            "type": "function",
            "funcName": "toDate",
            "funcArgs": [
                {
                    "type": "field",
                    "fieldName": ["dt"],
                },
                {
                    "type": "literal",
                    "dataType": "string",
                    "value": "Asia/Shanghai",
                },
            ],
        }

        expr: FuncExpressionDTO = SerdesUtils.column_default_value_decoder(json_str)
        self.assertEqual("toDate", expr.function_name())
        self.assertEqual(2, len(expr.args()))

    def test_column_default_value_decoder_with_unparsed(self) -> None:
        json_str = {"type": "unparsed", "unparsedExpression": "unparsed expression"}

        expr: UnparsedExpressionDTO = SerdesUtils.column_default_value_decoder(json_str)
        self.assertEqual(expr.unparsed_expression(), "unparsed expression")

    def test_column_position_serializer(self) -> None:
        with self.assertRaises(ValueError):
            SerdesUtils.column_position_encoder(None)

        self.assertEqual(
            SerdesUtils.column_position_encoder(First()),
            "first",
        )

        self.assertEqual(
            SerdesUtils.column_position_encoder(After("colA")),
            {"after": "colA"},
        )

        self.assertEqual(
            SerdesUtils.column_position_encoder(Default()),
            "default",
        )

    def test_column_position_deserializer(self) -> None:
        with self.assertRaises(ValueError):
            SerdesUtils.column_position_decoder(None)

        self.assertIsInstance(
            SerdesUtils.column_position_decoder("first"),
            First,
        )

        self.assertIsInstance(
            SerdesUtils.column_position_decoder("FIRST"),
            First,
        )

        self.assertIsInstance(
            SerdesUtils.column_position_decoder({"after": "colA"}),
            After,
        )

        self.assertEqual(
            SerdesUtils.column_position_decoder({"after": "colA"}).get_column(),
            "colA",
        )

        self.assertIsInstance(
            SerdesUtils.column_position_decoder("default"),
            Default,
        )

        self.assertIsInstance(
            SerdesUtils.column_position_decoder("DEFAULT"),
            Default,
        )

    def test_table_index_serializer(self) -> None:
        index_obj = Indexes.create_mysql_primary_key([["a", "b"]])
        serialized = SerdesUtils.table_index_encoder(index_obj)
        expected = {
            "indexType": "PRIMARY_KEY",
            "name": Indexes.DEFAULT_MYSQL_PRIMARY_KEY_NAME,
            "fieldNames": [["a", "b"]],
        }
        self.assertEqual(serialized, expected)

        index_obj = Indexes.of(Index.IndexType.PRIMARY_KEY, None, [["a", "b"]])
        serialized = SerdesUtils.table_index_encoder(index_obj)
        expected = {
            "indexType": "PRIMARY_KEY",
            "fieldNames": [["a", "b"]],
        }
        self.assertEqual(serialized, expected)

        index_obj = Indexes.unique("uk_1", [["a", "b"]])
        serialized = SerdesUtils.table_index_encoder(index_obj)
        expected = {
            "indexType": "UNIQUE_KEY",
            "name": "uk_1",
            "fieldNames": [["a", "b"]],
        }
        self.assertEqual(expected, serialized)

    def test_table_index_deserialize(self) -> None:
        data = {
            "indexType": "PRIMARY_KEY",
            "name": "idx_test",
            "fieldNames": ["a", "b"],
        }

        result = SerdesUtils.table_index_decoder(data)
        self.assertEqual(result.name(), "idx_test")
        self.assertEqual(result.type(), Index.IndexType.PRIMARY_KEY)
        self.assertEqual(result.field_names(), ["a", "b"])

        data = {
            "indexType": "PRIMARY_KEY",
            "fieldNames": ["a", "b"],
        }

        result = SerdesUtils.table_index_decoder(data)
        self.assertIsNone(result.name())
        self.assertEqual(result.type(), Index.IndexType.PRIMARY_KEY)
        self.assertEqual(result.field_names(), ["a", "b"])
