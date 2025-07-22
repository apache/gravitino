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
import unittest
from itertools import product

from gravitino.api.column import Column
from gravitino.api.types.json_serdes import TypeSerdes
from gravitino.api.types.json_serdes._helper.serdes_utils import SerdesUtils
from gravitino.api.types.types import Types
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.expressions.field_reference_dto import FieldReferenceDTO
from gravitino.dto.rel.expressions.func_expression_dto import FuncExpressionDTO
from gravitino.dto.rel.expressions.json_serdes.column_default_value_serdes import (
    ColumnDefaultValueSerdes,
)
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO
from gravitino.dto.rel.expressions.unparsed_expression_dto import UnparsedExpressionDTO
from gravitino.exceptions.base import IllegalArgumentException


class TestColumnDTO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._supported_types = [
            *SerdesUtils.TYPES.values(),
            Types.DecimalType.of(10, 2),
            Types.FixedType.of(10),
            Types.FixedCharType.of(10),
            Types.VarCharType.of(10),
            Types.StructType(
                fields=[
                    Types.StructType.Field.not_null_field(
                        name=f"field_{field_idx}",
                        field_type=type_,
                        comment=f"comment {field_idx}" if field_idx % 2 == 0 else "",
                    )
                    for type_, field_idx in zip(
                        SerdesUtils.TYPES.values(),
                        range(len(SerdesUtils.TYPES.values())),
                    )
                ]
            ),
            Types.UnionType.of(Types.DoubleType.get(), Types.FloatType.get()),
            Types.ListType.of(
                element_type=Types.StringType.get(), element_nullable=False
            ),
            Types.MapType.of(
                key_type=Types.StringType.get(),
                value_type=Types.StringType.get(),
                value_nullable=False,
            ),
            Types.ExternalType.of(catalog_string="external_type"),
            Types.UnparsedType.of(unparsed_type="unparsed_type"),
        ]
        cls._string_columns = [
            ColumnDTO.builder()
            .with_name(name=f"column_{idx}")
            .with_data_type(data_type=Types.StringType.get())
            .with_comment(comment=f"column_{idx} comment")
            .build()
            for idx in range(3)
        ]

    def test_column_dto_equality(self):
        column_dto_1 = self._string_columns[1]
        column_dto_2 = self._string_columns[2]
        self.assertNotEqual(column_dto_1, column_dto_2)
        self.assertEqual(column_dto_1, column_dto_1)
        self.assertNotEqual(column_dto_1, "test")

    def test_column_dto_hash(self):
        column_dto_1 = self._string_columns[1]
        column_dto_2 = self._string_columns[2]
        column_dto_dict = {column_dto_1: "column_1", column_dto_2: "column_2"}
        self.assertEqual("column_1", column_dto_dict.get(column_dto_1))
        self.assertNotEqual("column_1", column_dto_dict.get(column_dto_2))

    def test_column_dto_builder(self):
        instance = (
            ColumnDTO.builder()
            .with_name(name="")
            .with_data_type(data_type=Types.StringType.get())
            .with_comment(comment="comment")
            .with_default_value(
                default_value=LiteralDTO(
                    value="default_value", data_type=Types.StringType.get()
                )
            )
            .build()
        )
        self.assertIsInstance(instance, ColumnDTO)
        self.assertEqual(instance.name(), "")
        self.assertEqual(instance.data_type(), Types.StringType.get())
        self.assertEqual(instance.comment(), "comment")
        self.assertFalse(instance.auto_increment())
        self.assertTrue(instance.nullable())
        self.assertIsInstance(instance.default_value(), LiteralDTO)
        self.assertEqual(instance.default_value().value(), "default_value")

        with self.assertRaisesRegex(
            IllegalArgumentException,
            "Column name cannot be null",
        ):
            ColumnDTO.builder().with_data_type(
                data_type=Types.StringType.get()
            ).with_comment(comment="comment").with_default_value(
                default_value=LiteralDTO(
                    value="default_value", data_type=Types.StringType.get()
                )
            ).build()

        with self.assertRaisesRegex(
            IllegalArgumentException,
            "Column data type cannot be null",
        ):
            ColumnDTO.builder().with_name(name="column").with_comment(
                comment="comment"
            ).with_default_value(
                default_value=LiteralDTO(
                    value="default_value", data_type=Types.StringType.get()
                )
            ).build()

    def test_column_dto_violate_non_nullable(self):
        column_dto = (
            ColumnDTO.builder()
            .with_name(name="column_name")
            .with_data_type(data_type=Types.StringType.get())
            .with_comment(comment="comment")
            .with_nullable(nullable=False)
            .with_default_value(
                default_value=LiteralDTO(value="None", data_type=Types.NullType.get())
            )
            .build()
        )
        with self.assertRaisesRegex(
            IllegalArgumentException,
            "Column cannot be non-nullable with a null default value",
        ):
            column_dto.validate()

    def test_column_dto_default_value_not_set(self):
        column_dto = (
            ColumnDTO.builder()
            .with_name(name="column_name")
            .with_data_type(data_type=Types.StringType.get())
            .with_comment(comment="comment")
            .build()
        )

        self.assertEqual(column_dto.name(), "column_name")
        self.assertEqual(column_dto.nullable(), True)
        self.assertEqual(column_dto.auto_increment(), False)
        self.assertEqual(column_dto.comment(), "comment")
        self.assertEqual(column_dto.default_value(), Column.DEFAULT_VALUE_NOT_SET)

    def test_column_dto_serialize_with_default_value_not_set(self):
        """Test if `default_value` is excluded after having been serialized when its
        value is `Column.DEFAULT_VALUE_NOT_SET`
        """

        expected_dict = {
            "name": "",
            "type": "",
            "comment": "",
            "nullable": False,
            "autoIncrement": False,
        }
        for supported_type in self._supported_types:
            column_dto = (
                ColumnDTO.builder()
                .with_name(name=str(supported_type.name()))
                .with_data_type(data_type=supported_type)
                .with_comment(comment=supported_type.simple_string())
                .build()
            )
            expected_dict["name"] = str(supported_type.name())
            expected_dict["type"] = TypeSerdes.serialize(supported_type)
            expected_dict["comment"] = supported_type.simple_string()
            expected_dict["nullable"] = True
            expected_dict["autoIncrement"] = False

            serialized_dict = json.loads(column_dto.to_json())
            self.assertDictEqual(serialized_dict, expected_dict)

    def test_column_dto_deserialize_with_default_value_not_set(self):
        """Test if we can deserialize a valid JSON document of `ColumnDTO` with missing
        `default_value` as a `ColumnDTO` instance with `default_value=Column.DEFAULT_VALUE_NOT_SET`
        """

        for supported_type in self._supported_types:
            column_dto = (
                ColumnDTO.builder()
                .with_name(name=str(supported_type.name()))
                .with_data_type(data_type=supported_type)
                .with_comment(comment=supported_type.simple_string())
                .with_nullable(nullable=True)
                .with_auto_increment(auto_increment=False)
                .build()
            )
            serialized_json = column_dto.to_json()
            deserialized_column_dto = ColumnDTO.from_json(serialized_json)
            deserialized_json = deserialized_column_dto.to_json()

            self.assertIs(
                deserialized_column_dto.default_value(), Column.DEFAULT_VALUE_NOT_SET
            )
            self.assertEqual(serialized_json, deserialized_json)

    def test_column_dto_serdes_with_default_value_literal(self):
        expected_dict = {
            "name": "",
            "type": "",
            "comment": "",
            "defaultValue": None,
            "nullable": False,
            "autoIncrement": False,
        }
        for data_type, default_value_type in product(
            self._supported_types, self._supported_types
        ):
            default_value = (
                LiteralDTO.builder()
                .with_data_type(default_value_type)
                .with_value(default_value_type.simple_string())
                .build()
            )
            column_dto = (
                ColumnDTO.builder()
                .with_name(name=str(data_type.name()))
                .with_data_type(data_type=data_type)
                .with_default_value(default_value=default_value)
                .with_comment(comment=data_type.simple_string())
                .build()
            )
            expected_dict["name"] = str(data_type.name())
            expected_dict["type"] = TypeSerdes.serialize(data_type)
            expected_dict["comment"] = data_type.simple_string()
            expected_dict["defaultValue"] = ColumnDefaultValueSerdes.serialize(
                value=default_value
            )
            expected_dict["nullable"] = True
            expected_dict["autoIncrement"] = False

            serialized_result = column_dto.to_json()
            deserialized_dto = ColumnDTO.from_json(serialized_result)

            self.assertDictEqual(json.loads(serialized_result), expected_dict)
            self.assertEqual(deserialized_dto, column_dto)

    def test_column_dto_serdes_with_default_value_field_ref(self):
        expected_dict = {
            "name": "",
            "type": "",
            "comment": "",
            "defaultValue": None,
            "nullable": False,
            "autoIncrement": False,
        }
        default_value = (
            FieldReferenceDTO.builder().with_column_name(["field_reference"]).build()
        )
        for data_type in self._supported_types:
            column_dto = (
                ColumnDTO.builder()
                .with_name(name=str(data_type.name()))
                .with_data_type(data_type=data_type)
                .with_default_value(default_value=default_value)
                .with_comment(comment=data_type.simple_string())
                .build()
            )
            expected_dict["name"] = str(data_type.name())
            expected_dict["type"] = TypeSerdes.serialize(data_type)
            expected_dict["comment"] = data_type.simple_string()
            expected_dict["defaultValue"] = ColumnDefaultValueSerdes.serialize(
                value=default_value
            )
            expected_dict["nullable"] = True
            expected_dict["autoIncrement"] = False

            serialized_result = column_dto.to_json()
            deserialized_dto = ColumnDTO.from_json(serialized_result)

            self.assertDictEqual(json.loads(serialized_result), expected_dict)
            self.assertEqual(deserialized_dto, column_dto)

    def test_column_dto_serdes_with_default_value_func_expression(self):
        expected_dict = {
            "name": "",
            "type": "",
            "comment": "",
            "defaultValue": None,
            "nullable": False,
            "autoIncrement": False,
        }
        func_args = [
            LiteralDTO.builder()
            .with_data_type(Types.StringType.get())
            .with_value("year")
            .build(),
            FieldReferenceDTO.builder().with_column_name(["birthday"]).build(),
            FuncExpressionDTO.builder()
            .with_function_name("randint")
            .with_function_args(
                [
                    LiteralDTO.builder()
                    .with_data_type(Types.IntegerType.get())
                    .with_value("1")
                    .build(),
                    LiteralDTO.builder()
                    .with_data_type(Types.IntegerType.get())
                    .with_value("100")
                    .build(),
                ]
            )
            .build(),
        ]
        for data_type in self._supported_types:
            default_value = (
                FuncExpressionDTO.builder()
                .with_function_name("test_function")
                .with_function_args(func_args)
                .build()
            )
            column_dto = (
                ColumnDTO.builder()
                .with_name(name=str(data_type.name()))
                .with_data_type(data_type=data_type)
                .with_default_value(default_value=default_value)
                .with_comment(comment=data_type.simple_string())
                .build()
            )
            expected_dict["name"] = str(data_type.name())
            expected_dict["type"] = TypeSerdes.serialize(data_type)
            expected_dict["comment"] = data_type.simple_string()
            expected_dict["defaultValue"] = ColumnDefaultValueSerdes.serialize(
                value=default_value
            )
            expected_dict["nullable"] = True
            expected_dict["autoIncrement"] = False

            serialized_result = column_dto.to_json()
            deserialized_dto = ColumnDTO.from_json(serialized_result)

            self.assertDictEqual(json.loads(serialized_result), expected_dict)
            self.assertEqual(deserialized_dto, column_dto)

    def test_column_dto_serialize_with_default_value_unparsed(self):
        expected_dict = {
            "name": "",
            "type": "",
            "comment": "",
            "defaultValue": None,
            "nullable": False,
            "autoIncrement": False,
        }
        for data_type in self._supported_types:
            default_value = (
                UnparsedExpressionDTO.builder()
                .with_unparsed_expression("unparsed_expression")
                .build()
            )
            column_dto = (
                ColumnDTO.builder()
                .with_name(name=str(data_type.name()))
                .with_data_type(data_type=data_type)
                .with_default_value(default_value=default_value)
                .with_comment(comment=data_type.simple_string())
                .build()
            )
            expected_dict["name"] = str(data_type.name())
            expected_dict["type"] = TypeSerdes.serialize(data_type)
            expected_dict["comment"] = data_type.simple_string()
            expected_dict["defaultValue"] = ColumnDefaultValueSerdes.serialize(
                value=default_value
            )
            expected_dict["nullable"] = True
            expected_dict["autoIncrement"] = False

            serialized_result = column_dto.to_json()
            deserialized_dto = ColumnDTO.from_json(serialized_result)

            self.assertDictEqual(json.loads(serialized_result), expected_dict)
            self.assertEqual(deserialized_dto, column_dto)
