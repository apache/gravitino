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

from gravitino.api.column import Column
from gravitino.api.types.json_serdes import TypeSerdes
from gravitino.api.types.json_serdes._helper.serdes_utils import SerdesUtils
from gravitino.api.types.types import Types
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO
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
            ColumnDTO.builder(
                name=f"column_{idx}",
                data_type=Types.StringType.get(),
                comment=f"column_{idx} comment",
            )
            for idx in range(3)
        ]

    def test_column_dto_equality(self):
        column_dto_1 = self._string_columns[1]
        column_dto_2 = self._string_columns[2]
        self.assertNotEqual(column_dto_1, column_dto_2)
        self.assertEqual(column_dto_1, column_dto_1)

    def test_column_dto_hash(self):
        column_dto_1 = self._string_columns[1]
        column_dto_2 = self._string_columns[2]
        column_dto_dict = {column_dto_1: "column_1", column_dto_2: "column_2"}
        self.assertEqual("column_1", column_dto_dict.get(column_dto_1))
        self.assertNotEqual("column_1", column_dto_dict.get(column_dto_2))

    def test_column_dto_validate(self):
        with self.assertRaisesRegex(
            IllegalArgumentException,
            "Column name cannot be null or empty.",
        ):
            ColumnDTO.builder(
                name="",
                data_type=Types.StringType.get(),
                comment="comment",
                default_value=LiteralDTO(
                    value="default_value", data_type=Types.StringType.get()
                ),
            )

        with self.assertRaisesRegex(
            IllegalArgumentException,
            "Column data type cannot be null.",
        ):
            ColumnDTO.builder(
                name="column",
                data_type=None,
                comment="comment",
                default_value=LiteralDTO(
                    value="default_value", data_type=Types.StringType.get()
                ),
            )

    def test_column_dto_violate_non_nullable(self):
        column_dto = ColumnDTO.builder(
            name="column_name",
            data_type=Types.StringType.get(),
            comment="comment",
            nullable=False,
            default_value=LiteralDTO(value="None", data_type=Types.NullType.get()),
        )
        with self.assertRaisesRegex(
            IllegalArgumentException,
            "Column cannot be non-nullable with a null default value",
        ):
            column_dto.validate()

    def test_column_dto_default_value_not_set(self):
        column_dto = ColumnDTO.builder(
            name="column_name",
            data_type=Types.StringType.get(),
            comment="comment",
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
            column_dto = ColumnDTO.builder(
                name=str(supported_type.name()),
                data_type=supported_type,
                comment=supported_type.simple_string(),
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
            column_dto = ColumnDTO.builder(
                name=str(supported_type.name()),
                data_type=supported_type,
                comment=supported_type.simple_string(),
                nullable=True,
                auto_increment=False,
            )
            serialized_json = column_dto.to_json()
            deserialized_column_dto = ColumnDTO.from_json(serialized_json)
            deserialized_json = deserialized_column_dto.to_json()

            self.assertIs(
                deserialized_column_dto.default_value(), Column.DEFAULT_VALUE_NOT_SET
            )
            self.assertEqual(serialized_json, deserialized_json)
