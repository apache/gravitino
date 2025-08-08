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

import unittest
from unittest.mock import patch

from gravitino.api.column import Column
from gravitino.api.types.types import Types
from gravitino.dto.rel.expressions.field_reference_dto import FieldReferenceDTO
from gravitino.dto.rel.expressions.func_expression_dto import FuncExpressionDTO
from gravitino.dto.rel.expressions.json_serdes._helper.serdes_utils import SerdesUtils
from gravitino.dto.rel.expressions.json_serdes.column_default_value_serdes import (
    ColumnDefaultValueSerdes,
)
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO
from gravitino.dto.rel.expressions.unparsed_expression_dto import UnparsedExpressionDTO
from gravitino.exceptions.base import IllegalArgumentException


class TestColumnDefaultValueSerdes(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._literal_dto = (
            LiteralDTO.builder()
            .with_data_type(data_type=Types.StringType.get())
            .with_value(value="test_string")
            .build()
        )
        cls._dtos = [
            cls._literal_dto,
            FieldReferenceDTO.builder()
            .with_field_name(field_name=["field_name"])
            .build(),
            FuncExpressionDTO.builder()
            .with_function_name(function_name="simple_func_name")
            .with_function_args(function_args=[cls._literal_dto])
            .build(),
            UnparsedExpressionDTO.builder()
            .with_unparsed_expression(unparsed_expression="unparsed_expression")
            .build(),
        ]

    def test_column_default_serdes_serialize_empty(self):
        self.assertIsNone(ColumnDefaultValueSerdes.serialize(value=None))
        self.assertIsNone(
            ColumnDefaultValueSerdes.serialize(value=Column.DEFAULT_VALUE_NOT_SET)
        )

    def test_column_default_serdes_deserialize_empty(self):
        self.assertIs(
            ColumnDefaultValueSerdes.deserialize(data=None),
            Column.DEFAULT_VALUE_NOT_SET,
        )

        self.assertRaisesRegex(
            IllegalArgumentException,
            "Cannot parse function arg from invalid JSON",
            ColumnDefaultValueSerdes.deserialize,
            data="None",
        )

    def test_serialize_dto(self):
        for dto in self._dtos:
            with patch.object(
                SerdesUtils, "write_function_arg"
            ) as mock_write_function_arg:
                ColumnDefaultValueSerdes.serialize(value=dto)
                mock_write_function_arg.assert_called_once_with(arg=dto)

    def test_deserialize_dto(self):
        for dto in self._dtos:
            data = ColumnDefaultValueSerdes.serialize(value=dto)
            with patch.object(
                SerdesUtils, "read_function_arg"
            ) as mock_read_function_arg:
                ColumnDefaultValueSerdes.deserialize(data=data)
                mock_read_function_arg.assert_called_once_with(data=data)
