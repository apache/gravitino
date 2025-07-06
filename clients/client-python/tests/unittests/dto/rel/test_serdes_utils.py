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
from enum import Enum
from unittest.mock import patch

from gravitino.api.types.types import Types
from gravitino.dto.rel.expressions.field_reference_dto import FieldReferenceDTO
from gravitino.dto.rel.expressions.func_expression_dto import FuncExpressionDTO
from gravitino.dto.rel.expressions.function_arg import FunctionArg
from gravitino.dto.rel.expressions.json_serdes._helper.serdes_utils import SerdesUtils
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO
from gravitino.dto.rel.expressions.unparsed_expression_dto import UnparsedExpressionDTO
from gravitino.exceptions.base import IllegalArgumentException


class MockArgType(str, Enum):
    INVALID_ARG_TYPE = "invalid_arg_type"


class TestExpressionSerdesUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._literal_dto = (
            LiteralDTO.builder()
            .with_data_type(data_type=Types.StringType.get())
            .with_value(value="test_string")
            .build()
        )
        cls._field_reference_dto = (
            FieldReferenceDTO.builder()
            .with_field_name(field_name=["field_name"])
            .build()
        )
        cls._naive_func_expression_dto = (
            FuncExpressionDTO.builder()
            .with_function_name(function_name="simple_func_name")
            .with_function_args(function_args=[cls._literal_dto])
            .build()
        )
        cls._func_expression_dto = (
            FuncExpressionDTO.builder()
            .with_function_name(function_name="func_name")
            .with_function_args(
                function_args=[
                    cls._literal_dto,
                    cls._field_reference_dto,
                    cls._naive_func_expression_dto,
                ]
            )
            .build()
        )
        cls._unparsed_expression_dto = (
            UnparsedExpressionDTO.builder()
            .with_unparsed_expression(unparsed_expression="unparsed_expression")
            .build()
        )

    def test_write_function_arg_invalid_arg_type(self):
        mock_dto = (
            LiteralDTO.builder()
            .with_data_type(data_type=Types.StringType.get())
            .with_value(value="test")
            .build()
        )
        with patch.object(
            mock_dto, "arg_type", return_value=MockArgType.INVALID_ARG_TYPE
        ):
            self.assertRaises(ValueError, SerdesUtils.write_function_arg, arg=mock_dto)

    def test_write_function_arg_literal_dto(self):
        result = SerdesUtils.write_function_arg(arg=self._literal_dto)
        expected_result = {
            SerdesUtils.EXPRESSION_TYPE: self._literal_dto.arg_type().name.lower(),
            SerdesUtils.DATA_TYPE: self._literal_dto.data_type().simple_string(),
            SerdesUtils.LITERAL_VALUE: self._literal_dto.value(),
        }
        self.assertDictEqual(result, expected_result)

    def test_write_function_arg_field_reference_dto(self):
        result = SerdesUtils.write_function_arg(arg=self._field_reference_dto)
        expected_result = {
            SerdesUtils.EXPRESSION_TYPE: self._field_reference_dto.arg_type().name.lower(),
            SerdesUtils.FIELD_NAME: self._field_reference_dto.field_name(),
        }
        self.assertDictEqual(result, expected_result)

    def test_write_function_arg_func_expression_dto(self):
        result = SerdesUtils.write_function_arg(arg=self._naive_func_expression_dto)
        expected_result = {
            SerdesUtils.EXPRESSION_TYPE: self._naive_func_expression_dto.arg_type().name.lower(),
            SerdesUtils.FUNCTION_NAME: self._naive_func_expression_dto.function_name(),
            SerdesUtils.FUNCTION_ARGS: [
                SerdesUtils.write_function_arg(arg=self._literal_dto),
            ],
        }
        self.assertDictEqual(result, expected_result)

        result = SerdesUtils.write_function_arg(arg=self._func_expression_dto)
        expected_result = {
            SerdesUtils.EXPRESSION_TYPE: self._func_expression_dto.arg_type().name.lower(),
            SerdesUtils.FUNCTION_NAME: self._func_expression_dto.function_name(),
            SerdesUtils.FUNCTION_ARGS: [
                SerdesUtils.write_function_arg(arg=self._literal_dto),
                SerdesUtils.write_function_arg(arg=self._field_reference_dto),
                SerdesUtils.write_function_arg(arg=self._naive_func_expression_dto),
            ],
        }
        self.assertDictEqual(result, expected_result)

    def test_write_function_arg_unparsed_expression_dto(self):
        result = SerdesUtils.write_function_arg(arg=self._unparsed_expression_dto)
        expected_result = {
            SerdesUtils.EXPRESSION_TYPE: self._unparsed_expression_dto.arg_type().name.lower(),
            SerdesUtils.UNPARSED_EXPRESSION: self._unparsed_expression_dto.unparsed_expression(),
        }
        self.assertDictEqual(result, expected_result)

    def test_read_function_arg_invalid_data(self):
        self.assertRaisesRegex(
            IllegalArgumentException,
            "Cannot parse function arg from invalid JSON",
            SerdesUtils.read_function_arg,
            data=None,
        )

        self.assertRaisesRegex(
            IllegalArgumentException,
            "Cannot parse function arg from invalid JSON",
            SerdesUtils.read_function_arg,
            data="invalid_data",
        )

        self.assertRaisesRegex(
            IllegalArgumentException,
            "Cannot parse function arg from missing type",
            SerdesUtils.read_function_arg,
            data={},
        )

    def test_read_function_arg_literal_dto(self):
        data = {SerdesUtils.EXPRESSION_TYPE: self._literal_dto.arg_type().name.lower()}
        self.assertRaisesRegex(
            IllegalArgumentException,
            "Cannot parse literal arg from missing data type",
            SerdesUtils.read_function_arg,
            data=data,
        )

        data[SerdesUtils.DATA_TYPE] = self._literal_dto.data_type().simple_string()
        self.assertRaisesRegex(
            IllegalArgumentException,
            "Cannot parse literal arg from missing literal value",
            SerdesUtils.read_function_arg,
            data=data,
        )

        data[SerdesUtils.LITERAL_VALUE] = self._literal_dto.value()
        result = SerdesUtils.read_function_arg(data=data)
        self.assertEqual(result, self._literal_dto)

    def test_read_function_arg_field_reference_dto(self):
        data = {
            SerdesUtils.EXPRESSION_TYPE: self._field_reference_dto.arg_type().name.lower()
        }
        self.assertRaisesRegex(
            IllegalArgumentException,
            "Cannot parse field reference arg from missing field name",
            SerdesUtils.read_function_arg,
            data=data,
        )

        data[SerdesUtils.FIELD_NAME] = self._field_reference_dto.field_name()
        result = SerdesUtils.read_function_arg(data=data)
        self.assertEqual(result, self._field_reference_dto)

    def test_read_function_arg_func_expression_dto(self):
        data = {
            SerdesUtils.EXPRESSION_TYPE: self._naive_func_expression_dto.arg_type().name.lower()
        }
        self.assertRaisesRegex(
            IllegalArgumentException,
            "Cannot parse function function arg from missing function name",
            SerdesUtils.read_function_arg,
            data=data,
        )

        data[SerdesUtils.FUNCTION_NAME] = (
            self._naive_func_expression_dto.function_name()
        )
        self.assertRaisesRegex(
            IllegalArgumentException,
            "Cannot parse function function arg from missing function args",
            SerdesUtils.read_function_arg,
            data=data,
        )

        data[SerdesUtils.FUNCTION_ARGS] = [
            SerdesUtils.write_function_arg(arg=self._literal_dto),
        ]
        result = SerdesUtils.read_function_arg(data=data)
        self.assertEqual(result, self._naive_func_expression_dto)

        data[SerdesUtils.FUNCTION_ARGS] = []
        result = SerdesUtils.read_function_arg(data=data)
        self.assertEqual(
            result,
            FuncExpressionDTO.builder()
            .with_function_name(
                function_name=self._naive_func_expression_dto.function_name()
            )
            .with_function_args(function_args=FunctionArg.EMPTY_ARGS)
            .build(),
        )

    def test_read_function_arg_unparsed_expression_dto(self):
        data = {SerdesUtils.EXPRESSION_TYPE: "invalid_expression_type"}
        self.assertRaisesRegex(
            IllegalArgumentException,
            "Unknown function argument type",
            SerdesUtils.read_function_arg,
            data=data,
        )

        data[SerdesUtils.EXPRESSION_TYPE] = (
            self._unparsed_expression_dto.arg_type().name.lower()
        )
        data[SerdesUtils.UNPARSED_EXPRESSION] = {}
        self.assertRaisesRegex(
            IllegalArgumentException,
            "Cannot parse unparsed expression from missing string field unparsedExpression",
            SerdesUtils.read_function_arg,
            data=data,
        )

        data[SerdesUtils.UNPARSED_EXPRESSION] = (
            self._unparsed_expression_dto.unparsed_expression()
        )
        result = SerdesUtils.read_function_arg(data=data)
        self.assertEqual(result, self._unparsed_expression_dto)
