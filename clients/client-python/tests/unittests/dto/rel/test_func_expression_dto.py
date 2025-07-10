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

from gravitino.api.types.types import Types
from gravitino.dto.rel.expressions.field_reference_dto import FieldReferenceDTO
from gravitino.dto.rel.expressions.func_expression_dto import FuncExpressionDTO
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO


class TestFuncExpressionDTO(unittest.TestCase):
    def setUp(self) -> None:
        self._func_args = [
            LiteralDTO.builder()
            .with_value(value="year")
            .with_data_type(data_type=Types.StringType.get())
            .build(),
            FieldReferenceDTO.builder()
            .with_field_name(field_name=["birthday"])
            .build(),
        ]
        self._func_expressions = [
            FuncExpressionDTO.builder()
            .with_function_name(function_name="function_without_args")
            .with_function_args(function_args=[])
            .build(),
            FuncExpressionDTO.builder()
            .with_function_name(function_name="function_with_args")
            .with_function_args(function_args=self._func_args)
            .build(),
        ]

    def test_func_expression_dto(self):
        dto = self._func_expressions[1]

        self.assertEqual(dto.function_name(), "function_with_args")
        self.assertListEqual(dto.args(), self._func_args)
        self.assertListEqual(dto.arguments(), self._func_args)
        self.assertIs(dto.arg_type(), FuncExpressionDTO.ArgType.FUNCTION)

    def test_equality(self):
        dto = self._func_expressions[1]
        dto1 = (
            FuncExpressionDTO.builder()
            .with_function_name(function_name="function_with_args")
            .with_function_args(function_args=self._func_args)
            .build()
        )
        self.assertTrue(dto == dto1)
        self.assertFalse(dto == self._func_expressions[0])
        self.assertFalse(dto == self._func_args[0])

    def test_hash(self):
        dto_dict = {dto: idx for idx, dto in enumerate(self._func_expressions)}
        self.assertEqual(0, dto_dict.get(self._func_expressions[0]))
        self.assertNotEqual(0, dto_dict.get(self._func_expressions[1]))

    def test_builder(self):
        dto = (
            FuncExpressionDTO.builder()
            .with_function_name("function_name")
            .with_function_args(self._func_args)
            .build()
        )
        self.assertIsInstance(dto, FuncExpressionDTO)
        self.assertEqual(dto.function_name(), "function_name")
        self.assertListEqual(dto.args(), self._func_args)
