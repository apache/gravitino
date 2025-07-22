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
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO
from gravitino.dto.rel.expressions.unparsed_expression_dto import UnparsedExpressionDTO


class TestUnparsedExpressionDTO(unittest.TestCase):
    def setUp(self) -> None:
        self._dtos = [
            UnparsedExpressionDTO.builder()
            .with_unparsed_expression(unparsed_expression=f"unparsed_expression_{idx}")
            .build()
            for idx in range(3)
        ]

    def test_unparsed_expression_dto(self):
        dto = self._dtos[0]

        self.assertIs(dto.arg_type(), UnparsedExpressionDTO.ArgType.UNPARSED)
        self.assertEqual(dto.unparsed_expression(), "unparsed_expression_0")
        self.assertEqual(
            str(dto),
            "UnparsedExpressionDTO{unparsedExpression='unparsed_expression_0'}",
        )

    def test_equality(self):
        dto = self._dtos[0]
        similar_dto = (
            UnparsedExpressionDTO.builder()
            .with_unparsed_expression(unparsed_expression="unparsed_expression_0")
            .build()
        )
        another_dto = (
            UnparsedExpressionDTO.builder()
            .with_unparsed_expression(unparsed_expression="another_unparsed_expression")
            .build()
        )

        self.assertTrue(dto == similar_dto)
        self.assertFalse(dto == another_dto)
        self.assertFalse(
            dto == LiteralDTO(value="value", data_type=Types.StringType.get())
        )

    def test_hash(self):
        dto_dict = {dto: idx for idx, dto in enumerate(self._dtos)}

        self.assertEqual(0, dto_dict.get(self._dtos[0]))
        self.assertNotEqual(0, dto_dict.get(self._dtos[1]))
