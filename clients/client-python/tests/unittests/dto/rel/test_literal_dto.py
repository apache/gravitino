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


class TestLiteralDTO(unittest.TestCase):
    def setUp(self):
        self._literal_dto = (
            LiteralDTO.builder()
            .with_data_type(data_type=Types.IntegerType.get())
            .with_value(value="-1")
            .build()
        )

    def test_literal_dto(self):
        self.assertEqual(self._literal_dto.value(), "-1")
        self.assertEqual(self._literal_dto.data_type(), Types.IntegerType.get())

    def test_literal_dto_to_string(self):
        expected_str = f"LiteralDTO(value='{self._literal_dto.value()}', data_type={self._literal_dto.data_type()})"
        self.assertEqual(str(self._literal_dto), expected_str)

    def test_literal_dto_null(self):
        self.assertEqual(
            LiteralDTO.NULL,
            LiteralDTO.builder()
            .with_data_type(data_type=Types.NullType.get())
            .with_value(value="NULL")
            .build(),
        )

    def test_literal_dto_hash(self):
        second_literal_dto: LiteralDTO = (
            LiteralDTO.builder()
            .with_data_type(data_type=Types.IntegerType.get())
            .with_value(value="2")
            .build()
        )
        literal_dto_dict = {self._literal_dto: "test1", second_literal_dto: "test2"}

        self.assertEqual("test1", literal_dto_dict.get(self._literal_dto))
        self.assertNotEqual("test2", literal_dto_dict.get(self._literal_dto))

    def test_builder(self):
        dto = (
            LiteralDTO.Builder()
            .with_value("-1")
            .with_data_type(Types.IntegerType.get())
            .build()
        )
        self.assertIsInstance(dto, LiteralDTO)
        self.assertTrue(dto == self._literal_dto)

    def test_literal_dto_equality(self):
        self.assertEqual(self._literal_dto, self._literal_dto)
        self.assertNotEqual(self._literal_dto, "test")
