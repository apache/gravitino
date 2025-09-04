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
from gravitino.dto.rel.expressions.function_arg import FunctionArg
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO


class TestFieldReferenceDTO(unittest.TestCase):
    def setUp(self):
        self._field_references = [
            FieldReferenceDTO.builder()
            .with_field_name(field_name=[f"field_name_{idx}"])
            .build()
            for idx in range(3)
        ]

    def test_field_reference_dto(self):
        dto = (
            FieldReferenceDTO.builder()
            .with_field_name(field_name=self._field_references[0].field_name())
            .build()
        )
        self.assertListEqual(dto.field_name(), self._field_references[0].field_name())
        self.assertIs(dto.arg_type(), FunctionArg.ArgType.FIELD)

    def test_equality(self):
        dto = (
            FieldReferenceDTO.builder()
            .with_field_name(field_name=self._field_references[0].field_name())
            .build()
        )

        self.assertTrue(dto == self._field_references[0])
        self.assertFalse(dto == self._field_references[1])
        self.assertFalse(
            dto == LiteralDTO(value="value", data_type=Types.StringType.get())
        )

    def test_hash(self):
        dto_dict = {dto: idx for idx, dto in enumerate(self._field_references)}
        self.assertEqual(0, dto_dict.get(self._field_references[0]))
        self.assertNotEqual(0, dto_dict.get(self._field_references[1]))

    def test_builder(self):
        dto = FieldReferenceDTO.builder().with_field_name(["field_name"]).build()
        self.assertIsInstance(dto, FieldReferenceDTO)
        self.assertEqual(dto.field_name(), ["field_name"])

        dto = FieldReferenceDTO.builder().with_column_name("field_name").build()
        self.assertIsInstance(dto, FieldReferenceDTO)
        self.assertEqual(dto.field_name(), ["field_name"])
