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

from gravitino.api.expressions.literals.literals import Literals
from gravitino.api.expressions.named_reference import NamedReference
from gravitino.api.types.types import Types
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.partitioning.function_partitioning_dto import (
    FunctionPartitioningDTO,
)
from gravitino.dto.rel.partitioning.partitioning import Partitioning
from gravitino.exceptions.base import IllegalArgumentException


class TestFunctionPartitioningDTO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.columns = [
            ColumnDTO.builder()
            .with_name("field1")
            .with_data_type(Types.StringType.get())
            .with_comment("test field 1")
            .build(),
            ColumnDTO.builder()
            .with_name("field2")
            .with_data_type(Types.IntegerType.get())
            .with_comment("test field 2")
            .build(),
        ]

    def test_function_partitioning_dto(self):
        field_ref = NamedReference.field(["field1"])
        literal = Literals.integer_literal(value=10)

        dto = FunctionPartitioningDTO("test_function", field_ref, literal)

        dto.validate(self.columns)
        self.assertEqual("test_function", dto.function_name())
        self.assertEqual("test_function", dto.name())
        self.assertEqual([field_ref, literal], dto.args())
        self.assertEqual([field_ref, literal], dto.arguments())
        self.assertEqual(Partitioning.Strategy.FUNCTION, dto.strategy())

    def test_function_partitioning_dto_no_args(self):
        dto = FunctionPartitioningDTO("simple_function")

        self.assertEqual("simple_function", dto.function_name())
        self.assertEqual([], dto.args())
        self.assertEqual([], dto.arguments())

    def test_validate_non_existing_field(self):
        field_ref = NamedReference.field(["nonexistent"])
        dto = FunctionPartitioningDTO("test_function", field_ref)

        with self.assertRaises(IllegalArgumentException):
            dto.validate(self.columns)
