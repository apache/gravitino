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

from itertools import chain
from unittest import TestCase

from gravitino.api.expressions.named_reference import NamedReference
from gravitino.api.types.types import Types
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO
from gravitino.dto.rel.partitioning.list_partitioning_dto import ListPartitioningDTO
from gravitino.dto.rel.partitioning.partitioning import Partitioning
from gravitino.dto.rel.partitions.list_partition_dto import ListPartitionDTO
from gravitino.exceptions.base import IllegalArgumentException


class TestListPartitioningDTO(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.field_names = [["field1"], ["field2"]]
        cls.columns = [
            ColumnDTO.builder()
            .with_name(field_name)
            .with_data_type(Types.StringType.get())
            .with_comment(f"test {field_name}")
            .build()
            for field_name in chain.from_iterable(cls.field_names)
        ]
        cls.literal_dtos = [
            [
                LiteralDTO.builder()
                .with_data_type(Types.StringType.get())
                .with_value(value=field_name)
                .build()
            ]
            for field_name in chain.from_iterable(cls.field_names)
        ]

    def test_init_with_assignments(self):
        assignments = [
            ListPartitionDTO(
                name="list_partition", lists=self.literal_dtos, properties={}
            )
        ]
        dto = ListPartitioningDTO(self.field_names, assignments)

        dto.validate(self.columns)
        self.assertEqual(self.field_names, dto.field_names())
        self.assertEqual(assignments, dto.assignments())
        self.assertEqual(Partitioning.Strategy.LIST, dto.strategy())
        self.assertEqual("list", dto.name())

        arguments = dto.arguments()
        self.assertEqual(len(self.field_names), len(arguments))
        for arg in arguments:
            self.assertIsInstance(arg, NamedReference)

    def test_init_without_assignments(self):
        dto = ListPartitioningDTO(self.field_names, None)

        self.assertEqual(self.field_names, dto.field_names())
        self.assertEqual([], dto.assignments())

    def test_empty_field_names(self):
        dto = ListPartitioningDTO([], None)
        self.assertEqual([], dto.field_names())
        self.assertEqual([], dto.arguments())

    def test_validate_non_existing_field(self):
        dto = ListPartitioningDTO([["nonexistent"]])
        with self.assertRaises(IllegalArgumentException):
            dto.validate(self.columns)
