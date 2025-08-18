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

import random
import unittest
from itertools import chain

from gravitino.api.expressions.literals.literals import Literals
from gravitino.api.expressions.named_reference import NamedReference
from gravitino.api.types.types import Types
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO
from gravitino.dto.rel.partitioning.bucket_partitioning_dto import BucketPartitioningDTO
from gravitino.dto.rel.partitioning.function_partitioning_dto import (
    FunctionPartitioningDTO,
)
from gravitino.dto.rel.partitioning.list_partitioning_dto import ListPartitioningDTO
from gravitino.dto.rel.partitioning.partitioning import Partitioning
from gravitino.dto.rel.partitioning.range_partitioning_dto import RangePartitioningDTO
from gravitino.dto.rel.partitioning.truncate_partitioning_dto import (
    TruncatePartitioningDTO,
)
from gravitino.dto.rel.partitions.list_partition_dto import ListPartitionDTO
from gravitino.dto.rel.partitions.range_partition_dto import RangePartitionDTO
from gravitino.exceptions.base import IllegalArgumentException


class TestBucketPartitioningDTO(unittest.TestCase):
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
        cls.dto = BucketPartitioningDTO(10, ["field1"], ["field2"])

    def test_bucket_partitioning_dto(self):
        self.dto.validate(self.columns)
        self.assertEqual(10, self.dto.num_buckets())
        self.assertEqual([["field1"], ["field2"]], self.dto.field_names())
        self.assertEqual(Partitioning.Strategy.BUCKET, self.dto.strategy())
        self.assertEqual("bucket", self.dto.name())

    def test_validate_non_existing_field(self):
        dto = BucketPartitioningDTO(6, ["nonexistent"])
        with self.assertRaises(IllegalArgumentException):
            dto.validate(self.columns)

    def test_arguments(self):
        arguments = self.dto.arguments()
        self.assertIsNotNone(arguments)
        self.assertIsInstance(arguments, list)
        self.assertEqual(
            arguments,
            [
                Literals.integer_literal(10),
                NamedReference.field(field_name=["field1"]),
                NamedReference.field(field_name=["field2"]),
            ],
        )


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


class TestListPartitioningDTO(unittest.TestCase):
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


class TestRangePartitioningDTO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.field_names = [["field1", "field2"]]
        cls.columns = [
            ColumnDTO.builder()
            .with_name(field_name)
            .with_data_type(Types.StringType.get())
            .with_comment(f"test {field_name}")
            .build()
            for field_name in chain.from_iterable(cls.field_names)
        ]
        cls.literal_dtos = [
            LiteralDTO.builder()
            .with_data_type(Types.IntegerType.get())
            .with_value(value=str(random_int))
            .build()
            for random_int in (random.randint(1, 100), random.randint(100, 200))
        ]

    def test_init_with_assignments(self):
        assignments = [
            RangePartitionDTO(
                name="range_partition",
                lower=self.literal_dtos[0],
                upper=self.literal_dtos[1],
                properties={},
            )
        ]
        dto = RangePartitioningDTO(
            field_name=self.field_names[0], assignments=assignments
        )

        dto.validate(self.columns)
        self.assertEqual(self.field_names[0], dto.field_name())
        self.assertEqual(assignments, dto.assignments())
        self.assertEqual(Partitioning.Strategy.RANGE, dto.strategy())
        self.assertEqual("range", dto.name())

        arguments = dto.arguments()
        self.assertEqual(len(self.field_names), len(arguments))
        for arg in arguments:
            self.assertIsInstance(arg, NamedReference)

    def test_init_without_assignments(self):
        dto = RangePartitioningDTO(self.field_names[0])

        self.assertEqual(self.field_names[0], dto.field_name())
        self.assertEqual([], dto.assignments())

    def test_empty_field_names(self):
        dto = RangePartitioningDTO([])
        self.assertEqual([], dto.field_name())
        self.assertEqual([NamedReference.field(field_name=[])], dto.arguments())

    def test_validate_non_existing_field(self):
        dto = RangePartitioningDTO(field_name=["nonexistent"])
        with self.assertRaises(IllegalArgumentException):
            dto.validate(self.columns)


class TestTruncatePartitioningDTO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.field_names = [["field1", "field2"]]
        cls.columns = [
            ColumnDTO.builder()
            .with_name(field_name)
            .with_data_type(Types.StringType.get())
            .with_comment(f"test {field_name}")
            .build()
            for field_name in chain.from_iterable(cls.field_names)
        ]
        cls.literal_dtos = [
            LiteralDTO.builder()
            .with_data_type(Types.IntegerType.get())
            .with_value(value=str(random_int))
            .build()
            for random_int in (random.randint(1, 100), random.randint(100, 200))
        ]

    def test_init(self):
        dto = TruncatePartitioningDTO(field_name=self.field_names[0], width=20)

        dto.validate(self.columns)
        self.assertEqual(self.field_names[0], dto.field_name())
        self.assertEqual(20, dto.width())
        self.assertEqual(Partitioning.Strategy.TRUNCATE, dto.strategy())
        self.assertEqual("truncate", dto.name())

        arguments = dto.arguments()
        expected_arguments = [
            Literals.integer_literal(value=dto.width()),
            NamedReference.field(field_name=dto.field_name()),
        ]
        self.assertListEqual(arguments, expected_arguments)

    def test_empty_field_names(self):
        dto = TruncatePartitioningDTO(field_name=[], width=20)
        self.assertEqual([], dto.field_name())
        expected_arguments = [
            Literals.integer_literal(value=dto.width()),
            NamedReference.field(field_name=dto.field_name()),
        ]
        self.assertListEqual(dto.arguments(), expected_arguments)

    def test_validate_non_existing_field(self):
        dto = TruncatePartitioningDTO(field_name=["nonexistent"], width=20)
        with self.assertRaises(IllegalArgumentException):
            dto.validate(self.columns)
