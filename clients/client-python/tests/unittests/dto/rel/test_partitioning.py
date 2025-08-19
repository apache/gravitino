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

from gravitino.api.expressions.named_reference import NamedReference
from gravitino.api.types.types import Types
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.partitioning.partitioning import (
    Partitioning,
    SingleFieldPartitioning,
)
from gravitino.exceptions.base import IllegalArgumentException


class MockPartitioningDTO(SingleFieldPartitioning):
    def strategy(self) -> Partitioning.Strategy:
        return Partitioning.Strategy.IDENTITY


class TestPartitioning(unittest.TestCase):
    def test_strategy_enum_values(self):
        self.assertEqual(Partitioning.Strategy.IDENTITY, "identity")
        self.assertEqual(Partitioning.Strategy.YEAR, "year")
        self.assertEqual(Partitioning.Strategy.MONTH, "month")
        self.assertEqual(Partitioning.Strategy.DAY, "day")
        self.assertEqual(Partitioning.Strategy.HOUR, "hour")
        self.assertEqual(Partitioning.Strategy.BUCKET, "bucket")
        self.assertEqual(Partitioning.Strategy.TRUNCATE, "truncate")
        self.assertEqual(Partitioning.Strategy.LIST, "list")
        self.assertEqual(Partitioning.Strategy.RANGE, "range")
        self.assertEqual(Partitioning.Strategy.FUNCTION, "function")

    def test_get_by_name_valid_strategies(self):
        self.assertEqual(
            Partitioning.get_by_name("identity"), Partitioning.Strategy.IDENTITY
        )
        self.assertEqual(Partitioning.get_by_name("YEAR"), Partitioning.Strategy.YEAR)
        self.assertEqual(Partitioning.get_by_name("Month"), Partitioning.Strategy.MONTH)

    def test_get_by_name_invalid_strategy(self):
        invalid_strategy_name = "invalid"
        with self.assertRaisesRegex(
            IllegalArgumentException,
            f"Invalid partitioning strategy: {invalid_strategy_name}",
        ):
            Partitioning.get_by_name(invalid_strategy_name)

    def test_empty_partitioning_constant(self):
        self.assertEqual(len(Partitioning.EMPTY_PARTITIONING), 0)


class TestSingleFieldPartitioning(unittest.TestCase):
    def setUp(self):
        self.columns = [
            ColumnDTO.builder()
            .with_name("col1")
            .with_data_type(Types.StringType.get())
            .with_comment("test column")
            .build()
        ]

    def test_init_valid_field_name(self):
        partitioning = MockPartitioningDTO(["col1"])
        self.assertEqual(partitioning.field_name(), ["col1"])

    def test_init_invalid_field_name(self):
        with self.assertRaises(IllegalArgumentException):
            MockPartitioningDTO([])

        with self.assertRaises(IllegalArgumentException):
            MockPartitioningDTO(None)

    def test_validate_existing_field(self):
        partitioning = MockPartitioningDTO(["col1"])
        partitioning.validate(self.columns)

    def test_validate_non_existing_field(self):
        partitioning = MockPartitioningDTO(["nonexistent"])
        with self.assertRaises(IllegalArgumentException):
            partitioning.validate(self.columns)

    def test_arguments(self):
        partitioning = MockPartitioningDTO(["col1"])
        args = partitioning.arguments()
        self.assertEqual(len(args), 1)
        self.assertIsInstance(args[0], NamedReference)

    def test_strategy(self):
        partitioning = MockPartitioningDTO(["col1"])
        self.assertEqual(partitioning.strategy(), Partitioning.Strategy.IDENTITY)

    def test_name(self):
        partitioning = MockPartitioningDTO(["col1"])
        self.assertEqual(partitioning.name(), "identity")
