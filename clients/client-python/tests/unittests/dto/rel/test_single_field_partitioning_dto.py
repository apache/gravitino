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
from gravitino.dto.rel.partitioning.day_partitioning_dto import DayPartitioningDTO
from gravitino.dto.rel.partitioning.hour_partitioning_dto import HourPartitioningDTO
from gravitino.dto.rel.partitioning.identity_partitioning_dto import (
    IdentityPartitioningDTO,
)
from gravitino.dto.rel.partitioning.month_partitioning_dto import MonthPartitioningDTO
from gravitino.dto.rel.partitioning.partitioning import Partitioning
from gravitino.dto.rel.partitioning.year_partitioning_dto import YearPartitioningDTO
from gravitino.exceptions.base import IllegalArgumentException


class TestSingleFieldPartitioningDTO(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.field_name = [f"dummy_field_{i}" for i in range(3)]
        cls.partitioning_dto = {
            Partitioning.Strategy.IDENTITY: IdentityPartitioningDTO,
            Partitioning.Strategy.YEAR: YearPartitioningDTO,
            Partitioning.Strategy.MONTH: MonthPartitioningDTO,
            Partitioning.Strategy.DAY: DayPartitioningDTO,
            Partitioning.Strategy.HOUR: HourPartitioningDTO,
        }

    def test_single_field_partitioning_dto_empty_field_name(self):
        for partition_dto in self.partitioning_dto.values():
            self.assertRaisesRegex(
                IllegalArgumentException,
                "field_name cannot be null or empty",
                partition_dto,
            )

    def test_single_field_partitioning_dto(self):
        arguments = [NamedReference.field(self.field_name)]
        column_dtos = [
            ColumnDTO.builder()
            .with_name(name=f"dummy_field_{i}")
            .with_data_type(Types.StringType.get())
            .build()
            for i in range(3)
        ]
        not_existing_column_dto = (
            ColumnDTO.builder()
            .with_name(name="not_exist_field")
            .with_data_type(Types.StringType.get())
            .build()
        )
        for strategy, dto_class in self.partitioning_dto.items():
            dto = dto_class(*self.field_name)
            dto.validate(columns=column_dtos)
            with self.assertRaisesRegex(
                IllegalArgumentException,
                "not found in table",
            ):
                dto.validate(columns=[not_existing_column_dto])
            self.assertEqual(strategy, dto.strategy())
            self.assertEqual(strategy.value, dto.name())
            self.assertListEqual(self.field_name, dto.field_name())
            self.assertListEqual(arguments, dto.arguments())
