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
from gravitino.dto.rel.partitions.identity_partition_dto import IdentityPartitionDTO
from gravitino.dto.rel.partitions.list_partition_dto import ListPartitionDTO
from gravitino.dto.rel.partitions.partition_dto import PartitionDTO
from gravitino.dto.rel.partitions.range_partition_dto import RangePartitionDTO


class TestPartitionDTOs(unittest.TestCase):
    def test_identity_partition_dto(self):
        partition_name = "dt=2025-08-08/country=us"
        field_names = [["dt"], ["country"]]
        properties = {}
        values = [
            LiteralDTO.builder()
            .with_data_type(data_type=Types.DateType.get())
            .with_value(value="2025-08-08")
            .build(),
            LiteralDTO.builder()
            .with_data_type(data_type=Types.StringType.get())
            .with_value(value="us")
            .build(),
        ]
        dto = IdentityPartitionDTO(
            name=partition_name,
            field_names=field_names,
            values=values,
            properties=properties,
        )

        similar_dto = IdentityPartitionDTO(
            name=partition_name,
            field_names=field_names,
            values=values,
            properties=properties,
        )

        different_dto = IdentityPartitionDTO(
            name="different_partition",
            field_names=field_names,
            values=values,
            properties={},
        )

        dtos = {dto: 1, similar_dto: 2, different_dto: 3}

        self.assertIsInstance(dto, IdentityPartitionDTO)
        self.assertIs(dto.type(), PartitionDTO.Type.IDENTITY)
        self.assertEqual(dto.name(), partition_name)
        self.assertListEqual(dto.field_names(), field_names)
        self.assertListEqual(dto.values(), values)
        self.assertDictEqual(dto.properties(), properties)

        self.assertTrue(dto == similar_dto)
        self.assertFalse(dto == different_dto)
        self.assertFalse(dto == "dummy_string")

        self.assertEqual(len(dtos), 2)
        self.assertEqual(dtos[dto], 2)

    def test_list_partition_dto(self):
        partition_name = "p202508_California"
        properties = {}
        lists = [
            [
                LiteralDTO.builder()
                .with_data_type(data_type=Types.DateType.get())
                .with_value(value="2025-08-08")
                .build(),
                LiteralDTO.builder()
                .with_data_type(data_type=Types.StringType.get())
                .with_value(value="Los Angeles")
                .build(),
            ],
            [
                LiteralDTO.builder()
                .with_data_type(data_type=Types.DateType.get())
                .with_value(value="2025-08-08")
                .build(),
                LiteralDTO.builder()
                .with_data_type(data_type=Types.StringType.get())
                .with_value(value="San Francisco")
                .build(),
            ],
        ]
        dto = ListPartitionDTO(
            name=partition_name,
            lists=lists,
            properties=properties,
        )

        similar_dto = ListPartitionDTO(
            name=partition_name,
            lists=lists,
            properties=properties,
        )

        different_dto = ListPartitionDTO(
            name="different_partition",
            lists=lists,
            properties=properties,
        )

        dtos = {dto: 1, similar_dto: 2, different_dto: 3}

        self.assertIsInstance(dto, ListPartitionDTO)
        self.assertIs(dto.type(), PartitionDTO.Type.LIST)
        self.assertEqual(dto.name(), partition_name)
        self.assertListEqual(dto.lists(), lists)
        self.assertDictEqual(dto.properties(), properties)

        self.assertTrue(dto == similar_dto)
        self.assertFalse(dto == different_dto)
        self.assertFalse(dto == "dummy_string")

        self.assertEqual(len(dtos), 2)
        self.assertEqual(dtos[dto], 2)

    def test_range_partition_dto(self):
        partition_name = "p20250808"
        properties = {}
        upper = (
            LiteralDTO.builder()
            .with_data_type(data_type=Types.DateType.get())
            .with_value(value="2025-08-08")
            .build()
        )
        lower = (
            LiteralDTO.builder()
            .with_data_type(data_type=Types.NullType.get())
            .with_value(value="null")
            .build()
        )
        dto = RangePartitionDTO(
            name=partition_name,
            properties=properties,
            upper=upper,
            lower=lower,
        )

        similar_dto = RangePartitionDTO(
            name=partition_name,
            properties=properties,
            upper=upper,
            lower=lower,
        )

        different_dto = RangePartitionDTO(
            name="different_partition",
            properties=properties,
            upper=upper,
            lower=lower,
        )

        dtos = {dto: 1, similar_dto: 2, different_dto: 3}

        self.assertIsInstance(dto, RangePartitionDTO)
        self.assertIs(dto.type(), PartitionDTO.Type.RANGE)
        self.assertEqual(dto.name(), partition_name)
        self.assertEqual(dto.upper(), upper)
        self.assertEqual(dto.lower(), lower)
        self.assertDictEqual(dto.properties(), properties)

        self.assertTrue(dto == similar_dto)
        self.assertFalse(dto == different_dto)
        self.assertFalse(dto == "dummy_string")

        self.assertEqual(len(dtos), 2)
        self.assertEqual(dtos[dto], 2)
