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

from gravitino.api.expressions.distributions.strategy import Strategy
from gravitino.api.types.types import Types
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.distribution_dto import DistributionDTO
from gravitino.dto.rel.expressions.field_reference_dto import FieldReferenceDTO
from gravitino.exceptions.base import IllegalArgumentException


class TestDistributionDTO(unittest.TestCase):
    def test_distribution_dto_no_distribution(self):
        no_distribution = DistributionDTO.NONE

        self.assertEqual(no_distribution.number(), 0)
        self.assertIs(no_distribution.strategy(), Strategy.NONE)
        self.assertListEqual(no_distribution.args(), [])

    def test_distribution_dto_illegal_init(self):
        self.assertRaisesRegex(
            IllegalArgumentException,
            "bucketNum must be greater than or equal -1",
            DistributionDTO,
            Strategy.HASH,
            -2,
            [],
        )
        self.assertRaisesRegex(
            IllegalArgumentException,
            "expressions cannot be null",
            DistributionDTO,
            Strategy.RANGE,
            1,
            None,
        )

    def test_distribution_dto_equal(self):
        args = [FieldReferenceDTO.builder().with_column_name("score").build()]
        dto = DistributionDTO(Strategy.RANGE, 4, [])
        self.assertEqual(dto, dto)
        self.assertFalse(dto == "invalid_dto")
        self.assertFalse(dto == DistributionDTO(Strategy.HASH, 4, []))
        self.assertFalse(dto == DistributionDTO(Strategy.RANGE, 5, []))
        self.assertFalse(
            dto
            == DistributionDTO(
                Strategy.RANGE,
                4,
                args,
            )
        )
        self.assertTrue(dto == DistributionDTO(Strategy.RANGE, 4, []))

    def test_distribution_dto_hash(self):
        dto1 = DistributionDTO(Strategy.RANGE, 4, [])
        dto2 = DistributionDTO(Strategy.RANGE, 5, [])
        dto_dict = {
            dto1: 1,
            dto2: 2,
        }
        dto_dict[dto1] = 3
        self.assertEqual(len(dto_dict), 2)
        self.assertEqual(dto_dict[dto1], 3)

    def test_distribution_dto_init(self):
        column = (
            ColumnDTO.builder()
            .with_name("dummy_col")
            .with_data_type(Types.StringType.get())
            .build()
        )
        args = [FieldReferenceDTO.builder().with_column_name("score").build()]

        dto = DistributionDTO(None, 0, [])
        self.assertEqual(dto.number(), 0)
        self.assertIs(dto.strategy(), Strategy.HASH)
        dto.validate([])

        dto = DistributionDTO(Strategy.RANGE, 4, args)
        self.assertEqual(dto.number(), 4)
        self.assertIs(dto.strategy(), Strategy.RANGE)
        self.assertListEqual(dto.args(), args)
        self.assertListEqual(dto.expressions(), args)
        with self.assertRaises(IllegalArgumentException):
            dto.validate(columns=[column])
