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

import json
import unittest
from enum import Enum
from unittest.mock import patch

from gravitino.api.types.types import Types
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO
from gravitino.dto.rel.partitioning.bucket_partitioning_dto import BucketPartitioningDTO
from gravitino.dto.rel.partitioning.day_partitioning_dto import DayPartitioningDTO
from gravitino.dto.rel.partitioning.function_partitioning_dto import (
    FunctionPartitioningDTO,
)
from gravitino.dto.rel.partitioning.hour_partitioning_dto import HourPartitioningDTO
from gravitino.dto.rel.partitioning.identity_partitioning_dto import (
    IdentityPartitioningDTO,
)
from gravitino.dto.rel.partitioning.json_serdes.partitioning_serdes import (
    PartitioningSerdes,
)
from gravitino.dto.rel.partitioning.list_partitioning_dto import ListPartitioningDTO
from gravitino.dto.rel.partitioning.month_partitioning_dto import MonthPartitioningDTO
from gravitino.dto.rel.partitioning.partitioning import Partitioning
from gravitino.dto.rel.partitioning.range_partitioning_dto import RangePartitioningDTO
from gravitino.dto.rel.partitioning.truncate_partitioning_dto import (
    TruncatePartitioningDTO,
)
from gravitino.dto.rel.partitioning.year_partitioning_dto import YearPartitioningDTO
from gravitino.exceptions.base import IllegalArgumentException


class MockPartitionStrategy(str, Enum):
    INVALID_STRATEGY = "invalid_partitioning_strategy"


class TestPartitioningSerdes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.field_name = [f"dummy_field_{i}" for i in range(1)]
        cls.literals = {
            PartitioningSerdes.RANGE_PARTITION_LOWER: LiteralDTO.builder()
            .with_data_type(Types.IntegerType.get())
            .with_value("0")
            .build(),
            PartitioningSerdes.RANGE_PARTITION_UPPER: LiteralDTO.builder()
            .with_data_type(Types.IntegerType.get())
            .with_value("100")
            .build(),
        }
        cls.single_field_partitioning_dtos = {
            Partitioning.Strategy.IDENTITY: IdentityPartitioningDTO(*cls.field_name),
            Partitioning.Strategy.YEAR: YearPartitioningDTO(*cls.field_name),
            Partitioning.Strategy.MONTH: MonthPartitioningDTO(*cls.field_name),
            Partitioning.Strategy.DAY: DayPartitioningDTO(*cls.field_name),
            Partitioning.Strategy.HOUR: HourPartitioningDTO(*cls.field_name),
        }
        cls.non_single_field_partitioning_dtos = {
            Partitioning.Strategy.BUCKET: BucketPartitioningDTO(10, cls.field_name),
            Partitioning.Strategy.FUNCTION: FunctionPartitioningDTO(
                "func_name",
                *cls.literals.values(),
            ),
            Partitioning.Strategy.LIST: ListPartitioningDTO([cls.field_name]),
            Partitioning.Strategy.RANGE: RangePartitioningDTO(cls.field_name),
            Partitioning.Strategy.TRUNCATE: TruncatePartitioningDTO(10, cls.field_name),
        }

    def test_serialize_invalid_strategy(self):
        mock_dto = IdentityPartitioningDTO(*self.field_name)
        with patch.object(
            mock_dto, "strategy", return_value=MockPartitionStrategy.INVALID_STRATEGY
        ):
            self.assertRaisesRegex(
                IOError,
                "Unknown partitioning strategy",
                PartitioningSerdes.serialize,
                mock_dto,
            )

    def test_deserialize_invalid_json(self):
        invalid_partitioning_data = (None, "invalid_data", {})

        for invalid_data in invalid_partitioning_data:
            with self.assertRaisesRegex(
                IllegalArgumentException, "Cannot parse partitioning from invalid JSON"
            ):
                PartitioningSerdes.deserialize(invalid_data)

    def test_deserialize_invalid_strategy(self):
        """Tests missing strategy and unknown partitioning strategy."""

        invalid_data_base = {
            PartitioningSerdes.FIELD_NAME: self.field_name,
        }
        invalid_strategy_data = {
            PartitioningSerdes.STRATEGY: "invalid_strategy",
            **invalid_data_base,
        }

        with self.assertRaisesRegex(
            IllegalArgumentException,
            "Cannot parse partitioning from missing strategy",
        ):
            PartitioningSerdes.deserialize(invalid_data_base)

        with self.assertRaisesRegex(
            IOError,
            "Unknown partitioning strategy",
        ):
            PartitioningSerdes.deserialize(invalid_strategy_data)

    def test_serialize_single_field_partitioning_dto(self):
        for partitioning_dto in self.single_field_partitioning_dtos.values():
            serialized = PartitioningSerdes.serialize(partitioning_dto)
            self.assertEqual(
                partitioning_dto.name(), serialized[PartitioningSerdes.STRATEGY]
            )
            self.assertEqual(
                partitioning_dto.strategy().value,
                serialized[PartitioningSerdes.STRATEGY],
            )
            self.assertListEqual(
                partitioning_dto.field_name(),
                serialized[PartitioningSerdes.FIELD_NAME],
            )

    def test_deserialize_single_field_partitioning_dto(self):
        for partitioning_dto in self.single_field_partitioning_dtos.values():
            serialized = PartitioningSerdes.serialize(partitioning_dto)
            deserialized = PartitioningSerdes.deserialize(serialized)

            self.assertEqual(partitioning_dto.name(), deserialized.name())
            self.assertEqual(
                partitioning_dto.strategy().value, deserialized.strategy().value
            )
            self.assertListEqual(
                partitioning_dto.field_name(), deserialized.field_name()
            )

    def test_deserialize_single_field_partitioning_dto_from_json_string(self):
        for strategy, partitioning_dto in self.single_field_partitioning_dtos.items():
            json_string = f"""
            {{
                "{PartitioningSerdes.STRATEGY}": "{strategy.value}",
                "{PartitioningSerdes.FIELD_NAME}": {json.dumps(partitioning_dto.field_name())}
            }}
            """

            expected_serialized = json.loads(json_string)
            deserialized = PartitioningSerdes.deserialize(expected_serialized)

            self.assertEqual(partitioning_dto.name(), deserialized.name())
            self.assertEqual(
                partitioning_dto.strategy().value, deserialized.strategy().value
            )
            self.assertListEqual(
                partitioning_dto.field_name(), deserialized.field_name()
            )

    def test_serdes_bucket_partitioning_dto(self):
        field_names = [["score"]]
        json_string = f"""
        {{
            "{PartitioningSerdes.STRATEGY}": "{Partitioning.Strategy.BUCKET.value}",
            "{PartitioningSerdes.NUM_BUCKETS}": 10,
            "{PartitioningSerdes.FIELD_NAMES}": {json.dumps(field_names)}
        }}
        """

        expected_serialized = json.loads(json_string)
        deserialized = PartitioningSerdes.deserialize(expected_serialized)

        self.assertEqual(Partitioning.Strategy.BUCKET.name.lower(), deserialized.name())
        self.assertEqual(
            Partitioning.Strategy.BUCKET.value, deserialized.strategy().value
        )
        self.assertListEqual(field_names, deserialized.field_names())

        serialized = PartitioningSerdes.serialize(deserialized)
        self.assertDictEqual(expected_serialized, serialized)
