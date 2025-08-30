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
from gravitino.dto.rel.expressions.field_reference_dto import FieldReferenceDTO
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
        cls.field_names = [["createTime"], ["city"]]
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

    def test_serdes_truncate_partitioning_dto(self):
        field_name = ["score"]
        json_string = f"""
        {{
            "{PartitioningSerdes.STRATEGY}": "{Partitioning.Strategy.TRUNCATE.value}",
            "{PartitioningSerdes.WIDTH}": 20,
            "{PartitioningSerdes.FIELD_NAME}": {json.dumps(field_name)}
        }}
        """

        expected_serialized = json.loads(json_string)
        deserialized = PartitioningSerdes.deserialize(expected_serialized)

        self.assertEqual(
            Partitioning.Strategy.TRUNCATE.name.lower(), deserialized.name()
        )
        self.assertEqual(
            Partitioning.Strategy.TRUNCATE.value, deserialized.strategy().value
        )
        self.assertListEqual(field_name, deserialized.field_name())

        serialized = PartitioningSerdes.serialize(deserialized)
        self.assertDictEqual(expected_serialized, serialized)

    def test_serdes_list_partitioning_dto_invalid_assignments(self):
        json_string = f"""
        {{
            "{PartitioningSerdes.STRATEGY}": "{Partitioning.Strategy.LIST.value}",
            "{PartitioningSerdes.FIELD_NAMES}": {json.dumps(TestPartitioningSerdes.field_names)},
            "{PartitioningSerdes.ASSIGNMENTS_NAME}": "invalid_assignments"
        }}
        """
        with self.assertRaisesRegex(
            IllegalArgumentException,
            "Cannot parse list partitioning from non-array assignments",
        ):
            PartitioningSerdes.deserialize(json.loads(json_string))

    def test_serdes_list_partitioning_dto_invalid_list_assignment(self):
        json_string = f"""
        {{
            "{PartitioningSerdes.STRATEGY}": "{Partitioning.Strategy.LIST.value}",
            "{PartitioningSerdes.FIELD_NAMES}": {json.dumps(TestPartitioningSerdes.field_names)},
            "{PartitioningSerdes.ASSIGNMENTS_NAME}": [
                {{
                    "{PartitioningSerdes.PARTITION_TYPE}": "range",
                    "{PartitioningSerdes.PARTITION_NAME}": "p20200321",
                    "{PartitioningSerdes.RANGE_PARTITION_UPPER}": {{
                        "{PartitioningSerdes.TYPE}": "literal",
                        "{PartitioningSerdes.DATA_TYPE}": "date",
                        "{PartitioningSerdes.LITERAL_VALUE}": "2020-03-21"
                    }},
                    "{PartitioningSerdes.RANGE_PARTITION_LOWER}": {{
                        "{PartitioningSerdes.TYPE}": "literal",
                        "{PartitioningSerdes.DATA_TYPE}": "null",
                        "{PartitioningSerdes.LITERAL_VALUE}": "null"
                    }}
                }}
            ]
        }}
        """
        with self.assertRaisesRegex(
            IllegalArgumentException,
            "Cannot parse list partitioning from non-list assignment",
        ):
            PartitioningSerdes.deserialize(json.loads(json_string))

    def test_serdes_list_partitioning_dto(self):
        json_string = f"""
        {{
            "{PartitioningSerdes.STRATEGY}": "{Partitioning.Strategy.LIST.value}",
            "{PartitioningSerdes.FIELD_NAMES}": {json.dumps(TestPartitioningSerdes.field_names)}
        }}
        """

        expected_serialized = json.loads(json_string)
        deserialized = PartitioningSerdes.deserialize(expected_serialized)

        self.assertEqual(Partitioning.Strategy.LIST.name.lower(), deserialized.name())
        self.assertEqual(
            Partitioning.Strategy.LIST.value, deserialized.strategy().value
        )
        self.assertListEqual(
            TestPartitioningSerdes.field_names, deserialized.field_names()
        )

        serialized = PartitioningSerdes.serialize(deserialized)
        self.assertEqual(
            expected_serialized[PartitioningSerdes.STRATEGY],
            serialized[PartitioningSerdes.STRATEGY],
        )
        self.assertListEqual(
            expected_serialized[PartitioningSerdes.FIELD_NAMES],
            serialized[PartitioningSerdes.FIELD_NAMES],
        )
        self.assertEqual(
            [],
            serialized[PartitioningSerdes.ASSIGNMENTS_NAME],
        )

        json_string = f"""
        {{
            "{PartitioningSerdes.STRATEGY}": "{Partitioning.Strategy.LIST.value}",
            "{PartitioningSerdes.FIELD_NAMES}": {json.dumps(TestPartitioningSerdes.field_names)},
            "{PartitioningSerdes.ASSIGNMENTS_NAME}": [
                {{
                    "{PartitioningSerdes.PARTITION_TYPE}": "list",
                    "{PartitioningSerdes.PARTITION_NAME}": "p202204_California",
                    "{PartitioningSerdes.PARTITION_PROPERTIES}": {{}},
                    "{PartitioningSerdes.LIST_PARTITION_LISTS}": [
                        [
                            {{
                                "{PartitioningSerdes.TYPE}": "literal",
                                "{PartitioningSerdes.DATA_TYPE}": "date",
                                "{PartitioningSerdes.LITERAL_VALUE}": "2022-04-01"
                            }},
                            {{
                                "{PartitioningSerdes.TYPE}": "literal",
                                "{PartitioningSerdes.DATA_TYPE}": "string",
                                "{PartitioningSerdes.LITERAL_VALUE}": "Los Angeles"
                            }}
                        ],
                        [
                            {{
                                "{PartitioningSerdes.TYPE}": "literal",
                                "{PartitioningSerdes.DATA_TYPE}": "date",
                                "{PartitioningSerdes.LITERAL_VALUE}": "2022-04-01"
                            }},
                            {{
                                "{PartitioningSerdes.TYPE}": "literal",
                                "{PartitioningSerdes.DATA_TYPE}": "string",
                                "{PartitioningSerdes.LITERAL_VALUE}": "San Francisco"
                            }}
                        ]
                    ]
                }}
            ]
        }}
        """

        expected_serialized = json.loads(json_string)
        deserialized = PartitioningSerdes.deserialize(expected_serialized)
        serialized = PartitioningSerdes.serialize(deserialized)
        self.assertEqual(
            expected_serialized[PartitioningSerdes.STRATEGY],
            serialized[PartitioningSerdes.STRATEGY],
        )
        self.assertListEqual(
            expected_serialized[PartitioningSerdes.FIELD_NAMES],
            serialized[PartitioningSerdes.FIELD_NAMES],
        )
        self.assertEqual(
            expected_serialized[PartitioningSerdes.ASSIGNMENTS_NAME],
            serialized[PartitioningSerdes.ASSIGNMENTS_NAME],
        )

    def test_serdes_range_partitioning_dto_invalid_assignments(self):
        json_string = f"""
        {{
            "{PartitioningSerdes.STRATEGY}": "{Partitioning.Strategy.RANGE.value}",
            "{PartitioningSerdes.FIELD_NAME}": {json.dumps(TestPartitioningSerdes.field_name)},
            "{PartitioningSerdes.ASSIGNMENTS_NAME}": "invalid_assignments"
        }}
        """
        with self.assertRaisesRegex(
            IllegalArgumentException,
            "Cannot parse range partitioning from non-array assignments",
        ):
            PartitioningSerdes.deserialize(json.loads(json_string))

    def test_serdes_range_partitioning_dto_invalid_range_assignment(self):
        json_string = f"""
        {{
            "{PartitioningSerdes.STRATEGY}": "{Partitioning.Strategy.RANGE.value}",
            "{PartitioningSerdes.FIELD_NAME}": {json.dumps(TestPartitioningSerdes.field_name)},
            "{PartitioningSerdes.ASSIGNMENTS_NAME}": [
                {{
                    "{PartitioningSerdes.PARTITION_TYPE}": "list",
                    "{PartitioningSerdes.PARTITION_NAME}": "p202204_California",
                    "{PartitioningSerdes.PARTITION_PROPERTIES}": {{}},
                    "{PartitioningSerdes.LIST_PARTITION_LISTS}": [
                        [
                            {{
                                "{PartitioningSerdes.TYPE}": "literal",
                                "{PartitioningSerdes.DATA_TYPE}": "date",
                                "{PartitioningSerdes.LITERAL_VALUE}": "2022-04-01"
                            }},
                            {{
                                "{PartitioningSerdes.TYPE}": "literal",
                                "{PartitioningSerdes.DATA_TYPE}": "string",
                                "{PartitioningSerdes.LITERAL_VALUE}": "Los Angeles"
                            }}
                        ]
                    ]
                }}
            ]
        }}
        """
        with self.assertRaisesRegex(
            IllegalArgumentException,
            "Cannot parse range partitioning from non-range assignment",
        ):
            PartitioningSerdes.deserialize(json.loads(json_string))

    def test_serdes_range_partitioning_dto(self):
        json_string = f"""
        {{
            "{PartitioningSerdes.STRATEGY}": "{Partitioning.Strategy.RANGE.value}",
            "{PartitioningSerdes.FIELD_NAME}": {json.dumps(TestPartitioningSerdes.field_name)}
        }}
        """

        expected_serialized = json.loads(json_string)
        deserialized = PartitioningSerdes.deserialize(expected_serialized)

        self.assertEqual(Partitioning.Strategy.RANGE.name.lower(), deserialized.name())
        self.assertEqual(
            Partitioning.Strategy.RANGE.value, deserialized.strategy().value
        )
        self.assertListEqual(
            TestPartitioningSerdes.field_name, deserialized.field_name()
        )

        serialized = PartitioningSerdes.serialize(deserialized)
        self.assertEqual(
            expected_serialized[PartitioningSerdes.STRATEGY],
            serialized[PartitioningSerdes.STRATEGY],
        )
        self.assertListEqual(
            expected_serialized[PartitioningSerdes.FIELD_NAME],
            serialized[PartitioningSerdes.FIELD_NAME],
        )
        self.assertEqual(
            [],
            serialized[PartitioningSerdes.ASSIGNMENTS_NAME],
        )

        json_string = f"""
        {{
            "{PartitioningSerdes.STRATEGY}": "{Partitioning.Strategy.RANGE.value}",
            "{PartitioningSerdes.FIELD_NAME}": {json.dumps(TestPartitioningSerdes.field_name)},
            "{PartitioningSerdes.ASSIGNMENTS_NAME}": [
                {{
                    "{PartitioningSerdes.PARTITION_TYPE}": "range",
                    "{PartitioningSerdes.PARTITION_NAME}": "p20200321",
                    "{PartitioningSerdes.RANGE_PARTITION_UPPER}": {{
                        "{PartitioningSerdes.TYPE}": "literal",
                        "{PartitioningSerdes.DATA_TYPE}": "date",
                        "{PartitioningSerdes.LITERAL_VALUE}": "2020-03-21"
                    }},
                    "{PartitioningSerdes.RANGE_PARTITION_LOWER}": {{
                        "{PartitioningSerdes.TYPE}": "literal",
                        "{PartitioningSerdes.DATA_TYPE}": "null",
                        "{PartitioningSerdes.LITERAL_VALUE}": "null"
                    }},
                    "{PartitioningSerdes.PARTITION_PROPERTIES}": {{"key": "value"}}
                }}
            ]
        }}
        """

        expected_serialized = json.loads(json_string)
        deserialized = PartitioningSerdes.deserialize(expected_serialized)
        serialized = PartitioningSerdes.serialize(deserialized)
        self.assertEqual(
            expected_serialized[PartitioningSerdes.STRATEGY],
            serialized[PartitioningSerdes.STRATEGY],
        )
        self.assertListEqual(
            expected_serialized[PartitioningSerdes.FIELD_NAME],
            serialized[PartitioningSerdes.FIELD_NAME],
        )
        self.assertEqual(
            expected_serialized[PartitioningSerdes.ASSIGNMENTS_NAME],
            serialized[PartitioningSerdes.ASSIGNMENTS_NAME],
        )

    def test_serdes_function_partitioning_dto_invalid_args(self):
        json_string = f"""
        {{
            "{PartitioningSerdes.STRATEGY}": "{Partitioning.Strategy.FUNCTION.value}",
            "{PartitioningSerdes.FUNCTION_NAME}": "dummy_func_name"
        }}
        """

        with self.assertRaisesRegex(
            IllegalArgumentException,
            "Cannot parse function partitioning from missing function args",
        ):
            PartitioningSerdes.deserialize(json.loads(json_string))

    def test_serdes_function_partitioning_dto(self):
        field_arg = FieldReferenceDTO.builder().with_column_name("dt").build()
        literal_arg = (
            LiteralDTO.builder()
            .with_data_type(Types.StringType.get())
            .with_value("Asia/Shanghai")
            .build()
        )
        json_string = f"""
        {{
            "{PartitioningSerdes.STRATEGY}": "{Partitioning.Strategy.FUNCTION.value}",
            "{PartitioningSerdes.FUNCTION_NAME}": "to_date",
            "{PartitioningSerdes.FUNCTION_ARGS}": [
                {{
                    "{PartitioningSerdes.TYPE}": "field",
                    "{PartitioningSerdes.FIELD_NAME}": ["dt"]
                }},
                {{
                    "{PartitioningSerdes.TYPE}": "literal",
                    "{PartitioningSerdes.DATA_TYPE}": "string",
                    "{PartitioningSerdes.LITERAL_VALUE}": "Asia/Shanghai"
                }}
            ]
        }}
        """

        expected_serialized = json.loads(json_string)
        deserialized = PartitioningSerdes.deserialize(expected_serialized)

        self.assertIsInstance(deserialized, FunctionPartitioningDTO)
        self.assertEqual("to_date", deserialized.function_name())
        self.assertEqual(deserialized.function_name(), deserialized.name())
        self.assertEqual(
            Partitioning.Strategy.FUNCTION.value, deserialized.strategy().value
        )
        self.assertListEqual([field_arg, literal_arg], deserialized.args())

        serialized = PartitioningSerdes.serialize(deserialized)
        self.assertEqual(
            expected_serialized[PartitioningSerdes.STRATEGY],
            serialized[PartitioningSerdes.STRATEGY],
        )
        self.assertEqual(
            expected_serialized[PartitioningSerdes.FUNCTION_NAME],
            serialized[PartitioningSerdes.FUNCTION_NAME],
        )
        self.assertListEqual(
            expected_serialized[PartitioningSerdes.FUNCTION_ARGS],
            serialized[PartitioningSerdes.FUNCTION_ARGS],
        )
