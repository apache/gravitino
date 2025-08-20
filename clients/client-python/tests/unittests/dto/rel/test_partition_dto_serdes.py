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
from typing import cast
from unittest.mock import patch

from gravitino.api.types.types import Types
from gravitino.dto.rel.expressions.json_serdes._helper.serdes_utils import (
    SerdesUtils as ExpressionSerdesUtils,
)
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO
from gravitino.dto.rel.partitions.identity_partition_dto import IdentityPartitionDTO
from gravitino.dto.rel.partitions.json_serdes._helper.serdes_utils import SerdesUtils
from gravitino.dto.rel.partitions.json_serdes.partition_dto_serdes import (
    PartitionDTOSerdes,
)
from gravitino.dto.rel.partitions.list_partition_dto import ListPartitionDTO
from gravitino.dto.rel.partitions.partition_dto import PartitionDTO
from gravitino.dto.rel.partitions.range_partition_dto import RangePartitionDTO
from gravitino.exceptions.base import IllegalArgumentException


class MockPartitionDTOType(str, Enum):
    INVALID_PARTITION_TYPE = "invalid_partition_type"


class TestPartitionSerdesUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.literal_values = {
            "upper": LiteralDTO.builder()
            .with_data_type(Types.IntegerType.get())
            .with_value(value="0")
            .build(),
            "lower": LiteralDTO.builder()
            .with_data_type(Types.IntegerType.get())
            .with_value(value="100")
            .build(),
        }
        cls.field_names = [["upper"], ["lower"]]
        cls.properties = {"key1": "value1", "key2": "value2"}
        cls.identity_partition_dto = IdentityPartitionDTO(
            name="test_identity_partition",
            values=list(cls.literal_values.values()),
            field_names=cls.field_names,
            properties=cls.properties,
        )
        cls.range_partition_dto = RangePartitionDTO(
            name="test_range_partition",
            properties=cls.properties,
            **cls.literal_values,
        )
        cls.list_partition_dto = ListPartitionDTO(
            name="test_list_partition",
            lists=[[literal_value] for literal_value in cls.literal_values.values()],
            properties=cls.properties,
        )
        cls.partition_dtos = {
            PartitionDTO.Type.IDENTITY: cls.identity_partition_dto,
            PartitionDTO.Type.LIST: cls.list_partition_dto,
            PartitionDTO.Type.RANGE: cls.range_partition_dto,
        }

    def test_write_partition_dto_unknown_type(self):
        """Test that unknown partition types should raise IOError."""

        with patch.object(
            self.identity_partition_dto,
            "type",
            return_value=MockPartitionDTOType.INVALID_PARTITION_TYPE,
        ):
            self.assertRaises(
                IOError,
                SerdesUtils.write_partition,
                value=self.identity_partition_dto,
            )

    @patch(
        "gravitino.dto.rel.expressions.json_serdes._helper.serdes_utils.SerdesUtils.write_function_arg"
    )
    def test_write_partition_with_mocked_expression_serdes(
        self, mock_write_function_arg
    ):
        """Test write_partition with mocked expression serialization.

        To make sure the number of call to method `ExpressionsSerdesUtils.write_function_arg` are
        identical to the number of `LiteralDTO`s.
        """

        for partition_dto in self.partition_dtos.values():
            mock_write_function_arg.reset_mock()
            mock_write_function_arg_return = {"mocked": "function_arg"}
            mock_write_function_arg.return_value = mock_write_function_arg_return
            partition_dto_type = partition_dto.type()

            result = SerdesUtils.write_partition(partition_dto)

            self.assertEqual(
                mock_write_function_arg.call_count, len(self.literal_values)
            )
            if partition_dto_type is PartitionDTO.Type.IDENTITY:
                self.assertEqual(
                    result[SerdesUtils.IDENTITY_PARTITION_VALUES],
                    [mock_write_function_arg_return] * len(self.literal_values),
                )
            if partition_dto_type is PartitionDTO.Type.LIST:
                self.assertEqual(
                    result[SerdesUtils.LIST_PARTITION_LISTS],
                    [[mock_write_function_arg_return]] * len(self.literal_values),
                )
            if partition_dto_type is PartitionDTO.Type.RANGE:
                self.assertEqual(
                    [
                        result[SerdesUtils.RANGE_PARTITION_LOWER],
                        result[SerdesUtils.RANGE_PARTITION_UPPER],
                    ],
                    [mock_write_function_arg_return] * len(self.literal_values),
                )

    def test_write_partition_dto(self):
        """Test writing PartitionDTOs"""

        for partition_dto_type, partition_dto in self.partition_dtos.items():
            result = SerdesUtils.write_partition(partition_dto)

            self.assertEqual(
                result[SerdesUtils.PARTITION_TYPE], partition_dto_type.value
            )
            self.assertEqual(
                result[SerdesUtils.PARTITION_NAME],
                f"test_{partition_dto_type.value}_partition",
            )
            if partition_dto_type is PartitionDTO.Type.IDENTITY:
                self.assertEqual(result[SerdesUtils.FIELD_NAMES], self.field_names)
                self.assertIn(SerdesUtils.IDENTITY_PARTITION_VALUES, result)
                self.assertListEqual(
                    result[SerdesUtils.IDENTITY_PARTITION_VALUES],
                    [
                        ExpressionSerdesUtils.write_function_arg(literal_value)
                        for literal_value in self.literal_values.values()
                    ],
                )
            if partition_dto_type is PartitionDTO.Type.LIST:
                self.assertListEqual(
                    result[SerdesUtils.LIST_PARTITION_LISTS],
                    [
                        [ExpressionSerdesUtils.write_function_arg(literal_value)]
                        for literal_value in self.literal_values.values()
                    ],
                )
            if partition_dto_type is PartitionDTO.Type.RANGE:
                self.assertEqual(
                    result[SerdesUtils.RANGE_PARTITION_LOWER],
                    ExpressionSerdesUtils.write_function_arg(
                        arg=self.literal_values[SerdesUtils.RANGE_PARTITION_LOWER]
                    ),
                )
                self.assertEqual(
                    result[SerdesUtils.RANGE_PARTITION_UPPER],
                    ExpressionSerdesUtils.write_function_arg(
                        arg=self.literal_values[SerdesUtils.RANGE_PARTITION_UPPER]
                    ),
                )
            self.assertDictEqual(
                result[SerdesUtils.PARTITION_PROPERTIES], partition_dto.properties()
            )

    def test_write_partition_empty_values(self):
        """Test writing partition with empty values."""

        empty_partition = IdentityPartitionDTO(
            name="empty_partition", values=[], field_names=[], properties={}
        )

        result = SerdesUtils.write_partition(empty_partition)

        self.assertEqual(result[SerdesUtils.PARTITION_NAME], "empty_partition")
        self.assertEqual(result[SerdesUtils.FIELD_NAMES], [])
        self.assertEqual(result[SerdesUtils.IDENTITY_PARTITION_VALUES], [])

    def test_read_partition_dto_invalid_type(self):
        data = {SerdesUtils.PARTITION_TYPE: "invalid_type"}
        with self.assertRaises(IllegalArgumentException):
            SerdesUtils.read_partition(data=data)

    def test_read_partition_dto_invalid_data(self):
        invalid_json_data = (None, {})
        for data in invalid_json_data:
            self.assertRaisesRegex(
                IllegalArgumentException,
                "Partition must be a valid JSON object",
                SerdesUtils.read_partition,
                data=data,
            )

        self.assertRaisesRegex(
            IllegalArgumentException,
            "Partition must have a type field",
            SerdesUtils.read_partition,
            data={"invalid_field": "invalid_value"},
        )

    def test_read_partition_invalid_identity(self):
        identity_data_base = {
            SerdesUtils.PARTITION_TYPE: PartitionDTO.Type.IDENTITY.value
        }
        invalid_field_names_data = (
            identity_data_base,
            {**identity_data_base, **{SerdesUtils.FIELD_NAMES: "invalid_field_names"}},
        )
        invalid_values_data = (
            {**identity_data_base, **{SerdesUtils.FIELD_NAMES: []}},
            {
                **identity_data_base,
                **{
                    SerdesUtils.FIELD_NAMES: [],
                    SerdesUtils.IDENTITY_PARTITION_VALUES: "invalid_values",
                },
            },
        )
        for data in invalid_field_names_data:
            self.assertRaisesRegex(
                IllegalArgumentException,
                "Identity partition must have array of fieldNames",
                SerdesUtils.read_partition,
                data=data,
            )
        for data in invalid_values_data:
            self.assertRaisesRegex(
                IllegalArgumentException,
                "Identity partition must have array of values",
                SerdesUtils.read_partition,
                data=data,
            )

    def test_read_partition_invalid_list(self):
        list_data_base = {
            SerdesUtils.PARTITION_TYPE: PartitionDTO.Type.LIST.value,
            SerdesUtils.PARTITION_NAME: "list_partition",
        }
        invalid_partition_name = {
            SerdesUtils.PARTITION_TYPE: PartitionDTO.Type.LIST.value
        }
        invalid_lists_data = (
            list_data_base,
            {**list_data_base, **{SerdesUtils.LIST_PARTITION_LISTS: "invalid"}},
        )
        self.assertRaisesRegex(
            IllegalArgumentException,
            "List partition must have name",
            SerdesUtils.read_partition,
            data=invalid_partition_name,
        )
        for invalid_data in invalid_lists_data:
            self.assertRaisesRegex(
                IllegalArgumentException,
                "List partition must have array of lists",
                SerdesUtils.read_partition,
                data=invalid_data,
            )

    def test_read_partition_invalid_range(self):
        invalid_partition_name = {
            SerdesUtils.PARTITION_TYPE: PartitionDTO.Type.RANGE.value
        }
        invalid_range_data_upper = {
            SerdesUtils.PARTITION_TYPE: PartitionDTO.Type.RANGE.value,
            SerdesUtils.PARTITION_NAME: "range_partition",
        }
        invalid_range_data_lower = {
            SerdesUtils.PARTITION_TYPE: PartitionDTO.Type.RANGE.value,
            SerdesUtils.PARTITION_NAME: "range_partition",
            SerdesUtils.RANGE_PARTITION_UPPER: "upper",
        }
        self.assertRaisesRegex(
            IllegalArgumentException,
            "Range partition must have name",
            SerdesUtils.read_partition,
            data=invalid_partition_name,
        )
        self.assertRaisesRegex(
            IllegalArgumentException,
            "Range partition must have upper",
            SerdesUtils.read_partition,
            data=invalid_range_data_upper,
        )
        self.assertRaisesRegex(
            IllegalArgumentException,
            "Range partition must have lower",
            SerdesUtils.read_partition,
            data=invalid_range_data_lower,
        )

    def test_read_partition_dto(self):
        for partition_dto_type, partition_dto in self.partition_dtos.items():
            result = SerdesUtils.write_partition(partition_dto)
            partition_dto_read = SerdesUtils.read_partition(result)

            self.assertEqual(partition_dto.name(), partition_dto_read.name())
            self.assertEqual(partition_dto.type(), partition_dto_read.type())
            if partition_dto_type is PartitionDTO.Type.IDENTITY:
                dto = cast(IdentityPartitionDTO, partition_dto_read)
                self.assertListEqual(partition_dto.field_names(), dto.field_names())
                self.assertListEqual(partition_dto.values(), dto.values())
                self.assertEqual(partition_dto.properties(), dto.properties())

            if partition_dto_type is PartitionDTO.Type.LIST:
                dto = cast(ListPartitionDTO, partition_dto_read)
                self.assertListEqual(partition_dto.lists(), dto.lists())
                self.assertEqual(partition_dto.properties(), dto.properties())

            if partition_dto_type is PartitionDTO.Type.RANGE:
                dto = cast(RangePartitionDTO, partition_dto_read)
                self.assertEqual(partition_dto.lower(), dto.lower())
                self.assertEqual(partition_dto.upper(), dto.upper())
                self.assertEqual(partition_dto.properties(), dto.properties())

    def test_partition_dto_serdes(self):
        for partition_dto_type, partition_dto in self.partition_dtos.items():
            serialized_data = PartitionDTOSerdes.serialize(partition_dto)
            deserialized_partition_dto = PartitionDTOSerdes.deserialize(serialized_data)

            self.assertEqual(partition_dto.name(), deserialized_partition_dto.name())
            self.assertEqual(partition_dto.type(), deserialized_partition_dto.type())
            if partition_dto_type is PartitionDTO.Type.IDENTITY:
                dto = cast(IdentityPartitionDTO, deserialized_partition_dto)
                self.assertListEqual(partition_dto.field_names(), dto.field_names())
                self.assertListEqual(partition_dto.values(), dto.values())
                self.assertEqual(partition_dto.properties(), dto.properties())

            if partition_dto_type is PartitionDTO.Type.LIST:
                dto = cast(ListPartitionDTO, deserialized_partition_dto)
                self.assertListEqual(partition_dto.lists(), dto.lists())
                self.assertEqual(partition_dto.properties(), dto.properties())

            if partition_dto_type is PartitionDTO.Type.RANGE:
                dto = cast(RangePartitionDTO, deserialized_partition_dto)
                self.assertEqual(partition_dto.lower(), dto.lower())
                self.assertEqual(partition_dto.upper(), dto.upper())
                self.assertEqual(partition_dto.properties(), dto.properties())

    def test_partition_dto_serdes_identity_from_json_string(self):
        """Tests deserialize `IdentityPartitionDTO` from JSON string."""

        expected_json_string = """
            {
                "type": "identity",
                "name": "test_identity_partition",
                "fieldNames": [
                    [
                        "upper"
                    ],
                    [
                        "lower"
                    ]
                ],
                "values": [
                    {
                        "type": "literal",
                        "dataType": "integer",
                        "value": "0"
                    },
                    {
                        "type": "literal",
                        "dataType": "integer",
                        "value": "100"
                    }
                ],
                "properties": {
                    "key1": "value1",
                    "key2": "value2"
                }
            }
        """
        expected_serialized = json.loads(expected_json_string)
        deserialized_dto = PartitionDTOSerdes.deserialize(expected_serialized)
        self.assertTrue(deserialized_dto == self.identity_partition_dto)

    def test_partition_dto_serdes_list_from_json_string(self):
        """Tests deserialize `ListPartitionDTO` from JSON string."""

        additional_literal_values = {
            "upper": LiteralDTO.builder()
            .with_data_type(Types.IntegerType.get())
            .with_value(value="101")
            .build(),
            "lower": LiteralDTO.builder()
            .with_data_type(Types.IntegerType.get())
            .with_value(value="200")
            .build(),
        }
        list_partition_dto = ListPartitionDTO(
            name="test_list_partition",
            lists=[
                list(self.literal_values.values()),
                list(additional_literal_values.values()),
            ],
            properties={},
        )
        expected_json_string = """
            {
                "type": "list",
                "name": "test_list_partition",
                "lists": [
                    [
                        {
                            "type": "literal",
                            "dataType": "integer",
                            "value": "0"
                        },
                        {
                            "type": "literal",
                            "dataType": "integer",
                            "value": "100"
                        }
                    ],
                    [
                        {
                            "type": "literal",
                            "dataType": "integer",
                            "value": "101"
                        },
                        {
                            "type": "literal",
                            "dataType": "integer",
                            "value": "200"
                        }
                    ]
                ]
            }
        """
        expected_serialized = json.loads(expected_json_string)
        deserialized_dto = cast(
            ListPartitionDTO, PartitionDTOSerdes.deserialize(expected_serialized)
        )
        self.assertIs(deserialized_dto.type(), list_partition_dto.type())
        self.assertEqual(deserialized_dto.name(), list_partition_dto.name())
        self.assertListEqual(deserialized_dto.lists(), list_partition_dto.lists())
        self.assertEqual(deserialized_dto.properties(), {})

    def test_partition_dto_serdes_range_from_json_string(self):
        """Tests deserialize `RangePartitionDTO` from JSON string."""

        expected_json_string = """
            {
                "type": "range",
                "name": "test_range_partition",
                "upper": {
                    "type": "literal",
                    "dataType": "integer",
                    "value": "0"
                },
                "lower": {
                    "type": "literal",
                    "dataType": "integer",
                    "value": "100"
                },
                "properties": {
                    "key1": "value1",
                    "key2": "value2"
                }
            }
        """
        expected_serialized = json.loads(expected_json_string)
        deserialized_dto = PartitionDTOSerdes.deserialize(expected_serialized)
        self.assertTrue(deserialized_dto == self.range_partition_dto)
