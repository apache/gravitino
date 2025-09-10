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
from dataclasses import dataclass, field
from typing import cast

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.expressions.sorts.null_ordering import NullOrdering
from gravitino.api.expressions.sorts.sort_direction import SortDirection
from gravitino.api.types.types import Types
from gravitino.dto.rel.expressions.field_reference_dto import FieldReferenceDTO
from gravitino.dto.rel.expressions.func_expression_dto import FuncExpressionDTO
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO
from gravitino.dto.rel.json_serdes.sort_order_serdes import SortOrderSerdes
from gravitino.dto.rel.sort_order_dto import SortOrderDTO
from gravitino.exceptions.base import IllegalArgumentException


@dataclass
class MockDataClass(DataClassJsonMixin):
    sort_orders: list[SortOrderDTO] = field(
        metadata=config(
            field_name="sortOrders",
            encoder=lambda data: [SortOrderSerdes.serialize(item) for item in data],
            decoder=lambda data: [SortOrderSerdes.deserialize(item) for item in data],
        )
    )


class TestSortOrderSerdes(unittest.TestCase):
    def test_sort_order_serdes_invalid_json(self):
        json_strings = [
            '{"sortOrders": [{}]}',
            '{"sortOrders": [""]}',
            '{"sortOrders": [null]}',
        ]

        with self.assertRaisesRegex(
            IllegalArgumentException, "Cannot parse sort order from invalid JSON"
        ):
            for json_string in json_strings:
                MockDataClass.from_json(json_string)

    def test_sort_order_serdes_missing_sort_term(self):
        json_string = '{"sortOrders": [{"direction": "asc"}]}'

        with self.assertRaisesRegex(
            IllegalArgumentException, "Cannot parse sort order from missing sort term"
        ):
            MockDataClass.from_json(json_string)

    def test_sort_order_serdes_invalid_sort_term(self):
        json_string = '{"sortOrders": [{"sortTerm": null, "direction": "asc"}]}'

        with self.assertRaisesRegex(
            IllegalArgumentException, "expression cannot be null"
        ):
            MockDataClass.from_json(json_string)

    def test_sort_order_serdes(self):
        json_string = """
        {
            "sortOrders": [
                {
                    "sortTerm": {
                        "type": "field",
                        "fieldName": ["age"]
                    },
                    "direction": "asc",
                    "nullOrdering": "nulls_first"
                }
            ]
        }
        """

        mock_data_class = MockDataClass.from_json(json_string)
        sort_orders = mock_data_class.sort_orders
        self.assertEqual(len(sort_orders), 1)
        self.assertIsInstance(sort_orders[0], SortOrderDTO)
        self.assertEqual(
            cast(FieldReferenceDTO, sort_orders[0].sort_term()).field_name(), ["age"]
        )
        self.assertIs(sort_orders[0].null_ordering(), NullOrdering.NULLS_FIRST)
        self.assertIs(sort_orders[0].direction(), SortDirection.ASCENDING)

        serialized = mock_data_class.to_json()
        self.assertDictEqual(json.loads(json_string), json.loads(serialized))

    def test_sort_order_serdes_function_sort_term(self):
        json_string = """
        {
            "sortOrders": [
                {
                    "sortTerm": {
                        "type": "function",
                        "funcName": "date_trunc",
                        "funcArgs": [
                            {
                                "type": "literal",
                                "dataType": "string",
                                "value": "year"
                            },
                            {
                                "type": "field",
                                "fieldName": ["birthday"]
                            }
                        ]
                    },
                    "direction": "desc",
                    "nullOrdering": "nulls_last"
                }
            ]
        }
        """

        mock_data_class = MockDataClass.from_json(json_string)
        sort_orders = mock_data_class.sort_orders
        self.assertEqual(len(sort_orders), 1)
        self.assertIsInstance(sort_orders[0], SortOrderDTO)
        deserialized_func_dto = cast(FuncExpressionDTO, sort_orders[0].sort_term())
        expected_func_dto = (
            FuncExpressionDTO.builder()
            .with_function_name("date_trunc")
            .with_function_args(
                [
                    LiteralDTO.builder()
                    .with_data_type(Types.StringType())
                    .with_value("year")
                    .build(),
                    FieldReferenceDTO.builder().with_field_name(["birthday"]).build(),
                ]
            )
            .build()
        )
        self.assertTrue(deserialized_func_dto == expected_func_dto)
        self.assertIs(sort_orders[0].null_ordering(), NullOrdering.NULLS_LAST)
        self.assertIs(sort_orders[0].direction(), SortDirection.DESCENDING)

        serialized = mock_data_class.to_json()
        self.assertDictEqual(json.loads(json_string), json.loads(serialized))
