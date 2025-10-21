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

from gravitino.api.rel.expressions.distributions.strategy import Strategy
from gravitino.api.rel.expressions.sorts.null_ordering import NullOrdering
from gravitino.api.rel.expressions.sorts.sort_direction import SortDirection
from gravitino.api.rel.indexes.index import Index
from gravitino.api.rel.types.types import Types
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.distribution_dto import DistributionDTO
from gravitino.dto.rel.expressions.field_reference_dto import FieldReferenceDTO
from gravitino.dto.rel.indexes.index_dto import IndexDTO
from gravitino.dto.rel.partitioning.identity_partitioning_dto import (
    IdentityPartitioningDTO,
)
from gravitino.dto.rel.partitioning.partitioning import SingleFieldPartitioning
from gravitino.dto.rel.sort_order_dto import SortOrderDTO
from gravitino.dto.rel.table_dto import TableDTO
from gravitino.exceptions.base import IllegalArgumentException


class TestTableDTO(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.complete_json = """
        {
            "name": "example_table",
            "comment": "This is an example table",
            "audit": {
                "creator": "Apache Gravitino",
                "createTime":"2025-10-10T00:00:00"
            },
            "columns": [
                {
                    "name": "id",
                    "type": "integer",
                    "comment": "id column comment",
                    "nullable": false,
                    "autoIncrement": true,
                    "defaultValue": {
                        "type": "literal",
                        "dataType": "integer",
                        "value": "-1"
                    }
                },
                {
                    "name": "name",
                    "type": "varchar(500)",
                    "comment": "name column comment",
                    "nullable": true,
                    "autoIncrement": false,
                    "defaultValue": {
                        "type": "literal",
                        "dataType": "null",
                        "value": "null"
                    }
                },
                {
                    "name": "StartingDate",
                    "type": "timestamp",
                    "comment": "StartingDate column comment",
                    "nullable": false,
                    "autoIncrement": false,
                    "defaultValue": {
                        "type": "function",
                        "funcName": "current_timestamp",
                        "funcArgs": []
                    }
                },
                {
                    "name": "info",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "position",
                                "type": "string",
                                "nullable": true,
                                "comment": "position field comment"
                            },
                            {
                                "name": "contact",
                                "type": {
                                "type": "list",
                                "elementType": "integer",
                                "containsNull": false
                                },
                                "nullable": true,
                                "comment": "contact field comment"
                            },
                            {
                                "name": "rating",
                                "type": {
                                "type": "map",
                                "keyType": "string",
                                "valueType": "integer",
                                "valueContainsNull": false
                                },
                                "nullable": true,
                                "comment": "rating field comment"
                            }
                        ]
                    },
                    "comment": "info column comment",
                    "nullable": true
                },
                {
                    "name": "dt",
                    "type": "date",
                    "comment": "dt column comment",
                    "nullable": true
                }
            ],
            "partitioning": [
                {
                    "strategy": "identity",
                    "fieldName": [ "dt" ]
                }
            ],
            "distribution": {
                "strategy": "hash",
                "number": 32,
                "funcArgs": [
                    {
                        "type": "field",
                        "fieldName": [ "id" ]
                    }
                ]
            },
            "sortOrders": [
                {
                    "sortTerm": {
                        "type": "field",
                        "fieldName": [ "age" ]
                    },
                    "direction": "asc",
                    "nullOrdering": "nulls_first"
                }
            ],
            "indexes": [
                {
                    "indexType": "primary_key",
                    "name": "PRIMARY",
                    "fieldNames": [["id"]]
                }
            ],
            "properties": {
                "format": "ORC"
            }
        }
        """

    def test_table_dto_complete_deserialize(self):
        dto = TableDTO.from_json(self.complete_json)

        partitioning_dto = dto.partitioning()[0]
        self.assertIsInstance(partitioning_dto, IdentityPartitioningDTO)
        self.assertIs(
            partitioning_dto.strategy(), SingleFieldPartitioning.Strategy.IDENTITY
        )

        sort_order_dto = dto.sort_order()[0]
        self.assertIsInstance(sort_order_dto, SortOrderDTO)
        self.assertIs(sort_order_dto.direction(), SortDirection.ASCENDING)
        self.assertIsInstance(sort_order_dto.sort_term(), FieldReferenceDTO)

        index_dto = dto.index()[0]
        self.assertIsInstance(index_dto, IndexDTO)
        self.assertIs(index_dto.type(), Index.IndexType.PRIMARY_KEY)
        self.assertEqual(index_dto.name(), "PRIMARY")
        self.assertListEqual(index_dto.field_names(), [["id"]])

        self.assertDictEqual(dto.properties(), {"format": "ORC"})

    def test_table_dto_deserialize_required_fields_success(self):
        json_string = """
        {
            "name": "example_table",
            "audit": {
                "creator": "Apache Gravitino",
                "createTime":"2025-10-10T00:00:00"
            },
            "columns": [
                {
                    "name": "id",
                    "type": "integer",
                    "comment": "id column comment",
                    "nullable": false,
                    "autoIncrement": true,
                    "defaultValue": {
                        "type": "literal",
                        "dataType": "integer",
                        "value": "-1"
                    }
                }
            ]
        }
        """

        dto = TableDTO.from_json(json_string)
        self.assertEqual(dto.name(), "example_table")
        self.assertEqual(dto.audit_info().creator(), "Apache Gravitino")
        self.assertEqual(dto.audit_info().create_time(), "2025-10-10T00:00:00")
        self.assertEqual(len(dto.columns()), 1)
        self.assertIsInstance(dto.columns()[0], ColumnDTO)

        self.assertIsNone(dto.comment())
        self.assertIsNone(dto.distribution())
        self.assertIsNone(dto.index())
        self.assertIsNone(dto.partitioning())
        self.assertIsNone(dto.sort_order())
        self.assertIsNone(dto.distribution())
        self.assertIsNone(dto.properties())

    def test_table_dto_deserialize_invalid_name(self):
        json_string = """
        {
            "name": "",
            "audit": {
                "creator": "Apache Gravitino",
                "createTime":"2025-10-10T00:00:00"
            },
            "columns": [
                {
                    "name": "id",
                    "type": "integer",
                    "comment": "id column comment",
                    "nullable": false,
                    "autoIncrement": true,
                    "defaultValue": {
                        "type": "literal",
                        "dataType": "integer",
                        "value": "-1"
                    }
                }
            ]
        }
        """

        with self.assertRaisesRegex(
            IllegalArgumentException, "name cannot be null or empty"
        ):
            TableDTO.from_json(json_string)

    def test_table_dto_deserialize_invalid_audit(self):
        json_string = """
        {
            "name": "example_table",
            "columns": [
                {
                    "name": "id",
                    "type": "integer",
                    "comment": "id column comment",
                    "nullable": false,
                    "autoIncrement": true,
                    "defaultValue": {
                        "type": "literal",
                        "dataType": "integer",
                        "value": "-1"
                    }
                }
            ]
        }
        """

        with self.assertRaisesRegex(IllegalArgumentException, "audit cannot be null"):
            TableDTO.from_json(json_string)

    def test_table_dto_deserialize_invalid_columns(self):
        json_string = """
        {
            "name": "example_table",
            "audit": {
                "creator": "Apache Gravitino",
                "createTime":"2025-10-10T00:00:00"
            },
            "columns": []
        }
        """

        with self.assertRaisesRegex(
            IllegalArgumentException, "columns cannot be null or empty"
        ):
            TableDTO.from_json(json_string)

    def test_table_dto_serialize_with_none_values(self):
        """Tests the fields with none value are excluded after serialization."""

        optional_fields = {
            "comment",
            "distribution",
            "partitioning",
            "sortOrders",
            "indexes",
            "properties",
        }
        dto = TableDTO(
            "example_table",
            [
                ColumnDTO.builder()
                .with_name("id")
                .with_data_type(Types.IntegerType.get())
                .build()
            ],
            AuditDTO("Apache Gravitino"),
        )

        dto_dict = json.loads(dto.to_json())
        for field in optional_fields:
            self.assertNotIn(field, dto_dict)

    def test_table_dto_serialize_required_fields_success(self):
        dto = TableDTO(
            "example_table",
            [
                ColumnDTO.builder()
                .with_name("id")
                .with_data_type(Types.IntegerType.get())
                .build()
            ],
            AuditDTO("Apache Gravitino"),
        )

        dto_dict = json.loads(dto.to_json())
        self.assertEqual(dto_dict["name"], "example_table")
        self.assertEqual(dto_dict["audit"]["creator"], "Apache Gravitino")
        self.assertEqual(len(dto_dict["columns"]), 1)
        self.assertEqual(dto_dict["columns"][0]["name"], "id")
        self.assertEqual(dto_dict["columns"][0]["type"], "integer")

    def test_table_dto_serialize_with_distribution(self):
        """Tests the distribution field is serialized correctly."""

        dto = TableDTO(
            "example_table",
            [
                ColumnDTO.builder()
                .with_name("id")
                .with_data_type(Types.IntegerType.get())
                .build()
            ],
            AuditDTO("Apache Gravitino"),
            "comment for example_table",
            _distribution=DistributionDTO(
                strategy=Strategy.HASH,
                number=32,
                args=[FieldReferenceDTO(["id"])],
            ),
        )

        dto_dict = json.loads(dto.to_json())
        self.assertEqual(dto_dict["distribution"]["strategy"], "hash")
        self.assertEqual(dto_dict["distribution"]["number"], 32)
        self.assertEqual(dto_dict["distribution"]["funcArgs"][0]["fieldName"], ["id"])

    def test_table_dto_serialize_with_partitioning(self):
        """Tests the partitioning field is serialized correctly."""

        dto = TableDTO(
            "example_table",
            [
                ColumnDTO.builder()
                .with_name("id")
                .with_data_type(Types.IntegerType.get())
                .build()
            ],
            AuditDTO("Apache Gravitino"),
            "comment for example_table",
            _partitioning=[IdentityPartitioningDTO("id")],
        )

        dto_dict = json.loads(dto.to_json())
        self.assertEqual(dto_dict["partitioning"][0]["strategy"], "identity")
        self.assertEqual(dto_dict["partitioning"][0]["fieldName"], ["id"])

    def test_table_dto_serialize_with_sort_order(self):
        """Tests the sort order field is serialized correctly."""

        dto = TableDTO(
            "example_table",
            [
                ColumnDTO.builder()
                .with_name("id")
                .with_data_type(Types.IntegerType.get())
                .build()
            ],
            AuditDTO("Apache Gravitino"),
            "comment for example_table",
            _sort_orders=[
                SortOrderDTO(
                    FieldReferenceDTO(["id"]),
                    SortDirection.ASCENDING,
                    NullOrdering.NULLS_FIRST,
                )
            ],
        )

        dto_dict = json.loads(dto.to_json())
        self.assertEqual(dto_dict["sortOrders"][0]["sortTerm"]["fieldName"], ["id"])
        self.assertEqual(dto_dict["sortOrders"][0]["direction"], "asc")
        self.assertEqual(dto_dict["sortOrders"][0]["nullOrdering"], "nulls_first")

    def test_table_dto_serialize_with_indexes(self):
        """Tests the indexes field is serialized correctly."""

        dto = TableDTO(
            "example_table",
            [
                ColumnDTO.builder()
                .with_name("id")
                .with_data_type(Types.IntegerType.get())
                .build()
            ],
            AuditDTO("Apache Gravitino"),
            "comment for example_table",
            _indexes=[
                IndexDTO(
                    Index.IndexType.PRIMARY_KEY,
                    "PRIMARY",
                    [["id"]],
                )
            ],
        )

        dto_dict = json.loads(dto.to_json())
        self.assertEqual(dto_dict["indexes"][0]["indexType"], "PRIMARY_KEY")
        self.assertEqual(dto_dict["indexes"][0]["name"], "PRIMARY")
        self.assertEqual(dto_dict["indexes"][0]["fieldNames"], [["id"]])
