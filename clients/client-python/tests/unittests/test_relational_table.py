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
from http.client import HTTPResponse
from typing import cast
from unittest.mock import Mock, patch

from gravitino.api.rel.expressions.distributions.strategy import Strategy
from gravitino.api.rel.expressions.literals.literals import Literals
from gravitino.api.rel.expressions.sorts.null_ordering import NullOrdering
from gravitino.api.rel.expressions.sorts.sort_direction import SortDirection
from gravitino.api.rel.expressions.transforms.transforms import Transforms
from gravitino.api.rel.indexes.index import Index
from gravitino.api.rel.partitions.partitions import Partitions
from gravitino.client.generic_column import GenericColumn
from gravitino.client.relational_table import RelationalTable
from gravitino.dto.rel.partitions.json_serdes.partition_dto_serdes import (
    PartitionDTOSerdes,
)
from gravitino.dto.rel.table_dto import TableDTO
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.partition_list_response import PartitionListResponse
from gravitino.dto.responses.partition_name_list_response import (
    PartitionNameListResponse,
)
from gravitino.dto.responses.partition_response import PartitionResponse
from gravitino.namespace import Namespace
from gravitino.rest.rest_utils import encode_string
from gravitino.utils import HTTPClient, Response


class TestRelationalTable(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.TABLE_DTO_JSON_STRING = """
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

        cls.PARTITION_JSON_STRING = """
            {
                "type": "identity",
                "name": "test_identity_partition",
                "fieldNames": [
                    [
                        "column_name"
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
                ]
            }
        """

        cls.table_dto = TableDTO.from_json(cls.TABLE_DTO_JSON_STRING)
        cls.namespace = Namespace.of("metalake_demo", "test_catalog", "test_schema")
        cls.rest_client = HTTPClient("http://localhost:8090")
        cls.relational_table = RelationalTable(
            cls.namespace, cls.table_dto, cls.rest_client
        )

    def _get_mock_http_resp(self, json_str: str):
        mock_http_resp = Mock(HTTPResponse)
        mock_http_resp.getcode.return_value = 200
        mock_http_resp.read.return_value = json_str
        mock_http_resp.info.return_value = None
        mock_http_resp.url = None
        mock_resp = Response(mock_http_resp)
        return mock_resp

    def test_get_partition_request_path(self):
        expected = (
            f"api/metalakes/{encode_string(self.namespace.level(0))}"
            f"/catalogs/{encode_string(self.namespace.level(1))}"
            f"/schemas/{encode_string(self.namespace.level(2))}"
            f"/tables/{encode_string(self.relational_table.name())}"
            "/partitions/"
        )
        self.assertEqual(self.relational_table.get_partition_request_path(), expected)

    def test_list_partition_names(self):
        resp_body = PartitionNameListResponse(0, ["partition_1", "partition_2"])
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ):
            names = self.relational_table.list_partition_names()
            self.assertListEqual(names, resp_body.partition_names())

    def test_columns(self):
        cols = self.relational_table.columns()
        self.assertEqual(len(cols), len(self.table_dto.columns()))
        self.assertTrue(all(isinstance(col, GenericColumn) for col in cols))

    def test_list_partitions(self):
        expected_serialized = json.loads(TestRelationalTable.PARTITION_JSON_STRING)
        partitions = [PartitionDTOSerdes.deserialize(expected_serialized)]
        resp_body = PartitionListResponse(0, partitions)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ):
            partitions = self.relational_table.list_partitions()
            self.assertListEqual(partitions, resp_body.get_partitions())

    def test_get_partition(self):
        expected_serialized = json.loads(TestRelationalTable.PARTITION_JSON_STRING)
        partition_dto = PartitionDTOSerdes.deserialize(expected_serialized)
        resp_body = PartitionResponse(0, partition_dto)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ):
            partition = self.relational_table.get_partition("partition_name")
            self.assertEqual(partition, resp_body.get_partition())

    def test_drop_partition(self):
        resp_body = DropResponse(0, True)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.delete",
            return_value=mock_resp,
        ):
            self.assertTrue(self.relational_table.drop_partition("partition_name"))

    def test_add_partition(self):
        partition = Partitions.identity(
            "test_identity_partition",
            [["column_name"]],
            [Literals.integer_literal(0), Literals.integer_literal(100)],
        )
        expected_serialized = json.loads(TestRelationalTable.PARTITION_JSON_STRING)
        partitions = [PartitionDTOSerdes.deserialize(expected_serialized)]
        resp_body = PartitionListResponse(0, partitions)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.post",
            return_value=mock_resp,
        ):
            added_partition = self.relational_table.add_partition(partition)
            self.assertEqual(added_partition, resp_body.get_partitions()[0])

    def test_get_name(self):
        self.assertEqual(self.relational_table.name(), "example_table")

    def test_get_comment(self):
        self.assertEqual(self.relational_table.comment(), "This is an example table")

    def test_get_partitioning(self):
        partitioning_list = self.relational_table.partitioning()
        partitioning = cast(Transforms.IdentityTransform, partitioning_list[0])

        self.assertEqual(len(partitioning_list), 1)
        self.assertListEqual(partitioning.field_name(), ["dt"])

    def test_get_sort_order(self):
        sort_order_list = self.relational_table.sort_order()
        sort_order = sort_order_list[0]

        self.assertEqual(len(sort_order_list), 1)
        self.assertEqual(sort_order.direction(), SortDirection.ASCENDING)
        self.assertEqual(sort_order.null_ordering(), NullOrdering.NULLS_FIRST)

    def test_get_distribution(self):
        distribution = self.relational_table.distribution()
        self.assertEqual(distribution.strategy(), Strategy.HASH)
        self.assertEqual(distribution.number(), 32)

    def test_get_index(self):
        index_list = self.relational_table.index()
        index = index_list[0]
        self.assertEqual(len(index_list), 1)
        self.assertEqual(index.name(), "PRIMARY")
        self.assertEqual(index.type(), Index.IndexType.PRIMARY_KEY)

    def test_get_audit_info(self):
        audit_info = self.relational_table.audit_info()
        self.assertEqual(audit_info.creator(), "Apache Gravitino")
        self.assertEqual(audit_info.create_time(), "2025-10-10T00:00:00")

    def test_get_properties(self):
        properties = self.relational_table.properties()
        self.assertDictEqual(properties, {"format": "ORC"})
