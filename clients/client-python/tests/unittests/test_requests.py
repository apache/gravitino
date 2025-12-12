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
from typing import cast

from gravitino.dto.requests.add_partitions_request import AddPartitionsRequest
from gravitino.dto.requests.table_create_request import TableCreateRequest
from gravitino.exceptions.base import IllegalArgumentException


class TestRequests(unittest.TestCase):
    def test_add_partitions_request(self):
        json_str = """
            {
                "partitions": [
                    {
                        "type": "identity",
                        "name": "partition_1",
                        "fieldNames": [["id"]],
                        "values": [
                            {
                                "type": "literal",
                                "dataType": "integer",
                                "value": "0"
                            }
                        ],
                        "properties": {
                            "key1": "value1",
                            "key2": "value2"
                        }
                    }
                ]
            }
        """
        partitions = json.loads(json_str)
        req = AddPartitionsRequest.from_json(json_str)
        req_dict = cast(dict, req.to_dict())
        self.assertListEqual(req_dict["partitions"], partitions["partitions"])

        multiple_partitions_json = """
            {
                "partitions": [
                    {
                        "type": "identity",
                        "name": "partition_1",
                        "fieldNames": [["id"]],
                        "values": [
                            {
                                "type": "literal",
                                "dataType": "integer",
                                "value": "0"
                            }
                        ]
                    },
                    {
                        "type": "identity",
                        "name": "partition_2",
                        "fieldNames": [["id"]],
                        "values": [
                            {
                                "type": "literal",
                                "dataType": "integer",
                                "value": "1"
                            }
                        ]
                    }
                ]
            }
        """
        exceptions = {
            "partitions must not be null": '{"partitions": null}',
            "Haven't yet implemented multiple partitions": multiple_partitions_json,
        }
        for exception_str, json_str in exceptions.items():
            with self.assertRaisesRegex(IllegalArgumentException, exception_str):
                req = AddPartitionsRequest.from_json(json_str)
                req.validate()

    def test_table_create_request(self):
        json_str = """
            {
                "name": "example_table",
                "comment": "This is an example table",
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
                    },
                    {
                        "name": "age",
                        "type": "integer",
                        "comment": "age column comment",
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

        req = TableCreateRequest.from_json(json_str)
        req.validate()

        multiple_auto_increment_json_str = """
            {
                "name": "example_table",
                "comment": "This is an example table",
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
                        "name": "age",
                        "type": "integer",
                        "comment": "age column comment",
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
        exceptions = {
            '"name" field is required and cannot be empty': '{"name":"","columns":[]}',
            "Only one column can be auto-incremented.": multiple_auto_increment_json_str,
        }
        for exception_str, json_str in exceptions.items():
            with self.assertRaisesRegex(IllegalArgumentException, exception_str):
                req = TableCreateRequest.from_json(json_str)
                req.validate()
