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
from typing import Any


def _table_columns(include_age_column: bool = False) -> list[dict[str, Any]]:
    columns = [
        {
            "name": "id",
            "type": "integer",
            "comment": "id column comment",
            "nullable": False,
            "autoIncrement": True,
            "defaultValue": {
                "type": "literal",
                "dataType": "integer",
                "value": "-1",
            },
        },
        {
            "name": "name",
            "type": "varchar(500)",
            "comment": "name column comment",
            "nullable": True,
            "autoIncrement": False,
            "defaultValue": {
                "type": "literal",
                "dataType": "null",
                "value": "null",
            },
        },
        {
            "name": "StartingDate",
            "type": "timestamp",
            "comment": "StartingDate column comment",
            "nullable": False,
            "autoIncrement": False,
            "defaultValue": {
                "type": "function",
                "funcName": "current_timestamp",
                "funcArgs": [],
            },
        },
        {
            "name": "info",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "position",
                        "type": "string",
                        "nullable": True,
                        "comment": "position field comment",
                    },
                    {
                        "name": "contact",
                        "type": {
                            "type": "list",
                            "elementType": "integer",
                            "containsNull": False,
                        },
                        "nullable": True,
                        "comment": "contact field comment",
                    },
                    {
                        "name": "rating",
                        "type": {
                            "type": "map",
                            "keyType": "string",
                            "valueType": "integer",
                            "valueContainsNull": False,
                        },
                        "nullable": True,
                        "comment": "rating field comment",
                    },
                ],
            },
            "comment": "info column comment",
            "nullable": True,
        },
        {
            "name": "dt",
            "type": "date",
            "comment": "dt column comment",
            "nullable": True,
        },
    ]

    if include_age_column:
        columns.append(
            {
                "name": "age",
                "type": "integer",
                "comment": "age column comment",
                "nullable": True,
            }
        )

    return columns


def table_json(
    creator: str = "Apache Gravitino",
    include_age_column: bool = False,
    include_audit: bool = True,
    sort_field_name: str = "age",
) -> str:
    table_data: dict[str, Any] = {
        "name": "example_table",
        "comment": "This is an example table",
        "columns": _table_columns(include_age_column),
        "partitioning": [
            {
                "strategy": "identity",
                "fieldName": ["dt"],
            }
        ],
        "distribution": {
            "strategy": "hash",
            "number": 32,
            "funcArgs": [
                {
                    "type": "field",
                    "fieldName": ["id"],
                }
            ],
        },
        "sortOrders": [
            {
                "sortTerm": {
                    "type": "field",
                    "fieldName": [sort_field_name],
                },
                "direction": "asc",
                "nullOrdering": "nulls_first",
            }
        ],
        "indexes": [
            {
                "indexType": "primary_key",
                "name": "PRIMARY",
                "fieldNames": [["id"]],
            }
        ],
        "properties": {
            "format": "ORC",
        },
    }

    if include_audit:
        table_data["audit"] = {
            "creator": creator,
            "createTime": "2025-10-10T00:00:00",
        }

    return json.dumps(table_data, indent=4)


def identity_partition_json(
    field_names: list[list[str]] | None = None,
    properties: dict[str, str] | None = None,
) -> str:
    partition_data: dict[str, Any] = {
        "type": "identity",
        "name": "test_identity_partition",
        "fieldNames": field_names or [["column_name"]],
        "values": [
            {
                "type": "literal",
                "dataType": "integer",
                "value": "0",
            },
            {
                "type": "literal",
                "dataType": "integer",
                "value": "100",
            },
        ],
    }

    if properties is not None:
        partition_data["properties"] = properties

    return json.dumps(partition_data, indent=4)


TABLE_DTO_JSON_STRING = table_json()
TABLE_DTO_JSON_STRING_WITH_STARTING_DATE_SORT = table_json(
    sort_field_name="StartingDate"
)
TABLE_JSON_STRING_WITH_ANONYMOUS_CREATOR_AND_ID_SORT = table_json(
    creator="anonymous",
    sort_field_name="id",
)
TABLE_CREATE_REQUEST_JSON_STRING = table_json(
    include_age_column=True,
    include_audit=False,
)
IDENTITY_PARTITION_JSON_STRING = identity_partition_json()
IDENTITY_PARTITION_WITH_PROPERTIES_JSON_STRING = identity_partition_json(
    field_names=[["upper"], ["lower"]],
    properties={
        "key1": "value1",
        "key2": "value2",
    },
)
