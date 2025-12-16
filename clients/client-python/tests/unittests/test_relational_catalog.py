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
from http.client import HTTPResponse
from unittest.mock import Mock, patch

from gravitino.client.relational_catalog import RelationalCatalog
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.rel.distribution_dto import DistributionDTO
from gravitino.dto.rel.table_dto import TableDTO
from gravitino.dto.responses.entity_list_response import EntityListResponse
from gravitino.dto.responses.table_response import TableResponse
from gravitino.dto.util.dto_converters import DTOConverters
from gravitino.exceptions.base import (
    NoSuchSchemaException,
    NoSuchTableException,
    TableAlreadyExistsException,
)
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace
from gravitino.utils import HTTPClient, Response


class TestRelationalCatalog(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.metalake_name = "test_metalake"
        cls.catalog_name = "test_catalog"
        cls.schema_name = "test_schema"
        cls.table_name = "test_table"
        cls.catalog_namespace = Namespace.of(cls.metalake_name)
        cls.table_identifier = NameIdentifier.of(cls.schema_name, cls.table_name)
        cls.rest_client = HTTPClient("http://localhost:8090")
        cls.catalog = RelationalCatalog(
            catalog_namespace=cls.catalog_namespace,
            name=cls.catalog_name,
            catalog_type=RelationalCatalog.Type.RELATIONAL,
            provider="test_provider",
            audit=AuditDTO("anonymous"),
            rest_client=cls.rest_client,
        )
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
                        "fieldName": [ "StartingDate" ]
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
        cls.table_dto = TableDTO.from_json(cls.TABLE_DTO_JSON_STRING)

    def _get_mock_http_resp(self, json_str: str, return_code: int = 200):
        mock_http_resp = Mock(HTTPResponse)
        mock_http_resp.getcode.return_value = return_code
        mock_http_resp.read.return_value = json_str
        mock_http_resp.info.return_value = None
        mock_http_resp.url = None
        mock_resp = Response(mock_http_resp)
        return mock_resp

    def test_as_table_catalog(self):
        table_catalog = self.catalog.as_table_catalog()
        self.assertIs(table_catalog, self.catalog)

    def test_create_table(self):
        resp_body = TableResponse(0, self.table_dto)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.post",
            return_value=mock_resp,
        ):
            table = self.catalog.create_table(
                identifier=self.table_identifier,
                columns=DTOConverters.from_dtos(self.table_dto.columns()),
                partitioning=DTOConverters.from_dtos(self.table_dto.partitioning()),
                distribution=DTOConverters.from_dto(
                    self.table_dto.distribution() or DistributionDTO.NONE
                ),
                sort_orders=DTOConverters.from_dtos(self.table_dto.sort_order()),
                indexes=DTOConverters.from_dtos(self.table_dto.index()),
                properties=self.table_dto.properties(),
            )
            self.assertEqual(table.name(), self.table_dto.name())

    def test_load_table(self):
        resp_body = TableResponse(0, self.table_dto)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ):
            table = self.catalog.load_table(self.table_identifier)
            self.assertEqual(table.name(), self.table_dto.name())

    def test_list_tables(self):
        resp_body = EntityListResponse(
            0,
            [
                NameIdentifier.of(
                    self.metalake_name,
                    self.catalog_name,
                    self.schema_name,
                    self.table_name,
                )
            ],
        )
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ):
            tables = self.catalog.list_tables(namespace=Namespace.of(self.schema_name))
            self.assertEqual(len(tables), 1)
            self.assertEqual(tables[0], self.table_identifier)

    def test_load_table_not_exists(self):
        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=NoSuchTableException("Table not found"),
        ):
            with self.assertRaises(NoSuchTableException):
                self.catalog.load_table(self.table_identifier)

    def test_create_table_already_exists(self):
        with patch(
            "gravitino.utils.http_client.HTTPClient.post",
            side_effect=TableAlreadyExistsException("Table already exists"),
        ):
            with self.assertRaises(TableAlreadyExistsException):
                self.catalog.create_table(
                    identifier=self.table_identifier,
                    columns=DTOConverters.from_dtos(self.table_dto.columns()),
                )

    def test_list_tables_invalid_namespace(self):
        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=NoSuchSchemaException("Schema not found"),
        ):
            with self.assertRaises(NoSuchSchemaException):
                self.catalog.list_tables(namespace=Namespace.of("invalid_schema"))
