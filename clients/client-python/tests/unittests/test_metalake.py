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
from unittest.mock import MagicMock

from gravitino.api.catalog import Catalog
from gravitino.client.gravitino_client import GravitinoClient
from gravitino.api.catalog_change import CatalogChange
from gravitino.client.gravitino_metalake import GravitinoMetalake
from gravitino.constants.error import ErrorConstants
from gravitino.dto.metalake_dto import MetalakeDTO
from gravitino.dto.requests.catalog_update_request import CatalogUpdateRequest
from gravitino.dto.requests.catalog_updates_request import CatalogUpdatesRequest
from gravitino.dto.responses.metalake_response import MetalakeResponse
from gravitino.exceptions.base import (
    ConnectionFailedException,
    UnsupportedOperationException,
)
from gravitino.exceptions.handlers.catalog_error_handler import CATALOG_ERROR_HANDLER


class TestMetalake(unittest.TestCase):
    def test_list_catalogs_info_properties(self):
        for include_properties in (None, True, False):
            with self.subTest(include_properties=include_properties):
                rest_client = MagicMock()
                properties = {} if include_properties is False else {"in-use": "true"}
                rest_client.get.return_value.body = json.dumps(
                    {
                        "code": 0,
                        "catalogs": [
                            {
                                "name": "catalog",
                                "type": "relational",
                                "provider": "hive",
                                "comment": "test catalog",
                                "properties": properties,
                                "audit": {"creator": "tester"},
                            }
                        ],
                    }
                ).encode("utf-8")
                metalake = GravitinoMetalake(
                    MetalakeDTO("metalake/name", None, {}, None), rest_client
                )
                catalogs = (
                    metalake.list_catalogs_info()
                    if include_properties is None
                    else metalake.list_catalogs_info(
                        include_properties=include_properties
                    )
                )
                rest_client.get.assert_called_once_with(
                    "api/metalakes/metalake%2Fname/catalogs",
                    params={
                        "details": "true",
                        "includeProperties": (
                            "false" if include_properties is False else "true"
                        ),
                    },
                    error_handler=CATALOG_ERROR_HANDLER,
                )
                self.assertEqual(1, len(catalogs))
                catalog = catalogs[0]
                self.assertEqual("catalog", catalog.name())
                self.assertEqual(Catalog.Type.RELATIONAL, catalog.type())
                self.assertEqual("hive", catalog.provider())
                self.assertEqual("test catalog", catalog.comment())
                self.assertEqual(properties, catalog.properties())
                self.assertEqual("tester", catalog.audit_info().creator())

    def test_client_list_catalogs_info_delegation(self):
        client = MagicMock(spec=GravitinoClient)
        for include_properties in (None, True, False):
            with self.subTest(include_properties=include_properties):
                metalake = client.get_metalake.return_value
                metalake.reset_mock()
                catalogs = (
                    GravitinoClient.list_catalogs_info(client)
                    if include_properties is None
                    else GravitinoClient.list_catalogs_info(
                        client, include_properties=include_properties
                    )
                )
                metalake.list_catalogs_info.assert_called_once_with(
                    True if include_properties is None else include_properties
                )
                self.assertIs(metalake.list_catalogs_info.return_value, catalogs)

    def test_existing_catalog_connection(self):
        rest_client = MagicMock()
        rest_client.post.return_value.body = b'{"code":0}'
        metalake = GravitinoMetalake(
            MetalakeDTO("metalake", None, {}, None), rest_client
        )

        metalake.test_connection("catalog/name")

        rest_client.post.assert_called_once_with(
            "api/metalakes/metalake/catalogs/catalog%2Fname/testConnection",
            error_handler=CATALOG_ERROR_HANDLER,
        )

    def test_existing_catalog_connection_with_changes(self):
        rest_client = MagicMock()
        rest_client.post.return_value.body = b'{"code":0}'
        metalake = GravitinoMetalake(
            MetalakeDTO("metalake", None, {}, None), rest_client
        )

        metalake.test_connection("catalog", CatalogChange.set_property("key", "value"))

        expected_request = CatalogUpdatesRequest(
            [CatalogUpdateRequest.SetCatalogPropertyRequest("key", "value")]
        )
        rest_client.post.assert_called_once_with(
            "api/metalakes/metalake/catalogs/catalog/testConnection",
            json=expected_request,
            error_handler=CATALOG_ERROR_HANDLER,
        )

    def test_existing_catalog_connection_failure(self):
        rest_client = MagicMock()
        rest_client.post.return_value.body = (
            '{"code":%d,"type":"ConnectionFailedException",'
            '"message":"connection failed","stack":null}'
            % ErrorConstants.CONNECTION_FAILED_CODE.value
        ).encode("utf-8")
        metalake = GravitinoMetalake(
            MetalakeDTO("metalake", None, {}, None), rest_client
        )

        with self.assertRaisesRegex(ConnectionFailedException, "connection failed"):
            metalake.test_connection("catalog")

    def test_existing_catalog_connection_unsupported(self):
        rest_client = MagicMock()
        rest_client.post.return_value.body = (
            '{"code":%d,"type":"UnsupportedOperationException",'
            '"message":"unsupported","stack":null}'
            % ErrorConstants.UNSUPPORTED_OPERATION_CODE.value
        ).encode("utf-8")
        metalake = GravitinoMetalake(
            MetalakeDTO("metalake", None, {}, None), rest_client
        )

        with self.assertRaisesRegex(UnsupportedOperationException, "unsupported"):
            metalake.test_connection("catalog")

    def test_from_json_metalake_response(self):
        str_json = (
            b'{"code":0,"metalake":{"name":"example_name18","comment":"This is a sample comment",'
            b'"properties":{"key1":"value1","key2":"value2"},'
            b'"audit":{"creator":"anonymous","createTime":"2024-04-05T10:10:35.218Z"}}}'
        )
        metalake_response = MetalakeResponse.from_json(str_json, infer_missing=True)
        self.assertEqual(metalake_response.code(), 0)
        self.assertIsNotNone(metalake_response.metalake())
        self.assertEqual(metalake_response.metalake().name(), "example_name18")
        self.assertEqual(
            metalake_response.metalake().audit_info().creator(), "anonymous"
        )

    def test_from_error_json_metalake_response(self):
        str_json = (
            b'{"code":0, "undefined-key1":"undefined-value1", '
            b'"metalake":{"undefined-key2":1, "name":"example_name18","comment":"This is a sample comment",'
            b'"properties":{"key1":"value1","key2":"value2"},'
            b'"audit":{"creator":"anonymous","createTime":"2024-04-05T10:10:35.218Z"}}}'
        )
        metalake_response = MetalakeResponse.from_json(str_json, infer_missing=True)
        self.assertEqual(metalake_response.code(), 0)
        self.assertIsNotNone(metalake_response.metalake())
        self.assertEqual(metalake_response.metalake().name(), "example_name18")
        self.assertEqual(
            metalake_response.metalake().audit_info().creator(), "anonymous"
        )
