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
from unittest.mock import MagicMock

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
