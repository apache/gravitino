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
from unittest.mock import patch

from gravitino.api.metadata_object import MetadataObject
from gravitino.api.metadata_objects import MetadataObjects
from gravitino.client.metadata_object_role_operations import (
    MetadataObjectRoleOperations,
)
from gravitino.dto.responses.error_response import ErrorResponse
from gravitino.dto.responses.name_list_response import NameListResponse
from gravitino.exceptions.base import (
    IllegalMetadataObjectException,
    IllegalPrivilegeException,
    NoSuchMetadataObjectException,
    NoSuchMetalakeException,
    NoSuchRoleException,
    RoleAlreadyExistsException,
)
from gravitino.exceptions.handlers.role_error_handler import ROLE_ERROR_HANDLER
from gravitino.utils import HTTPClient
from tests.unittests import mock_base


class TestMetadataObjectRoleOperations(unittest.TestCase):
    REST_CLIENT = HTTPClient("http://localhost:8090")
    METALAKE_NAME = "demo_metalake"

    def test_list_binding_role_names(self) -> None:
        expected_role_names_lst = ["role1", "role2"]
        metadata_object = MetadataObjects.of(
            ["catalog", "schema", "table"], MetadataObject.Type.TABLE
        )
        role_operations = MetadataObjectRoleOperations(
            TestMetadataObjectRoleOperations.METALAKE_NAME,
            metadata_object,
            TestMetadataObjectRoleOperations.REST_CLIENT,
        )
        role_names_list_resp = NameListResponse(
            0,
            expected_role_names_lst,
        )
        json_str = role_names_list_resp.to_json()
        mock_resp = mock_base.mock_http_response(json_str)

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ) as mock_get:
            retrieved_roles = role_operations.list_binding_role_names()
            self.assertEqual(expected_role_names_lst, retrieved_roles)
            mock_get.assert_called_once_with(
                "api/metalakes/demo_metalake/objects/table/catalog.schema.table/roles",
                params={},
                error_handler=ROLE_ERROR_HANDLER,
            )

    def test_role_error_handler(self) -> None:
        with self.assertRaises(IllegalPrivilegeException):
            ROLE_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    IllegalPrivilegeException,  # type: ignore
                    "mock error",
                )
            )

        with self.assertRaises(IllegalMetadataObjectException):
            ROLE_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    IllegalMetadataObjectException,  # type: ignore
                    "mock error",
                )
            )

        with self.assertRaises(NoSuchMetalakeException):
            ROLE_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    NoSuchMetalakeException,  # type: ignore
                    "mock error",
                )
            )

        with self.assertRaises(NoSuchRoleException):
            ROLE_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    NoSuchRoleException,  # type: ignore
                    "mock error",
                )
            )

        with self.assertRaises(NoSuchMetadataObjectException):
            ROLE_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    NoSuchMetadataObjectException,  # type: ignore
                    "mock error",
                )
            )

        with self.assertRaises(RoleAlreadyExistsException):
            ROLE_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    RoleAlreadyExistsException,  # type: ignore
                    "mock error",
                )
            )
