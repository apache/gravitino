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

from __future__ import annotations

import unittest
from unittest.mock import patch

from gravitino import GravitinoClient
from gravitino.api.authorization.owner import Owner
from gravitino.api.metadata_object import MetadataObject
from gravitino.dto.authorization.owner_dto import OwnerDTO
from gravitino.dto.metadata_object_dto import MetadataObjectDTO
from gravitino.dto.requests.owner_set_request import OwnerSetRequest
from gravitino.dto.responses.error_response import ErrorResponse
from gravitino.dto.responses.owner_response import OwnerResponse
from gravitino.dto.responses.set_response import SetResponse
from gravitino.exceptions.base import (
    IllegalArgumentException,
    MetalakeNotInUseException,
    NoSuchMetadataObjectException,
    NotFoundException,
    UnsupportedOperationException,
)
from gravitino.exceptions.handlers.owner_error_handler import OWNER_ERROR_HANDLER
from tests.unittests import mock_base


@mock_base.mock_data
class TestOwner(unittest.TestCase):
    _metalake_name: str = "metalake_demo"

    def test_get_owner(self, *mock_method) -> None:
        owner_resp = OwnerResponse(0, {"name": "alice", "type": "USER"})
        json_str = owner_resp.to_json()
        mock_resp = mock_base.mock_http_response(json_str)
        client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
            check_version=False,
        )

        metadata_object = (
            MetadataObjectDTO.builder()
            .type(MetadataObject.Type.CATALOG)
            .full_name("test_catalog")
            .build()
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ):
            owner = client.get_owner(metadata_object)
            self.assertIsNotNone(owner)
            self.assertEqual("alice", owner.name())
            self.assertEqual(Owner.Type.USER, owner.type())

    def test_get_owner_none(self, *mock_method) -> None:
        owner_resp = OwnerResponse(0, None)
        json_str = owner_resp.to_json()
        mock_resp = mock_base.mock_http_response(json_str)
        client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
            check_version=False,
        )

        metadata_object = (
            MetadataObjectDTO.builder()
            .type(MetadataObject.Type.CATALOG)
            .full_name("test_catalog")
            .build()
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ):
            owner = client.get_owner(metadata_object)
            self.assertIsNone(owner)

    def test_set_owner(self, *mock_method) -> None:
        set_resp = SetResponse(0, True)
        json_str = set_resp.to_json()
        mock_resp = mock_base.mock_http_response(json_str)
        client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
            check_version=False,
        )

        metadata_object = (
            MetadataObjectDTO.builder()
            .type(MetadataObject.Type.CATALOG)
            .full_name("test_catalog")
            .build()
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.put",
            return_value=mock_resp,
        ) as mock_put:
            client.set_owner(metadata_object, "alice", Owner.Type.USER)
            mock_put.assert_called_once()

    def test_set_owner_returns_false(self, *mock_method) -> None:
        set_resp = SetResponse(0, False)
        json_str = set_resp.to_json()
        mock_resp = mock_base.mock_http_response(json_str)
        client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
            check_version=False,
        )

        metadata_object = (
            MetadataObjectDTO.builder()
            .type(MetadataObject.Type.CATALOG)
            .full_name("test_catalog")
            .build()
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.put",
            return_value=mock_resp,
        ) as mock_put:
            client.set_owner(metadata_object, "alice", Owner.Type.USER)
            mock_put.assert_called_once()

    def test_get_owner_with_group(self, *mock_method) -> None:
        owner_resp = OwnerResponse(0, {"name": "admin_group", "type": "GROUP"})
        json_str = owner_resp.to_json()
        mock_resp = mock_base.mock_http_response(json_str)
        client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
            check_version=False,
        )

        metadata_object = (
            MetadataObjectDTO.builder()
            .type(MetadataObject.Type.SCHEMA)
            .full_name("test_catalog.test_schema")
            .build()
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ):
            owner = client.get_owner(metadata_object)
            self.assertIsNotNone(owner)
            self.assertEqual("admin_group", owner.name())
            self.assertEqual(Owner.Type.GROUP, owner.type())


class TestOwnerErrorHandler(unittest.TestCase):
    def test_illegal_arguments(self):
        with self.assertRaises(IllegalArgumentException):
            OWNER_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    IllegalArgumentException, "mock error"
                )
            )

    def test_not_found_metadata_object(self):
        with self.assertRaises(NoSuchMetadataObjectException):
            OWNER_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    NoSuchMetadataObjectException, "mock error"
                )
            )

    def test_not_found_generic(self):
        with self.assertRaises(NotFoundException):
            OWNER_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(NotFoundException, "mock error")
            )

    def test_unsupported_operation(self):
        with self.assertRaises(UnsupportedOperationException):
            OWNER_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    UnsupportedOperationException, "mock error"
                )
            )

    def test_not_in_use(self):
        with self.assertRaises(MetalakeNotInUseException):
            OWNER_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    MetalakeNotInUseException, "mock error"
                )
            )

    def test_internal_error(self):
        with self.assertRaises(RuntimeError):
            OWNER_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(RuntimeError, "mock error")
            )


class TestOwnerSetRequestValidation(unittest.TestCase):
    def test_validate_empty_name(self):
        req = OwnerSetRequest.__new__(OwnerSetRequest)
        req._name = ""
        req._type = Owner.Type.USER
        with self.assertRaises(ValueError):
            req.validate()

    def test_validate_none_type(self):
        req = OwnerSetRequest.__new__(OwnerSetRequest)
        req._name = "alice"
        req._type = None
        with self.assertRaises(ValueError):
            req.validate()

    def test_validate_success(self):
        req = OwnerSetRequest("alice", Owner.Type.USER)
        req.validate()


class TestOwnerResponseValidation(unittest.TestCase):
    def test_validate_owner_with_empty_name(self):
        owner_dto = OwnerDTO(_name="", _type=Owner.Type.USER)
        resp = OwnerResponse(0, owner_dto)
        with self.assertRaises(ValueError):
            resp.validate()

    def test_validate_owner_with_none_type(self):
        owner_dto = OwnerDTO(_name="alice", _type=None)
        resp = OwnerResponse(0, owner_dto)
        with self.assertRaises(ValueError):
            resp.validate()

    def test_validate_no_owner(self):
        resp = OwnerResponse(0, None)
        resp.validate()
