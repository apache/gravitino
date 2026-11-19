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
from dataclasses import replace
from io import BytesIO
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError

from gravitino.api.rel.table import Table
from gravitino.api.rel.view import View
from gravitino.client.generic_view import GenericView
from gravitino.client.relational_table import RelationalTable
from gravitino.constants.error import ErrorConstants
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.rel.sql_representation_dto import SQLRepresentationDTO
from gravitino.dto.rel.table_dto import TableDTO
from gravitino.dto.rel.view_dto import ViewDTO
from gravitino.exceptions.base import (
    ForbiddenException,
    IllegalArgumentException,
    InternalError,
    NoSuchMetadataObjectException,
    NotFoundException,
    UnsupportedOperationException,
)
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace
from gravitino.utils import HTTPClient
from tests.unittests.fixtures.table_fixtures import TABLE_DTO_JSON_STRING


class TestMetadataObjectSemanticModelOperations(unittest.TestCase):
    def setUp(self):
        client = HTTPClient("http://localhost:8090")
        self.addCleanup(client.close)
        namespace = Namespace.of("metalake /%? 测试", "catalog", "schema")
        source_name = "orders /%? 明细"
        table = replace(TableDTO.from_json(TABLE_DTO_JSON_STRING), _name=source_name)
        self.sources = {
            "table": RelationalTable(namespace, table, client),
            "view": GenericView(
                ViewDTO(
                    _name=source_name,
                    _audit=AuditDTO("creator"),
                    _representations=[
                        SQLRepresentationDTO(_dialect="trino", _sql="SELECT 1")
                    ],
                ),
                client,
                namespace,
            ),
        }
        opener_patch = patch("gravitino.utils.http_client.build_opener")
        self.opener = opener_patch.start().return_value
        self.addCleanup(opener_patch.stop)
        self.opener.open.return_value.getcode.return_value = 200

    def test_one_get_preserves_full_identifiers_and_server_order(self):
        model_namespace = ["metalake /%? 测试", "another_catalog", "schema", "a"]
        identifiers = [
            {"namespace": model_namespace, "name": name}
            for name in ("Orders.raw", "orders.raw")
        ] + [
            {"namespace": model_namespace[:-1] + ["a-b"], "name": name}
            # Supplementary characters precede U+E000 in UTF-16 order.
            for name in ("收入", "\U00010000", "\ue000")
        ]
        expected = [
            NameIdentifier.of(*item["namespace"], item["name"]) for item in identifiers
        ]
        for source_type, source in self.sources.items():
            with self.subTest(source=source_type):
                self._respond({"code": 0, "identifiers": identifiers})
                actual = (
                    source.supports_semantic_models().list_semantic_model_datasets()
                )
                self.assertEqual(expected, actual)
                self.assertEqual(4, actual[0].namespace().length())
                self.assertEqual("Orders.raw", actual[0].name())
                self.assertEqual(
                    NameIdentifier.of(*model_namespace),
                    NameIdentifier.of(*actual[0].namespace().levels()),
                )
                self._assert_request(source_type)

    def test_empty_results(self):
        for source_type, source in self.sources.items():
            with self.subTest(source=source_type):
                self._respond({"code": 0, "identifiers": []})
                self.assertEqual(
                    [], source.supports_semantic_models().list_semantic_model_datasets()
                )
                self._assert_request(source_type)

    def test_http_errors_are_not_empty_results(self):
        cases = (
            (404, ErrorConstants.NOT_FOUND_CODE, NoSuchMetadataObjectException),
            (404, ErrorConstants.NOT_FOUND_CODE, NotFoundException),
            (400, ErrorConstants.ILLEGAL_ARGUMENTS_CODE, IllegalArgumentException),
            (403, ErrorConstants.FORBIDDEN_CODE, ForbiddenException),
            (500, ErrorConstants.INTERNAL_ERROR_CODE, InternalError),
        )
        for source_type, source in self.sources.items():
            for status, code, exception_type in cases:
                with self.subTest(source=source_type, error=exception_type):
                    self.opener.open.reset_mock()
                    payload = json.dumps(
                        {
                            "code": code,
                            "type": exception_type.__name__,
                            "message": "lookup failed",
                        }
                    ).encode("utf-8")
                    self.opener.open.side_effect = HTTPError(
                        "http://localhost:8090",
                        status,
                        "lookup failed",
                        {},
                        BytesIO(payload),
                    )
                    with self.assertRaises(exception_type) as raised:
                        source.supports_semantic_models().list_semantic_model_datasets()
                    self.assertIs(exception_type, type(raised.exception))
                    self.assertIn("lookup failed", str(raised.exception))
                    self._assert_request(source_type)

    def test_transport_failures_are_not_empty_results(self):
        for source_type, source in self.sources.items():
            with self.subTest(source=source_type):
                self.opener.open.reset_mock()
                failure = URLError("source lookup unavailable")
                self.opener.open.side_effect = failure
                with self.assertRaises(URLError) as raised:
                    source.supports_semantic_models().list_semantic_model_datasets()
                self.assertIs(failure, raised.exception)
                self._assert_request(source_type)

    def test_invalid_response_is_rejected(self):
        for source_type, source in self.sources.items():
            for payload in ({"code": 0}, {"code": 0, "identifiers": None}):
                with self.subTest(source=source_type, payload=payload):
                    self._respond(payload)
                    with self.assertRaisesRegex(
                        IllegalArgumentException, "identifiers must not be null"
                    ):
                        source.supports_semantic_models().list_semantic_model_datasets()
                    self._assert_request(source_type)

    def test_default_capabilities_remain_optional(self):
        with self.assertRaises(UnsupportedOperationException):
            Table.supports_semantic_models(Mock())
        with self.assertRaises(UnsupportedOperationException):
            View.supports_semantic_models(Mock())

    def _respond(self, body):
        self.opener.open.reset_mock()
        self.opener.open.side_effect = None
        self.opener.open.return_value.read.return_value = json.dumps(body).encode(
            "utf-8"
        )

    def _assert_request(self, source_type):
        self.opener.open.assert_called_once()
        request = self.opener.open.call_args.args[0]
        self.assertEqual("GET", request.get_method())
        self.assertEqual(
            "http://localhost:8090/api/metalakes/"
            "metalake%20%2F%25%3F%20%E6%B5%8B%E8%AF%95/objects/"
            + source_type
            + "/catalog.schema.orders%20%2F%25%3F%20%E6%98%8E%E7%BB%86"
            "/semantic-model-datasets",
            request.full_url,
        )
