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

import io
import json
import unittest
from decimal import Decimal
from unittest.mock import Mock, patch
from urllib.error import HTTPError

from gravitino.api.semantic.semantic_model_catalog import SemanticModelCatalog
from gravitino.client.relational_catalog import RelationalCatalog
from gravitino.dto.audit_dto import AuditDTO
from gravitino.exceptions.base import (
    CatalogNotInUseException,
    ConnectionFailedException,
    ForbiddenException,
    IllegalArgumentException,
    IllegalNameIdentifierException,
    IllegalNamespaceException,
    IllegalSemanticModelException,
    InternalError,
    MetalakeNotInUseException,
    NoSuchCatalogException,
    NoSuchMetalakeException,
    NoSuchSchemaException,
    NoSuchSemanticModelException,
    SemanticModelAlreadyExistsException,
    UnauthorizedException,
    UnsupportedOperationException,
)
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace
from gravitino.utils.http_client import HTTPClient


class TestSemanticModelOssie(unittest.TestCase):
    def setUp(self):
        self.client = HTTPClient("http://localhost:8090")
        self.models = RelationalCatalog(
            catalog_namespace=Namespace.of("metalake /雪"),
            name="catalog +%",
            catalog_type=RelationalCatalog.Type.RELATIONAL,
            provider="lakehouse-generic",
            audit=AuditDTO("test"),
            rest_client=self.client,
        ).as_semantic_model_catalog()
        self.namespace = Namespace.of("schema #?")
        self.identifier = NameIdentifier.of("schema #?", "sales /é+%")
        self.path = (
            "http://localhost:8090/api/metalakes/metalake%20%2F%E9%9B%AA"
            "/catalogs/catalog%20%2B%25/schemas/schema%20%23%3F/semantic-models"
        )
        self.response_body = b"""{"code":0,"semanticModel":{
            "name":"sales", "comment":"comment", "properties":{"domain":"sales"},
            "audit":{"creator":"test","createTime":"2026-01-01T00:00:00Z"},
            "definition":{
                "datasets":[{"name":"orders","source":{"namespace":["catalog","schema"],"name":"orders"}}],
                "aiContext":{"nested":{"precise":0.12345678901234567890123456789,
                    "large":1234567890123456789012345678901234567890,"values":[true,null,"extra"]}}
            }
        }}"""
        opener_patch = patch("gravitino.utils.http_client.build_opener")
        self.opener = opener_patch.start().return_value
        self.addCleanup(opener_patch.stop)
        self.opener.open.return_value.getcode.return_value = 200
        self.opener.open.return_value.read.return_value = self.response_body

    def test_import_raw_text_and_lossless_response(self):
        self.assertIsInstance(self.models, SemanticModelCatalog)
        for document_format, content in (
            ("yaml", "name: café 雪\nnumber: 0.12345678901234567890\n"),
            ("json", '{"name":"café 雪","number":0.12345678901234567890}'),
        ):
            with self.subTest(document_format=document_format):
                model = self.models.import_ossie_document(
                    self.namespace, content, document_format
                )
                request = self.opener.open.call_args.args[0]
                self.assertEqual(self.path + "/ossie", request.full_url)
                self.assertEqual("POST", request.get_method())
                self.assertEqual(content.encode("utf-8"), request.data)
                self.assertEqual(
                    "application/" + document_format, request.get_header("Content-type")
                )
                self.assertEqual(
                    "application/vnd.gravitino.v1+json", request.get_header("Accept")
                )
                self.assertEqual("sales", model.name())
                self.assertEqual("comment", model.comment())
                self.assertEqual({"domain": "sales"}, model.properties())
                self.assertEqual("test", model.audit_info().creator())
                self.assertEqual(
                    {
                        "nested": {
                            "precise": Decimal("0.12345678901234567890123456789"),
                            "large": 1234567890123456789012345678901234567890,
                            "values": [True, None, "extra"],
                        }
                    },
                    model.definition().ai_context().object().additional_properties(),
                )

    def test_export_returns_document_text_unchanged(self):
        for document_format, content in (
            ("yaml", "---\nname: café 雪\nnumber: 0.12345678901234567890\n"),
            ("json", '{ "name": "café 雪", "number": 0.12345678901234567890 }\n'),
        ):
            with self.subTest(document_format=document_format):
                self.opener.open.return_value.read.return_value = content.encode(
                    "utf-8"
                )
                exported = self.models.export_ossie_document(
                    self.identifier, document_format
                )
                request = self.opener.open.call_args.args[0]
                self.assertEqual(
                    self.path
                    + "/sales%20%2F%C3%A9%2B%25/ossie?format="
                    + document_format,
                    request.full_url,
                )
                self.assertEqual("GET", request.get_method())
                self.assertIsNone(request.data)
                self.assertEqual(
                    "application/" + document_format, request.get_header("Accept")
                )
                self.assertEqual(content, exported)

    def test_default_format_and_headers_do_not_leak(self):
        self.models.import_ossie_document(self.namespace, "name: sales")
        self.assertEqual(
            "application/yaml",
            self.opener.open.call_args.args[0].get_header("Content-type"),
        )
        self.models.export_ossie_document(self.identifier)
        self.assertTrue(
            self.opener.open.call_args.args[0].full_url.endswith("?format=yaml")
        )
        self.assertEqual(
            "application/yaml", self.opener.open.call_args.args[0].get_header("Accept")
        )
        self.models.load_semantic_model(self.identifier)
        self.assertEqual(
            "application/vnd.gravitino.v1+json",
            self.opener.open.call_args.args[0].get_header("Accept"),
        )
        self.assertEqual(
            "application/json",
            self.opener.open.call_args.args[0].get_header("Content-type"),
        )

    def test_invalid_local_arguments_do_not_send_requests(self):
        for content in (None, "", " \t\n", b"name: sales", {"name": "sales"}):
            with self.subTest(content=content), self.assertRaises(
                IllegalArgumentException
            ):
                self.models.import_ossie_document(self.namespace, content)
        for document_format in (None, "", "csv", "YAML", 1, []):
            with self.subTest(document_format=document_format):
                with self.assertRaises(IllegalArgumentException):
                    self.models.import_ossie_document(
                        self.namespace, "name: sales", document_format
                    )
                with self.assertRaises(IllegalArgumentException):
                    self.models.export_ossie_document(self.identifier, document_format)
        for namespace in (None, Namespace.empty(), Namespace.of("catalog", "schema")):
            with self.subTest(namespace=namespace), self.assertRaises(
                IllegalNamespaceException
            ):
                self.models.import_ossie_document(namespace, "name: sales")
        with self.assertRaises(IllegalNameIdentifierException):
            self.models.export_ossie_document(None)
        with self.assertRaises(IllegalNamespaceException):
            self.models.export_ossie_document(NameIdentifier.of("model"))
        self.opener.open.assert_not_called()

    def test_import_validates_native_response(self):
        for body, message in (
            (b'{"code":0,"semanticModel":null}', "semanticModel must not be null"),
            (
                self.response_body.replace(b'"code":0', b'"code":-1'),
                "code must be >= 0",
            ),
        ):
            with self.subTest(body=body):
                self.opener.open.return_value.read.return_value = body
                with self.assertRaisesRegex(IllegalArgumentException, message):
                    self.models.import_ossie_document(self.namespace, "name: sales")

    def test_http_error_envelopes_map_to_sdk_exceptions(self):
        cases = (
            (400, 1001, IllegalSemanticModelException),
            (400, 1001, IllegalArgumentException),
            (404, 1003, NoSuchMetalakeException),
            (404, 1003, NoSuchCatalogException),
            (404, 1003, NoSuchSchemaException),
            (404, 1003, NoSuchSemanticModelException),
            (409, 1004, SemanticModelAlreadyExistsException),
            (401, 1011, UnauthorizedException),
            (403, 1008, ForbiddenException),
            (409, 1009, CatalogNotInUseException),
            (409, 1009, MetalakeNotInUseException),
            (500, 1002, InternalError),
            (501, 1006, UnsupportedOperationException),
            (502, 1007, ConnectionFailedException),
        )
        for operation in (
            lambda: self.models.import_ossie_document(self.namespace, "name: sales"),
            lambda: self.models.export_ossie_document(self.identifier),
        ):
            for status, code, exception in cases:
                with self.subTest(operation=operation, exception=exception):
                    body = json.dumps(
                        {
                            "code": code,
                            "type": exception.__name__,
                            "message": "server detail",
                        }
                    ).encode()
                    self.opener.open.side_effect = HTTPError(
                        self.path, status, "error", {}, io.BytesIO(body)
                    )
                    with self.assertRaisesRegex(exception, "server detail"):
                        operation()

    def test_base_api_defaults_preserve_existing_implementations(self):
        for method, args in (
            (
                SemanticModelCatalog.import_ossie_document,
                (self.namespace, "name: sales"),
            ),
            (SemanticModelCatalog.export_ossie_document, (self.identifier,)),
        ):
            with self.assertRaises(UnsupportedOperationException):
                method(Mock(spec=SemanticModelCatalog), *args)
