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
from decimal import Decimal
from uuid import uuid4

import simplejson

from gravitino import Catalog, GravitinoAdminClient, NameIdentifier
from gravitino.api.rel.types.types import Types
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.exceptions.base import (
    IllegalArgumentException,
    IllegalSemanticModelException,
    NoSuchSchemaException,
    NoSuchSemanticModelException,
    SemanticModelAlreadyExistsException,
)
from gravitino.namespace import Namespace
from tests.integration.integration_test_env import IntegrationTestEnv


class TestSemanticModelOssie(IntegrationTestEnv):
    """Exercise Ossie documents through the public SDK and a real server."""

    def setUp(self):
        super().setUp()
        admin = GravitinoAdminClient(uri="http://localhost:8090")
        self.addCleanup(admin.close)
        metalake_name = "ossie_it_" + uuid4().hex
        client = self.create_metalake_client(admin, metalake_name)
        self.addCleanup(client.close)
        self.addCleanup(admin.drop_metalake, metalake_name, force=True)
        self.catalog = client.create_catalog(
            "ossie_catalog", Catalog.Type.RELATIONAL, "lakehouse-generic", "", {}
        )
        self.catalog.as_schemas().create_schema("source", "", {})
        self.catalog.as_table_catalog().create_table(
            NameIdentifier.of("source", "orders"),
            [
                ColumnDTO.builder()
                .with_name("order_id")
                .with_data_type(Types.StringType.get())
                .build()
            ],
            properties={
                "format": "delta",
                "external": "true",
                "location": f"file:///tmp/{metalake_name}/orders",
            },
        )
        self.models = self.catalog.as_semantic_model_catalog()
        self.schema = "models_" + uuid4().hex
        self.catalog.as_schemas().create_schema(self.schema, "", {})
        self.addCleanup(self.catalog.as_schemas().drop_schema, self.schema, True)
        self.namespace = Namespace.of(self.schema)
        self.identifier = NameIdentifier.of(self.schema, "sales")

    @staticmethod
    def _yaml_document():
        return """version: 0.2.0.dev0
name: sales
description: Sales definitions - café 雪
ai_context:
  instructions: Use certified definitions
  nested:
    precise: 0.12345678901234567890123456789
    large: 1234567890123456789012345678901234567890
    values: [true, null, extra]
datasets:
  - name: orders
    source: ossie_catalog.source.orders
    primary_key: [order_id]
    fields:
      - name: order_id
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: orders.order_id
        datatype: String
        ai_context: Order identifier
metrics:
  - name: order_count
    expression:
      dialects:
        - dialect: ANSI_SQL
          expression: COUNT(orders.order_id)
    datatype: Integer
custom_extensions:
  - vendor_name: EXAMPLE
    data: '{"certified":true}'
  - vendor_name: GRAVITINO_PROPERTIES
    data: '{"domain":"sales"}'
"""

    @staticmethod
    def _json_document():
        return simplejson.dumps(
            {
                "version": "0.2.0.dev0",
                "name": "sales",
                "description": "Sales definitions - café 雪",
                "ai_context": {
                    "instructions": "Use certified definitions",
                    "nested": {
                        "precise": Decimal("0.12345678901234567890123456789"),
                        "large": 1234567890123456789012345678901234567890,
                        "values": [True, None, "extra"],
                    },
                },
                "datasets": [
                    {
                        "name": "orders",
                        "source": "ossie_catalog.source.orders",
                        "primary_key": ["order_id"],
                        "fields": [
                            {
                                "name": "order_id",
                                "datatype": "String",
                                "expression": {
                                    "dialects": [
                                        {
                                            "dialect": "ANSI_SQL",
                                            "expression": "orders.order_id",
                                        }
                                    ]
                                },
                                "ai_context": "Order identifier",
                            }
                        ],
                    }
                ],
                "metrics": [
                    {
                        "name": "order_count",
                        "datatype": "Integer",
                        "expression": {
                            "dialects": [
                                {
                                    "dialect": "ANSI_SQL",
                                    "expression": "COUNT(orders.order_id)",
                                }
                            ]
                        },
                    }
                ],
                "custom_extensions": [
                    {"vendor_name": "EXAMPLE", "data": '{"certified":true}'},
                    {
                        "vendor_name": "GRAVITINO_PROPERTIES",
                        "data": '{"domain":"sales"}',
                    },
                ],
            },
            use_decimal=True,
            ensure_ascii=False,
        )

    def _assert_model(self, model):
        self.assertEqual("sales", model.name())
        self.assertEqual("Sales definitions - café 雪", model.comment())
        self.assertEqual("sales", model.properties()["domain"])
        self.assertEqual("0.2.0.dev0", model.properties()["ossie-version"])
        definition = model.definition()
        self.assertEqual(
            "Use certified definitions", definition.ai_context().object().instructions()
        )
        self.assertEqual('{"certified":true}', definition.custom_extensions()[0].data())
        self.assertEqual(
            NameIdentifier.of("ossie_catalog", "source", "orders"),
            definition.datasets()[0].source(),
        )
        self.assertEqual("order_id", definition.datasets()[0].fields()[0].name())
        self.assertEqual("order_count", definition.metrics()[0].name())
        self.assertEqual("EXAMPLE", definition.custom_extensions()[0].vendor_name())
        self.assertEqual(
            {
                "nested": {
                    "precise": Decimal("0.12345678901234567890123456789"),
                    "large": 1234567890123456789012345678901234567890,
                    "values": [True, None, "extra"],
                }
            },
            definition.ai_context().object().additional_properties(),
        )

    def _round_trip(self, content, document_format):
        imported = self.models.import_ossie_document(
            self.namespace, content, document_format
        )
        self._assert_model(imported)
        self._assert_model(self.models.load_semantic_model(self.identifier))
        for output_format in ("yaml", "json"):
            with self.subTest(output_format=output_format):
                exported = self.models.export_ossie_document(
                    self.identifier, output_format
                )
                self.assertIsInstance(exported, str)
                if output_format == "json":
                    document = json.loads(exported, parse_float=Decimal)
                    self.assertEqual("sales", document["name"])
                    self.assertEqual(
                        "ossie_catalog.source.orders", document["datasets"][0]["source"]
                    )
                    self.assertEqual(
                        Decimal("0.12345678901234567890123456789"),
                        document["ai_context"]["nested"]["precise"],
                    )
                self.assertTrue(self.models.drop_semantic_model(self.identifier))
                restored = self.models.import_ossie_document(
                    self.namespace, exported, output_format
                )
                self._assert_model(restored)
                self.assertEqual(imported.definition(), restored.definition())

    def test_yaml_round_trip(self):
        self._round_trip(self._yaml_document(), "yaml")

    def test_json_round_trip(self):
        self._round_trip(self._json_document(), "json")

    def test_default_format_is_yaml(self):
        imported = self.models.import_ossie_document(
            self.namespace, self._yaml_document()
        )
        self._assert_model(imported)
        exported = self.models.export_ossie_document(self.identifier)
        self.assertIn("datasets:", exported)
        self.models.drop_semantic_model(self.identifier)
        self._assert_model(self.models.import_ossie_document(self.namespace, exported))

    def test_conflicting_import_does_not_replace_model(self):
        original = self.models.import_ossie_document(
            self.namespace, self._yaml_document()
        )
        for content, document_format in (
            (self._yaml_document(), "yaml"),
            (self._json_document(), "json"),
        ):
            with self.subTest(document_format=document_format):
                with self.assertRaises(SemanticModelAlreadyExistsException):
                    self.models.import_ossie_document(
                        self.namespace,
                        content.replace("Sales definitions", "Changed"),
                        document_format,
                    )
                loaded = self.models.load_semantic_model(self.identifier)
                self.assertEqual(original.definition(), loaded.definition())
                self.assertEqual(original.comment(), loaded.comment())

    def test_malformed_documents_are_rejected_without_creating_models(self):
        for content, document_format in (
            ("name: [", "yaml"),
            ("{", "json"),
            (self._yaml_document(), "json"),
            ('{"name":"sales","datasets":[]}', "json"),
        ):
            with self.subTest(document_format=document_format, content=content):
                with self.assertRaises(IllegalSemanticModelException):
                    self.models.import_ossie_document(
                        self.namespace, content, document_format
                    )
                self.assertEqual([], self.models.list_semantic_models(self.namespace))
        with self.assertRaises(IllegalArgumentException):
            self.models.import_ossie_document(self.namespace, " ")

    def test_missing_resources(self):
        for content, document_format in (
            (self._yaml_document(), "yaml"),
            (self._json_document(), "json"),
        ):
            with self.subTest(document_format=document_format):
                with self.assertRaises(NoSuchSchemaException):
                    self.models.import_ossie_document(
                        Namespace.of("missing"), content, document_format
                    )
                with self.assertRaises(NoSuchSemanticModelException):
                    self.models.export_ossie_document(self.identifier, document_format)
                with self.assertRaises(IllegalSemanticModelException):
                    self.models.import_ossie_document(
                        self.namespace,
                        content.replace("source.orders", "source.missing"),
                        document_format,
                    )
                self.assertEqual([], self.models.list_semantic_models(self.namespace))
