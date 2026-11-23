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

import shutil
import tempfile
from uuid import uuid4

import requests

from gravitino import Catalog, GravitinoAdminClient, NameIdentifier
from gravitino.api.rel.column import Column
from gravitino.api.rel.sql_representation import SQLRepresentation
from gravitino.api.rel.table_change import TableChange
from gravitino.api.rel.types.types import Types
from gravitino.api.rel.view_change import ViewChange
from gravitino.api.semantic.dataset import Dataset
from gravitino.api.semantic.semantic_model_change import SemanticModelChange
from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition
from gravitino.exceptions.base import NoSuchMetadataObjectException
from tests.integration.integration_test_env import IntegrationTestEnv


class TestSemanticModelSourceLookup(IntegrationTestEnv):
    """Exercise the source capability and REST contract against a real server."""

    def setUp(self):
        super().setUp()
        self.location = tempfile.mkdtemp(prefix="source_lookup_")
        self.addCleanup(shutil.rmtree, self.location)
        admin = GravitinoAdminClient(uri="http://localhost:8090")
        self.addCleanup(admin.close)
        self.metalake = "source_lookup_" + uuid4().hex
        self.client = self.create_metalake_client(admin, self.metalake)
        self.addCleanup(self.client.close)
        self.addCleanup(admin.drop_metalake, self.metalake, force=True)
        self.source_catalog = self.client.create_catalog(
            "sources",
            Catalog.Type.RELATIONAL,
            "lakehouse-iceberg",
            "",
            {
                "catalog-backend": "jdbc",
                "warehouse": self.location,
                "uri": f"jdbc:h2:mem:{self.metalake};DB_CLOSE_DELAY=-1;MODE=MYSQL",
                "jdbc-driver": "org.h2.Driver",
                "jdbc-initialize": "true",
            },
        )
        self.source_catalog.as_schemas().create_schema("raw", "", {})
        self.model_catalog = self.client.create_catalog(
            "models", Catalog.Type.RELATIONAL, "lakehouse-generic", "", {}
        )
        for schema in ("semantic", "other"):
            self.model_catalog.as_schemas().create_schema(schema, "", {})
        self.models = self.model_catalog.as_semantic_model_catalog()
        self.table = self._create_table("orders")

    def test_cross_catalog_identifiers_order_and_rest_parity(self):
        aliases = [
            "orders.raw",
            "Orders.raw",
            "dataset_" + "x" * 200,
            "\U00010000",
            "\ue000",
        ]
        expected_aliases = [aliases[1], aliases[2], aliases[0], aliases[3], aliases[4]]
        definition = self._definition("orders", *aliases)
        for schema, model in (("semantic", "a-b"), ("semantic", "a"), ("other", "a")):
            self.models.create_semantic_model(
                NameIdentifier.of(schema, model), "", definition, {}
            )
        expected = [
            self._dataset_id(schema, model, alias)
            for schema, model in (
                ("other", "a"),
                ("semantic", "a"),
                ("semantic", "a-b"),
            )
            for alias in expected_aliases
        ]
        self.assertEqual(expected, self._lookup(self.table))
        with requests.get(
            f"http://localhost:8090/api/metalakes/{self.metalake}/objects/"
            "table/sources.raw.orders/semantic-model-datasets",
            timeout=10,
        ) as response:
            response.raise_for_status()
            self.assertEqual(
                {
                    "code": 0,
                    "identifiers": [
                        {"namespace": item.namespace().levels(), "name": item.name()}
                        for item in expected
                    ],
                },
                response.json(),
            )
        for dataset in expected:
            model = NameIdentifier.of(*dataset.namespace().levels())
            catalog = self.client.load_catalog(model.namespace().level(1))
            loaded = catalog.as_semantic_model_catalog().load_semantic_model(
                NameIdentifier.of(model.namespace().level(2), model.name())
            )
            self.assertIn(
                dataset.name(), [item.name() for item in loaded.definition().datasets()]
            )
        self.model_catalog.as_schemas().drop_schema("other", True)
        self.assertEqual(expected[5:], self._lookup(self.table))
        self.client.drop_catalog("models", True)
        self.assertEqual([], self._lookup(self.table))

    def test_source_rename_recreation_and_definition_rebinding(self):
        identifier = NameIdentifier.of("semantic", "sales")
        definition = self._definition("orders", "orders.raw")
        self.assertEqual([], self._lookup(self.table))
        self.models.create_semantic_model(identifier, "", definition, {})
        expected = [self._dataset_id("semantic", "sales", "orders.raw")]
        self.assertEqual(expected, self._lookup(self.table))
        tables = self.source_catalog.as_table_catalog()
        renamed = tables.alter_table(
            NameIdentifier.of("raw", "orders"), TableChange.rename("orders_v2")
        )
        with self.assertRaises(NoSuchMetadataObjectException):
            self._lookup(self.table)
        self.assertEqual(expected, self._lookup(renamed))
        self.assertEqual(
            "orders",
            self.models.load_semantic_model(identifier)
            .definition()
            .datasets()[0]
            .source()
            .name(),
        )
        self.models.alter_semantic_model(
            identifier, SemanticModelChange.rename("renamed")
        )
        identifier = NameIdentifier.of("semantic", "renamed")
        expected = [self._dataset_id("semantic", "renamed", "orders.raw")]
        self.assertEqual(expected, self._lookup(renamed))
        replacement = self._definition("orders_v2", "orders.raw")
        self.models.alter_semantic_model(
            identifier, SemanticModelChange.replace_definition(replacement)
        )
        tables.drop_table(NameIdentifier.of("raw", "orders_v2"))
        with self.assertRaises(NoSuchMetadataObjectException):
            self._lookup(renamed)
        recreated = self._create_table("orders_v2")
        self.assertEqual([], self._lookup(recreated))
        self.models.alter_semantic_model(
            identifier, SemanticModelChange.set_property("reviewed", "true")
        )
        self.assertEqual([], self._lookup(recreated))
        self.models.alter_semantic_model(
            identifier, SemanticModelChange.replace_definition(replacement)
        )
        self.assertEqual(expected, self._lookup(recreated))
        self.models.drop_semantic_model(identifier)
        self.assertEqual([], self._lookup(recreated))

    def test_view_lookup_is_direct_and_tracks_source_identity(self):
        views = self.source_catalog.as_view_catalog()
        view = views.create_view(
            NameIdentifier.of("raw", "order_view"),
            [Column.of("id", Types.LongType.get())],
            [SQLRepresentation("trino", "SELECT id FROM orders")],
            default_catalog="sources",
            default_schema="raw",
        )
        identifier = NameIdentifier.of("semantic", "view_model")
        self.models.create_semantic_model(
            identifier, "", self._definition("order_view", "view.raw"), {}
        )
        expected = [self._dataset_id("semantic", "view_model", "view.raw")]
        self.assertEqual(expected, self._lookup(view))
        self.assertEqual([], self._lookup(self.table))
        renamed = views.alter_view(
            NameIdentifier.of("raw", "order_view"), ViewChange.rename("order_view_v2")
        )
        with self.assertRaises(NoSuchMetadataObjectException):
            self._lookup(view)
        self.assertEqual(expected, self._lookup(renamed))
        views.drop_view(NameIdentifier.of("raw", "order_view_v2"))
        with self.assertRaises(NoSuchMetadataObjectException):
            self._lookup(renamed)
        self.models.drop_semantic_model(identifier)

    def _create_table(self, name):
        return self.source_catalog.as_table_catalog().create_table(
            NameIdentifier.of("raw", name),
            [Column.of("id", Types.LongType.get())],
            properties={},
        )

    def _dataset_id(self, schema, model, alias):
        return NameIdentifier.of(self.metalake, "models", schema, model, alias)

    @staticmethod
    def _definition(source, *aliases):
        return SemanticModelDefinition(
            [
                Dataset(alias, NameIdentifier.of("sources", "raw", source))
                for alias in aliases
            ]
        )

    @staticmethod
    def _lookup(source):
        return source.supports_semantic_models().list_semantic_model_datasets()
