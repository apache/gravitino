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

from decimal import Decimal
from uuid import uuid4

from gravitino import Catalog, GravitinoAdminClient, NameIdentifier
from gravitino.api.rel.column import Column
from gravitino.api.rel.types.types import Types
from gravitino.api.semantic.ai_context import AIContext
from gravitino.api.semantic.ai_context_object import AIContextObject
from gravitino.api.semantic.custom_extension import CustomExtension
from gravitino.api.semantic.data_type import DataType
from gravitino.api.semantic.dataset import Dataset
from gravitino.api.semantic.dialect_expression import DialectExpression
from gravitino.api.semantic.expression import Expression
from gravitino.api.semantic.metric import Metric
from gravitino.api.semantic.semantic_model_change import SemanticModelChange
from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition
from gravitino.api.semantic.semantic_model_metric import SemanticModelMetric
from gravitino.exceptions.base import NoSuchSchemaException
from gravitino.namespace import Namespace
from tests.integration.integration_test_env import IntegrationTestEnv


class TestSemanticModelMetricDiscovery(IntegrationTestEnv):
    """Exercise metric discovery through the public SDK and a real server."""

    def setUp(self):
        super().setUp()
        admin = GravitinoAdminClient(uri="http://localhost:8090")
        self.addCleanup(admin.close)
        metalake = "metrics_it_" + uuid4().hex
        client = self.create_metalake_client(admin, metalake)
        self.addCleanup(client.close)
        self.addCleanup(admin.drop_metalake, metalake, force=True)
        self.catalog = client.create_catalog(
            "metrics_catalog", Catalog.Type.RELATIONAL, "lakehouse-generic", "", {}
        )
        for schema in ("source", "semantic", "other"):
            self.catalog.as_schemas().create_schema(schema, "", {})
        self.catalog.as_table_catalog().create_table(
            NameIdentifier.of("source", "orders"),
            [Column.of("amount", Types.LongType.get())],
            properties={
                "format": "delta",
                "external": "true",
                "location": f"file:///tmp/{metalake}/orders",
            },
        )
        self.models = self.catalog.as_semantic_model_catalog()
        self.namespace = Namespace.of("semantic")

    def test_discovery_preserves_complete_metrics_order_and_schema_isolation(self):
        self.assertEqual([], self.models.list_metrics(self.namespace))
        self.assertEqual([], self.models.list_metric_infos(self.namespace))
        metrics = [
            self._metric("revenue"),
            self._metric("Revenue"),
            self._metric("收入"),
        ]
        self._create("b", [metrics[1]])
        self._create("a", metrics)
        self._create("empty", [])
        self._create("null_metrics", None)
        self.models.create_semantic_model(
            NameIdentifier.of("other", "a"), "", self._definition(metrics), {}
        )

        expected = [
            SemanticModelMetric(Namespace.of("semantic", "a"), metrics[1]),
            SemanticModelMetric(Namespace.of("semantic", "a"), metrics[0]),
            SemanticModelMetric(Namespace.of("semantic", "a"), metrics[2]),
            SemanticModelMetric(Namespace.of("semantic", "b"), metrics[1]),
        ]
        self._assert_discovery(expected)
        self.assertEqual(3, len(self.models.list_metrics(Namespace.of("other"))))
        actual = self.models.list_metric_infos(self.namespace)[0].metric()
        self.assertIsInstance(
            actual.ai_context().object().additional_properties()["threshold"], Decimal
        )

    def test_discovery_tracks_definition_replacement_rename_and_drop(self):
        identifier = NameIdentifier.of("semantic", "sales")
        original = self._metric("Revenue")
        self._create("sales", [original, self._metric("removed")])
        changed = Metric(
            "Revenue",
            Expression([DialectExpression("ANSI_SQL", "COUNT(*)")]),
            ai_context=AIContext.of("Updated"),
        )
        self.models.alter_semantic_model(
            identifier,
            SemanticModelChange.replace_definition(self._definition([changed])),
        )
        self._assert_discovery(
            [SemanticModelMetric(Namespace.of("semantic", "sales"), changed)]
        )

        self.models.alter_semantic_model(
            identifier, SemanticModelChange.rename("renamed")
        )
        identifier = NameIdentifier.of("semantic", "renamed")
        self._assert_discovery(
            [SemanticModelMetric(Namespace.of("semantic", "renamed"), changed)]
        )
        self.models.alter_semantic_model(
            identifier, SemanticModelChange.replace_definition(self._definition([]))
        )
        self._assert_discovery([])
        self.models.alter_semantic_model(
            identifier,
            SemanticModelChange.replace_definition(self._definition([original])),
        )
        self._assert_discovery(
            [SemanticModelMetric(Namespace.of("semantic", "renamed"), original)]
        )
        self.assertTrue(self.models.drop_semantic_model(identifier))
        self._assert_discovery([])

    def test_missing_and_dropped_schema(self):
        for method in (self.models.list_metrics, self.models.list_metric_infos):
            with self.subTest(method=method), self.assertRaises(NoSuchSchemaException):
                method(Namespace.of("missing"))
        self._create("sales", [self._metric("Revenue")])
        self.assertTrue(self.catalog.as_schemas().drop_schema("semantic", True))
        for method in (self.models.list_metrics, self.models.list_metric_infos):
            with self.subTest(method=method), self.assertRaises(NoSuchSchemaException):
                method(self.namespace)
        self.catalog.as_schemas().create_schema("semantic", "", {})
        self._assert_discovery([])

    def _create(self, name, metrics):
        self.models.create_semantic_model(
            NameIdentifier.of("semantic", name), "", self._definition(metrics), {}
        )

    def _assert_discovery(self, expected):
        self.assertEqual(expected, self.models.list_metric_infos(self.namespace))
        self.assertEqual(
            [
                NameIdentifier.builder(info.namespace(), info.metric().name())
                for info in expected
            ],
            self.models.list_metrics(self.namespace),
        )

    @staticmethod
    def _definition(metrics):
        return SemanticModelDefinition(
            [
                Dataset(
                    "orders", NameIdentifier.of("metrics_catalog", "source", "orders")
                )
            ],
            metrics=metrics,
        )

    @staticmethod
    def _metric(name):
        return Metric(
            name,
            Expression([DialectExpression("ANSI_SQL", "SUM(orders.amount)")]),
            "Revenue - café 雪",
            DataType.DECIMAL,
            AIContext.of(
                AIContextObject(
                    instructions="Use certified metrics",
                    synonyms=["Sales"],
                    examples=["By year"],
                    additional_properties={
                        "threshold": Decimal("0.12345678901234567890123456789"),
                        "nested": {
                            "large": 1234567890123456789012345678901234567890,
                            "values": [True, None, "extra"],
                        },
                    },
                )
            ),
            [CustomExtension("vendor", '{"opaque": "雪"}')],
        )
