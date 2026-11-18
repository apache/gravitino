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

from gravitino.api.semantic.ai_context import AIContext
from gravitino.api.semantic.ai_context_object import AIContextObject
from gravitino.api.semantic.custom_extension import CustomExtension
from gravitino.api.semantic.data_type import DataType
from gravitino.api.semantic.dataset import Dataset
from gravitino.api.semantic.dialect_expression import DialectExpression
from gravitino.api.semantic.dialects import Dialects
from gravitino.api.semantic.expression import Expression
from gravitino.api.semantic.field import Field
from gravitino.api.semantic.metric import Metric
from gravitino.api.semantic.relationship import Relationship
from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition
from gravitino.name_identifier import NameIdentifier


def _orders_dataset() -> Dataset:
    order_amount = Field(
        "order_amount",
        Expression([DialectExpression(Dialects.ANSI_SQL, "order_amount")]),
        datatype=DataType.DECIMAL,
    )
    return Dataset(
        "orders",
        NameIdentifier.of("sales", "mart", "orders"),
        fields=[order_amount],
    )


def _customers_dataset() -> Dataset:
    return Dataset("customers", NameIdentifier.of("sales", "mart", "customers"))


class TestSemanticModelDefinition(unittest.TestCase):
    def test_minimal_definition(self):
        dataset = _orders_dataset()
        definition = SemanticModelDefinition(datasets=[dataset])

        self.assertEqual([dataset], definition.datasets())
        self.assertIsNone(definition.ai_context())
        self.assertIsNone(definition.relationships())
        self.assertIsNone(definition.metrics())
        self.assertIsNone(definition.custom_extensions())

    def test_complete_definition(self):
        orders = _orders_dataset()
        customers = _customers_dataset()
        relationship = Relationship(
            "orders_to_customers", "orders", "customers", ["customer_id"], ["id"]
        )
        metric = Metric(
            "total_revenue",
            Expression(
                [DialectExpression(Dialects.ANSI_SQL, "SUM(orders.order_amount)")]
            ),
        )
        ai_context = AIContext.of(
            AIContextObject(
                instructions="Use certified metrics only",
                additional_properties={"audience": "finance"},
            )
        )
        extension = CustomExtension("acme", "{}")

        definition = SemanticModelDefinition(
            datasets=[orders, customers],
            ai_context=ai_context,
            relationships=[relationship],
            metrics=[metric],
            custom_extensions=[extension],
        )

        self.assertEqual([orders, customers], definition.datasets())
        self.assertEqual(ai_context, definition.ai_context())
        self.assertEqual([relationship], definition.relationships())
        self.assertEqual([metric], definition.metrics())
        self.assertEqual([extension], definition.custom_extensions())

    def test_definition_preserves_collection_order(self):
        orders = _orders_dataset()
        customers = _customers_dataset()

        definition = SemanticModelDefinition(datasets=[orders, customers])
        reversed_definition = SemanticModelDefinition(datasets=[customers, orders])

        self.assertEqual([orders, customers], definition.datasets())
        self.assertNotEqual(reversed_definition, definition)

    def test_definition_returns_copies(self):
        datasets = [_orders_dataset()]
        definition = SemanticModelDefinition(datasets=datasets)

        datasets.append(_customers_dataset())
        definition.datasets().clear()

        self.assertEqual(1, len(definition.datasets()))

    def test_definition_equality(self):
        definition = SemanticModelDefinition(datasets=[_orders_dataset()])
        equal_definition = SemanticModelDefinition(datasets=[_orders_dataset()])

        self.assertEqual(equal_definition, definition)
        self.assertEqual(hash(equal_definition), hash(definition))
        self.assertNotEqual(
            SemanticModelDefinition(datasets=[_customers_dataset()]), definition
        )
        self.assertNotEqual(definition, "invalid")

    def test_definition_rejects_invalid_arguments(self):
        with self.assertRaisesRegex(ValueError, "datasets must not be null or empty"):
            SemanticModelDefinition(datasets=[])
        with self.assertRaisesRegex(ValueError, "datasets must not be null or empty"):
            SemanticModelDefinition(datasets=None)
        with self.assertRaisesRegex(ValueError, r"datasets\[0\] must not be null"):
            SemanticModelDefinition(datasets=[None])
        with self.assertRaisesRegex(ValueError, r"relationships\[0\] must not be null"):
            SemanticModelDefinition(datasets=[_orders_dataset()], relationships=[None])
        with self.assertRaisesRegex(ValueError, r"metrics\[0\] must not be null"):
            SemanticModelDefinition(datasets=[_orders_dataset()], metrics=[None])
        with self.assertRaisesRegex(
            ValueError, r"customExtensions\[0\] must not be null"
        ):
            SemanticModelDefinition(
                datasets=[_orders_dataset()], custom_extensions=[None]
            )
