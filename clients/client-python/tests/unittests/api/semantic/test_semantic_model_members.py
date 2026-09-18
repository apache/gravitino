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
from gravitino.api.semantic.custom_extension import CustomExtension
from gravitino.api.semantic.data_type import DataType
from gravitino.api.semantic.dataset import Dataset
from gravitino.api.semantic.dialect_expression import DialectExpression
from gravitino.api.semantic.dialects import Dialects
from gravitino.api.semantic.dimension import Dimension
from gravitino.api.semantic.expression import Expression
from gravitino.api.semantic.field import Field
from gravitino.api.semantic.metric import Metric
from gravitino.api.semantic.relationship import Relationship
from gravitino.name_identifier import NameIdentifier


def _expression(expression: str = "order_amount") -> Expression:
    return Expression([DialectExpression(Dialects.ANSI_SQL, expression)])


class TestSemanticModelMembers(unittest.TestCase):
    def test_field_defaults(self):
        field = Field("order_amount", _expression())

        self.assertEqual("order_amount", field.name())
        self.assertEqual(_expression(), field.expression())
        self.assertIsNone(field.dimension())
        self.assertIsNone(field.label())
        self.assertIsNone(field.description())
        self.assertIsNone(field.datatype())
        self.assertIsNone(field.ai_context())
        self.assertIsNone(field.custom_extensions())

    def test_field_with_all_members(self):
        extension = CustomExtension("acme", "{}")
        field = Field(
            name="order_date",
            expression=_expression("order_date"),
            dimension=Dimension(is_time=True),
            label="Order date",
            description="The date the order was placed",
            datatype=DataType.DATE,
            ai_context=AIContext.of("Order placement date"),
            custom_extensions=[extension],
        )

        self.assertEqual(Dimension(is_time=True), field.dimension())
        self.assertEqual("Order date", field.label())
        self.assertEqual("The date the order was placed", field.description())
        self.assertEqual(DataType.DATE, field.datatype())
        self.assertEqual(AIContext.of("Order placement date"), field.ai_context())
        self.assertEqual([extension], field.custom_extensions())

    def test_field_equality(self):
        field = Field("order_amount", _expression(), datatype=DataType.DECIMAL)
        equal_field = Field("order_amount", _expression(), datatype=DataType.DECIMAL)

        self.assertEqual(equal_field, field)
        self.assertEqual(hash(equal_field), hash(field))
        self.assertNotEqual(Field("order_amount", _expression()), field)
        self.assertNotEqual(field, "invalid")

    def test_field_rejects_invalid_arguments(self):
        with self.assertRaisesRegex(ValueError, "name must not be null or empty"):
            Field("", _expression())
        with self.assertRaisesRegex(ValueError, "expression must not be null"):
            Field("order_amount", None)
        with self.assertRaisesRegex(
            ValueError, r"customExtensions\[0\] must not be null"
        ):
            Field("order_amount", _expression(), custom_extensions=[None])

    def test_dataset_defaults(self):
        source = NameIdentifier.of("sales", "mart", "orders")
        dataset = Dataset("orders", source)

        self.assertEqual("orders", dataset.name())
        self.assertEqual(source, dataset.source())
        self.assertIsNone(dataset.primary_key())
        self.assertIsNone(dataset.unique_keys())
        self.assertIsNone(dataset.description())
        self.assertIsNone(dataset.ai_context())
        self.assertIsNone(dataset.fields())
        self.assertIsNone(dataset.custom_extensions())

    def test_dataset_with_all_members(self):
        field = Field("order_amount", _expression())
        dataset = Dataset(
            name="orders",
            source=NameIdentifier.of("sales", "mart", "orders"),
            primary_key=["order_id"],
            unique_keys=[["order_id"], ["customer_id", "order_date"]],
            description="Order facts",
            ai_context=AIContext.of("Certified order facts"),
            fields=[field],
            custom_extensions=[CustomExtension("acme", "{}")],
        )

        self.assertEqual(["order_id"], dataset.primary_key())
        self.assertEqual(
            [["order_id"], ["customer_id", "order_date"]], dataset.unique_keys()
        )
        self.assertEqual("Order facts", dataset.description())
        self.assertEqual([field], dataset.fields())

    def test_dataset_returns_copies(self):
        primary_key = ["order_id"]
        unique_keys = [["order_id"]]
        fields = [Field("order_amount", _expression())]
        dataset = Dataset(
            "orders",
            NameIdentifier.of("sales", "mart", "orders"),
            primary_key=primary_key,
            unique_keys=unique_keys,
            fields=fields,
        )

        primary_key.append("mutated")
        unique_keys[0].append("mutated")
        fields.clear()
        dataset.primary_key().clear()
        dataset.unique_keys()[0].clear()
        dataset.fields().clear()

        self.assertEqual(["order_id"], dataset.primary_key())
        self.assertEqual([["order_id"]], dataset.unique_keys())
        self.assertEqual(1, len(dataset.fields()))

    def test_dataset_rejects_invalid_arguments(self):
        source = NameIdentifier.of("sales", "mart", "orders")

        with self.assertRaisesRegex(ValueError, "name must not be null or empty"):
            Dataset("", source)
        with self.assertRaisesRegex(ValueError, "source must not be null"):
            Dataset("orders", None)
        with self.assertRaisesRegex(
            ValueError, r"primaryKey\[0\] must not be null or empty"
        ):
            Dataset("orders", source, primary_key=[""])
        with self.assertRaisesRegex(
            ValueError, r"uniqueKeys\[0\] must not be null or empty"
        ):
            Dataset("orders", source, unique_keys=[[]])
        with self.assertRaisesRegex(
            ValueError, r"uniqueKeys\[1\]\[0\] must not be null or empty"
        ):
            Dataset("orders", source, unique_keys=[["order_id"], [None]])
        with self.assertRaisesRegex(ValueError, r"fields\[0\] must not be null"):
            Dataset("orders", source, fields=[None])

    def test_metric(self):
        metric = Metric(
            name="total_revenue",
            expression=_expression("SUM(orders.order_amount)"),
            description="Total revenue across all orders",
            datatype=DataType.DECIMAL,
        )

        self.assertEqual("total_revenue", metric.name())
        self.assertEqual(_expression("SUM(orders.order_amount)"), metric.expression())
        self.assertEqual("Total revenue across all orders", metric.description())
        self.assertEqual(DataType.DECIMAL, metric.datatype())
        self.assertIsNone(metric.ai_context())
        self.assertIsNone(metric.custom_extensions())

    def test_metric_equality(self):
        metric = Metric("total_revenue", _expression())
        equal_metric = Metric("total_revenue", _expression())

        self.assertEqual(equal_metric, metric)
        self.assertEqual(hash(equal_metric), hash(metric))
        self.assertNotEqual(Metric("other", _expression()), metric)
        self.assertNotEqual(metric, "invalid")

    def test_metric_rejects_invalid_arguments(self):
        with self.assertRaisesRegex(ValueError, "name must not be null or empty"):
            Metric("", _expression())
        with self.assertRaisesRegex(ValueError, "expression must not be null"):
            Metric("total_revenue", None)

    def test_relationship(self):
        relationship = Relationship(
            name="orders_to_customers",
            from_dataset="orders",
            to_dataset="customers",
            from_columns=["customer_id"],
            to_columns=["id"],
        )

        self.assertEqual("orders_to_customers", relationship.name())
        self.assertEqual("orders", relationship.from_dataset())
        self.assertEqual("customers", relationship.to_dataset())
        self.assertEqual(["customer_id"], relationship.from_columns())
        self.assertEqual(["id"], relationship.to_columns())
        self.assertIsNone(relationship.ai_context())
        self.assertIsNone(relationship.custom_extensions())

    def test_relationship_returns_copies(self):
        from_columns = ["customer_id"]
        relationship = Relationship(
            "orders_to_customers", "orders", "customers", from_columns, ["id"]
        )

        from_columns.append("mutated")
        relationship.from_columns().clear()

        self.assertEqual(["customer_id"], relationship.from_columns())

    def test_relationship_equality(self):
        relationship = Relationship(
            "orders_to_customers", "orders", "customers", ["customer_id"], ["id"]
        )
        equal_relationship = Relationship(
            "orders_to_customers", "orders", "customers", ["customer_id"], ["id"]
        )

        self.assertEqual(equal_relationship, relationship)
        self.assertEqual(hash(equal_relationship), hash(relationship))
        self.assertNotEqual(relationship, "invalid")

    def test_relationship_rejects_invalid_arguments(self):
        with self.assertRaisesRegex(ValueError, "name must not be null or empty"):
            Relationship("", "orders", "customers", ["customer_id"], ["id"])
        with self.assertRaisesRegex(ValueError, "from must not be null or empty"):
            Relationship("rel", "", "customers", ["customer_id"], ["id"])
        with self.assertRaisesRegex(ValueError, "to must not be null or empty"):
            Relationship("rel", "orders", "", ["customer_id"], ["id"])
        with self.assertRaisesRegex(
            ValueError, "fromColumns must not be null or empty"
        ):
            Relationship("rel", "orders", "customers", [], ["id"])
        with self.assertRaisesRegex(ValueError, "toColumns must not be null or empty"):
            Relationship("rel", "orders", "customers", ["customer_id"], [])
        with self.assertRaisesRegex(
            ValueError, r"fromColumns\[0\] must not be null or empty"
        ):
            Relationship("rel", "orders", "customers", [""], ["id"])
        with self.assertRaisesRegex(
            ValueError, "fromColumns and toColumns must have the same length"
        ):
            Relationship("rel", "orders", "customers", ["customer_id"], ["id", "extra"])
