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

from gravitino.api.semantic.data_type import DataType
from gravitino.api.semantic.dialect_expression import DialectExpression
from gravitino.api.semantic.expression import Expression
from gravitino.api.semantic.plan_field_ref import PlanFieldRef
from gravitino.api.semantic.plan_filter import PlanFilter
from gravitino.api.semantic.plan_join import PlanJoin
from gravitino.api.semantic.plan_selection import PlanSelection
from gravitino.api.semantic.plan_source import PlanSource
from gravitino.api.semantic.semantic_query import SemanticQuery
from gravitino.api.semantic.semantic_query_filter import SemanticQueryFilter
from gravitino.api.semantic.semantic_query_plan import SemanticQueryPlan
from gravitino.name_identifier import NameIdentifier

MODEL = NameIdentifier.of("metalake", "catalog", "schema", "sales_model")
ORDERS = NameIdentifier.of("catalog", "schema", "orders_table")


def expression(text: str) -> Expression:
    return Expression([DialectExpression("ANSI_SQL", text)])


def plan(limit=None, joins=None, group_by=None, warnings=None) -> SemanticQueryPlan:
    revenue = PlanFieldRef.of_expression(
        "orders", expression("SUM(orders.amount)"), DataType.DECIMAL
    )
    region = PlanFieldRef.of_expression(
        "regions", expression("regions.name"), DataType.STRING
    )
    return SemanticQueryPlan(
        semantic_model=MODEL,
        semantic_model_version=3,
        sources=[
            PlanSource("orders", ORDERS),
            PlanSource("regions", ORDERS),
        ],
        joins=[] if joins is None else joins,
        selections=[
            PlanSelection.of("Gross_Revenue", PlanSelection.Role.METRIC, True, revenue),
            PlanSelection.of("Region", PlanSelection.Role.DIMENSION, False, region),
        ],
        filters=[],
        group_by=["Region"] if group_by is None else group_by,
        limit=limit,
        warnings=[] if warnings is None else warnings,
    )


class TestSemanticQuery(unittest.TestCase):
    def test_build_query(self):
        query_filter = SemanticQueryFilter.of(
            "order_date", SemanticQueryFilter.Operator.GE, "2024-01-01"
        )
        query = SemanticQuery(
            metrics=["order_amount", "order_count"],
            dimensions=["region"],
            filters=[query_filter],
            limit=100,
        )

        self.assertEqual(["order_amount", "order_count"], query.metrics())
        self.assertEqual(["region"], query.dimensions())
        self.assertEqual([query_filter], query.filters())
        self.assertEqual(100, query.limit())

        same = SemanticQuery(
            ["order_amount", "order_count"], ["region"], [query_filter], 100
        )
        self.assertEqual(query, same)
        self.assertEqual(hash(query), hash(same))

    def test_query_defaults_optional_values(self):
        query = SemanticQuery(metrics=["revenue"])

        self.assertEqual([], query.dimensions())
        self.assertEqual([], query.filters())
        self.assertIsNone(query.limit())

    def test_query_requires_metric_or_dimension(self):
        with self.assertRaisesRegex(
            ValueError, "a semantic query must select at least one metric or dimension"
        ):
            SemanticQuery()

    def test_query_rejects_negative_limit(self):
        with self.assertRaisesRegex(ValueError, "limit must be null or non-negative"):
            SemanticQuery(metrics=["revenue"], limit=-1)
        with self.assertRaisesRegex(ValueError, "limit must be null or non-negative"):
            SemanticQuery(metrics=["revenue"], limit=True)

    def test_query_rejects_blank_names_and_non_filters(self):
        with self.assertRaisesRegex(
            ValueError, "metrics\\[0\\] must not be null or empty"
        ):
            SemanticQuery(metrics=[""])
        with self.assertRaisesRegex(
            ValueError, "dimensions\\[0\\] must not be null or empty"
        ):
            SemanticQuery(dimensions=[" "])
        with self.assertRaisesRegex(ValueError, "filters\\[0\\] must not be null"):
            SemanticQuery(metrics=["revenue"], filters=[None])

    def test_query_is_immutable(self):
        metrics = ["revenue"]
        query = SemanticQuery(metrics=metrics)
        metrics.append("cost")

        self.assertEqual(["revenue"], query.metrics())
        query.metrics().append("cost")
        self.assertEqual(["revenue"], query.metrics())

    def test_query_equality(self):
        query = SemanticQuery(metrics=["revenue"])

        self.assertNotEqual(query, SemanticQuery(metrics=["revenue"], limit=10))
        self.assertNotEqual(query, "not a query")


class TestSemanticQueryFilter(unittest.TestCase):
    def test_filter_validation(self):
        with self.assertRaisesRegex(
            ValueError, "filter field must not be null or empty"
        ):
            SemanticQueryFilter.of("", SemanticQueryFilter.Operator.EQ, "1")
        with self.assertRaisesRegex(ValueError, "filter operator must not be null"):
            SemanticQueryFilter.of("field", None, "1")
        with self.assertRaisesRegex(ValueError, "filter value must not be null"):
            SemanticQueryFilter.of("field", SemanticQueryFilter.Operator.EQ, None)

    def test_filter_accessors_and_equality(self):
        query_filter = SemanticQueryFilter.of(
            "region", SemanticQueryFilter.Operator.NE, "us-east"
        )

        self.assertEqual("region", query_filter.field())
        self.assertEqual(SemanticQueryFilter.Operator.NE, query_filter.operator())
        self.assertEqual("us-east", query_filter.value())
        self.assertEqual(
            query_filter,
            SemanticQueryFilter.of(
                "region", SemanticQueryFilter.Operator.NE, "us-east"
            ),
        )
        self.assertEqual(
            hash(query_filter),
            hash(
                SemanticQueryFilter.of(
                    "region", SemanticQueryFilter.Operator.NE, "us-east"
                )
            ),
        )
        self.assertNotEqual(
            query_filter,
            SemanticQueryFilter.of(
                "region", SemanticQueryFilter.Operator.EQ, "us-east"
            ),
        )
        self.assertNotEqual(query_filter, "not a filter")

    def test_filter_operator_values(self):
        self.assertEqual(
            ["EQ", "NE", "GT", "GE", "LT", "LE"],
            [operator.value for operator in SemanticQueryFilter.Operator],
        )


class TestSemanticQueryPlan(unittest.TestCase):
    def test_plan_accessors_and_equality(self):
        value = plan(limit=1000, warnings=["warning"])

        self.assertEqual(MODEL, value.semantic_model())
        self.assertEqual(3, value.semantic_model_version())
        self.assertEqual(
            [PlanSource("orders", ORDERS), PlanSource("regions", ORDERS)],
            value.sources(),
        )
        self.assertEqual(
            ["Gross_Revenue", "Region"], [s.name() for s in value.selections()]
        )
        self.assertEqual(["Region"], value.group_by())
        self.assertEqual(1000, value.limit())
        self.assertEqual(["warning"], value.warnings())

        self.assertEqual(value, plan(limit=1000, warnings=["warning"]))
        self.assertEqual(hash(value), hash(plan(limit=1000, warnings=["warning"])))
        self.assertNotEqual(value, plan(limit=2000, warnings=["warning"]))
        self.assertNotEqual(value, "not a plan")

    def test_plan_is_immutable(self):
        sources = [PlanSource("orders", ORDERS)]
        revenue = PlanFieldRef.of_column("orders", "amount", None)
        value = SemanticQueryPlan(
            MODEL,
            1,
            sources,
            [],
            [PlanSelection.of("revenue", PlanSelection.Role.METRIC, True, revenue)],
            [],
            [],
        )
        sources.append(PlanSource("regions", ORDERS))
        value.sources().append(PlanSource("regions", ORDERS))

        self.assertEqual(1, len(value.sources()))

    def test_plan_requires_positive_version(self):
        with self.assertRaisesRegex(
            ValueError, "semanticModelVersion must be positive"
        ):
            SemanticQueryPlan(MODEL, 0, [PlanSource("orders", ORDERS)], [], [], [], [])
        with self.assertRaisesRegex(
            ValueError, "semanticModelVersion must be positive"
        ):
            SemanticQueryPlan(
                MODEL, True, [PlanSource("orders", ORDERS)], [], [], [], []
            )

    def test_plan_validates_sources_and_selections(self):
        revenue = PlanFieldRef.of_column("orders", "amount", None)
        selection = PlanSelection.of(
            "revenue", PlanSelection.Role.METRIC, True, revenue
        )
        with self.assertRaisesRegex(ValueError, "sources must not be null or empty"):
            SemanticQueryPlan(MODEL, 1, [], [], [selection], [], [])
        with self.assertRaisesRegex(
            ValueError, "sources must not contain duplicate dataset"
        ):
            SemanticQueryPlan(
                MODEL,
                1,
                [PlanSource("orders", ORDERS), PlanSource("orders", ORDERS)],
                [],
                [selection],
                [],
                [],
            )

        with self.assertRaisesRegex(ValueError, "selections must not be null or empty"):
            SemanticQueryPlan(MODEL, 1, [PlanSource("orders", ORDERS)], [], [], [], [])
        with self.assertRaisesRegex(ValueError, "selection names must be unique"):
            SemanticQueryPlan(
                MODEL,
                1,
                [PlanSource("orders", ORDERS)],
                [],
                [selection, selection],
                [],
                [],
            )
        with self.assertRaisesRegex(
            ValueError, "selection revenue references dataset regions"
        ):
            SemanticQueryPlan(
                MODEL,
                1,
                [PlanSource("orders", ORDERS)],
                [],
                [
                    PlanSelection.of(
                        "revenue",
                        PlanSelection.Role.METRIC,
                        True,
                        PlanFieldRef.of_column("regions", "amount", None),
                    )
                ],
                [],
                [],
            )
        with self.assertRaisesRegex(
            ValueError, "groupBy must equal the non-aggregate selection names"
        ):
            SemanticQueryPlan(
                MODEL,
                1,
                [PlanSource("orders", ORDERS)],
                [],
                [selection],
                [],
                ["revenue"],
            )

    def test_plan_validates_join_order(self):
        join = PlanJoin.of(
            PlanJoin.Type.INNER,
            "orders_to_regions",
            "regions",
            ["id"],
            "orders",
            ["region_id"],
        )
        with self.assertRaisesRegex(
            ValueError, "must add a dataset that appears later in sources"
        ):
            plan(joins=[join])

    def test_field_reference_requires_exactly_one_representation(self):
        self.assertEqual(
            "orders",
            PlanFieldRef.of_column("orders", "amount", None).dataset(),
        )
        with self.assertRaisesRegex(
            ValueError, "exactly one of expression and column must be set"
        ):
            PlanFieldRef("orders", None, None, None)
        with self.assertRaisesRegex(ValueError, "column must not be blank"):
            PlanFieldRef.of_column("orders", " ", None)
        with self.assertRaisesRegex(ValueError, "dataset must not be null or blank"):
            PlanFieldRef.of_column("", "amount", None)

    def test_join_validation(self):
        join = PlanJoin.of(
            PlanJoin.Type.INNER,
            "orders_to_regions",
            "orders",
            ["region_id"],
            "regions",
            ["id"],
        )
        self.assertEqual(["region_id"], join.from_columns())
        self.assertEqual(["id"], join.to_columns())
        with self.assertRaisesRegex(
            ValueError, "fromColumns and toColumns must have the same length"
        ):
            PlanJoin.of(
                PlanJoin.Type.INNER, None, "orders", ["a", "b"], "regions", ["c"]
            )
        with self.assertRaisesRegex(ValueError, "relationship must not be blank"):
            PlanJoin.of(PlanJoin.Type.INNER, " ", "orders", ["a"], "regions", ["b"])

    def test_selection_validation(self):
        field = PlanFieldRef.of_column("orders", "amount", None)
        with self.assertRaisesRegex(
            ValueError, "aggregate must be true exactly for METRIC selections"
        ):
            PlanSelection.of("revenue", PlanSelection.Role.METRIC, False, field)
        with self.assertRaisesRegex(
            ValueError, "selection name must not be null or blank"
        ):
            PlanSelection.of("", PlanSelection.Role.METRIC, True, field)

    def test_filter_validation(self):
        field = PlanFieldRef.of_column("orders", "amount", None)
        query_filter = PlanFilter.of(field, SemanticQueryFilter.Operator.GE, "1")
        self.assertEqual(field, query_filter.field())
        with self.assertRaisesRegex(ValueError, "value must not be null"):
            PlanFilter.of(field, SemanticQueryFilter.Operator.GE, None)
