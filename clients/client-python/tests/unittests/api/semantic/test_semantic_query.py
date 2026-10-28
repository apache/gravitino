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

from gravitino.api.semantic.semantic_query import SemanticQuery
from gravitino.api.semantic.semantic_query_filter import SemanticQueryFilter
from gravitino.api.semantic.semantic_query_plan import SemanticQueryPlan


class TestSemanticQuery(unittest.TestCase):
    def test_build_query(self):
        query_filter = SemanticQueryFilter.of(
            "order_date", SemanticQueryFilter.Operator.GE, "2024-01-01"
        )
        query = SemanticQuery(
            dialect="trino",
            metrics=["order_amount", "order_count"],
            dimensions=["region"],
            filters=[query_filter],
        )

        self.assertEqual(["order_amount", "order_count"], query.metrics())
        self.assertEqual(["region"], query.dimensions())
        self.assertEqual([query_filter], query.filters())
        self.assertEqual("trino", query.dialect())

        same = SemanticQuery(
            "trino", ["order_amount", "order_count"], ["region"], [query_filter]
        )
        self.assertEqual(query, same)
        self.assertEqual(hash(query), hash(same))

    def test_query_defaults_optional_collections(self):
        query = SemanticQuery(dialect="spark", metrics=["revenue"])

        self.assertEqual([], query.dimensions())
        self.assertEqual([], query.filters())

    def test_query_requires_metric_or_dimension(self):
        with self.assertRaisesRegex(
            ValueError, "a semantic query must select at least one metric or dimension"
        ):
            SemanticQuery(dialect="trino")

    def test_query_rejects_blank_dialect(self):
        with self.assertRaisesRegex(ValueError, "dialect must not be null or empty"):
            SemanticQuery(dialect=" ", metrics=["revenue"])

    def test_query_rejects_blank_names_and_non_filters(self):
        with self.assertRaisesRegex(
            ValueError, "metrics\\[0\\] must not be null or empty"
        ):
            SemanticQuery(dialect="trino", metrics=[""])
        with self.assertRaisesRegex(
            ValueError, "dimensions\\[0\\] must not be null or empty"
        ):
            SemanticQuery(dialect="trino", dimensions=[" "])
        with self.assertRaisesRegex(ValueError, "filters\\[0\\] must not be null"):
            SemanticQuery(dialect="trino", metrics=["revenue"], filters=[None])

    def test_query_is_immutable(self):
        metrics = ["revenue"]
        query = SemanticQuery(dialect="trino", metrics=metrics)
        metrics.append("cost")

        self.assertEqual(["revenue"], query.metrics())
        query.metrics().append("cost")
        self.assertEqual(["revenue"], query.metrics())

    def test_query_equality(self):
        query = SemanticQuery(dialect="trino", metrics=["revenue"])

        self.assertNotEqual(query, SemanticQuery(dialect="spark", metrics=["revenue"]))
        self.assertNotEqual(query, "not a query")

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

    def test_plan_validation(self):
        with self.assertRaisesRegex(ValueError, "sql must not be null or empty"):
            SemanticQueryPlan("", "trino", "planner")
        with self.assertRaisesRegex(ValueError, "dialect must not be null or empty"):
            SemanticQueryPlan("SELECT 1", " ", "planner")
        with self.assertRaisesRegex(ValueError, "planner must not be null or empty"):
            SemanticQueryPlan("SELECT 1", "trino", "")
        with self.assertRaisesRegex(
            ValueError, "warnings\\[0\\] must not be null or empty"
        ):
            SemanticQueryPlan("SELECT 1", "trino", "planner", [" "])

    def test_plan_build_and_equality(self):
        plan = SemanticQueryPlan("SELECT 1", "trino", "planner", ["warning"])

        self.assertEqual("SELECT 1", plan.sql())
        self.assertEqual("trino", plan.dialect())
        self.assertEqual("planner", plan.planner())
        self.assertEqual(["warning"], plan.warnings())
        self.assertEqual(
            plan, SemanticQueryPlan("SELECT 1", "trino", "planner", ["warning"])
        )
        self.assertEqual(
            hash(plan),
            hash(SemanticQueryPlan("SELECT 1", "trino", "planner", ["warning"])),
        )
        self.assertNotEqual(
            plan, SemanticQueryPlan("SELECT 2", "trino", "planner", ["warning"])
        )
        self.assertNotEqual(plan, "not a plan")
