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
from gravitino.dto.requests.plan_semantic_query_request import (
    PlanSemanticQueryRequest,
)
from gravitino.dto.responses.semantic_query_plan_response import (
    SemanticQueryPlanResponse,
)
from gravitino.dto.semantic.semantic_query_filter_dto import SemanticQueryFilterDTO


class TestSemanticQueryFilterDTO(unittest.TestCase):
    def test_round_trip(self):
        query_filter = SemanticQueryFilter.of(
            "order_date", SemanticQueryFilter.Operator.GE, "2024-01-01"
        )

        dto = SemanticQueryFilterDTO.from_filter(query_filter)

        self.assertEqual(
            {
                "field": "order_date",
                "operator": "ge",
                "value": "2024-01-01",
            },
            dto.to_dict(),
        )
        dto.validate()
        self.assertEqual(query_filter, dto.to_filter())

    def test_operator_is_parsed_case_insensitively(self):
        for operator in SemanticQueryFilter.Operator:
            dto = SemanticQueryFilterDTO(
                _field="field", _operator=operator.value.lower(), _value="value"
            )
            self.assertEqual(operator, dto.to_filter().operator())

    def test_validation(self):
        with self.assertRaisesRegex(
            ValueError, '"field" field is required and cannot be empty'
        ):
            SemanticQueryFilterDTO(_field="", _operator="eq", _value="value").validate()
        with self.assertRaisesRegex(
            ValueError, '"operator" field is required and cannot be empty'
        ):
            SemanticQueryFilterDTO(
                _field="field", _operator="", _value="value"
            ).validate()
        with self.assertRaisesRegex(
            ValueError, '"value" field is required and cannot be null'
        ):
            SemanticQueryFilterDTO(_field="field", _operator="eq").validate()
        with self.assertRaisesRegex(
            ValueError, "Unsupported filter operator 'between'"
        ):
            SemanticQueryFilterDTO(
                _field="field", _operator="between", _value="value"
            ).validate()


class TestPlanSemanticQueryRequest(unittest.TestCase):
    def test_from_query_and_json_wire_names(self):
        query = SemanticQuery(
            dialect="trino",
            metrics=["revenue"],
            dimensions=["region"],
            filters=[
                SemanticQueryFilter.of(
                    "order_date", SemanticQueryFilter.Operator.GE, "2024-01-01"
                )
            ],
        )

        request = PlanSemanticQueryRequest.from_query(query)

        self.assertEqual(
            {
                "metrics": ["revenue"],
                "dimensions": ["region"],
                "filters": [
                    {"field": "order_date", "operator": "ge", "value": "2024-01-01"}
                ],
                "dialect": "trino",
            },
            request.to_dict(),
        )
        request.validate()
        self.assertEqual(query, request.to_query())

    def test_from_json_and_omitted_collections(self):
        request = PlanSemanticQueryRequest.from_json(
            '{"metrics":["revenue"],"dialect":"spark"}', infer_missing=True
        )
        request.validate()

        query = request.to_query()
        self.assertEqual(["revenue"], query.metrics())
        self.assertEqual([], query.dimensions())
        self.assertEqual([], query.filters())
        self.assertEqual("spark", query.dialect())

    def test_validation(self):
        with self.assertRaisesRegex(
            ValueError, '"dialect" field is required and cannot be empty'
        ):
            PlanSemanticQueryRequest(_metrics=["revenue"]).validate()
        with self.assertRaisesRegex(
            ValueError, '"metrics"\\[0\\] field cannot be null or empty'
        ):
            PlanSemanticQueryRequest(_metrics=[""], _dialect="trino").validate()
        with self.assertRaisesRegex(
            ValueError, '"dimensions"\\[0\\] field cannot be null or empty'
        ):
            PlanSemanticQueryRequest(_dimensions=[" "], _dialect="trino").validate()
        with self.assertRaisesRegex(
            ValueError, '"filters"\\[0\\] field cannot be null'
        ):
            PlanSemanticQueryRequest(_filters=[None], _dialect="trino").validate()
        with self.assertRaisesRegex(
            ValueError, "At least one metric or dimension must be selected"
        ):
            PlanSemanticQueryRequest(_dialect="trino").validate()


class TestSemanticQueryPlanResponse(unittest.TestCase):
    def test_from_json_and_to_plan(self):
        response = SemanticQueryPlanResponse.from_json(
            '{"code":0,"sql":"SELECT 1","dialect":"trino",'
            '"planner":"test-planner","warnings":["warning"]}',
            infer_missing=True,
        )
        response.validate()

        plan = response.to_plan()
        self.assertEqual(
            SemanticQueryPlan("SELECT 1", "trino", "test-planner", ["warning"]), plan
        )

    def test_round_trip(self):
        response = SemanticQueryPlanResponse(
            _code=0,
            _sql="SELECT 1",
            _dialect="spark",
            _planner="planner",
            _warnings=[],
        )

        self.assertEqual(
            {
                "code": 0,
                "sql": "SELECT 1",
                "dialect": "spark",
                "planner": "planner",
                "warnings": [],
            },
            response.to_dict(),
        )
        response.validate()
        self.assertEqual(
            SemanticQueryPlan("SELECT 1", "spark", "planner", []),
            response.to_plan(),
        )

    def test_validation(self):
        cases = [
            ({"_sql": None}, "sql must not be null or empty"),
            ({"_sql": "SELECT 1", "_dialect": ""}, "dialect must not be null or empty"),
            (
                {"_sql": "SELECT 1", "_dialect": "trino"},
                "planner must not be null or empty",
            ),
            (
                {"_sql": "SELECT 1", "_dialect": "trino", "_planner": "planner"},
                "warnings must not be null",
            ),
            (
                {
                    "_sql": "SELECT 1",
                    "_dialect": "trino",
                    "_planner": "planner",
                    "_warnings": [" "],
                },
                "warnings\\[0\\] must not be null or empty",
            ),
        ]
        for fields, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError, message):
                    SemanticQueryPlanResponse(
                        _code=0,
                        _sql=fields.get("_sql"),
                        _dialect=fields.get("_dialect"),
                        _planner=fields.get("_planner"),
                        _warnings=fields.get("_warnings"),
                    ).validate()
