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
from gravitino.api.semantic.plan_selection import PlanSelection
from gravitino.api.semantic.plan_source import PlanSource
from gravitino.api.semantic.semantic_query import SemanticQuery
from gravitino.api.semantic.semantic_query_filter import SemanticQueryFilter
from gravitino.api.semantic.semantic_query_plan import SemanticQueryPlan
from gravitino.dto.requests.plan_semantic_query_request import (
    PlanSemanticQueryRequest,
)
from gravitino.dto.responses.semantic_query_plan_response import (
    SemanticQueryPlanResponse,
)
from gravitino.dto.semantic.plan_field_ref_dto import PlanFieldRefDTO
from gravitino.dto.semantic.plan_filter_dto import PlanFilterDTO
from gravitino.dto.semantic.plan_join_dto import PlanJoinDTO
from gravitino.dto.semantic.plan_selection_dto import PlanSelectionDTO
from gravitino.dto.semantic.plan_source_dto import PlanSourceDTO
from gravitino.dto.semantic.semantic_query_filter_dto import SemanticQueryFilterDTO
from gravitino.dto.semantic.semantic_query_plan_dto import SemanticQueryPlanDTO
from gravitino.name_identifier import NameIdentifier

PLAN_JSON = (
    '{"semanticModel":["metalake","catalog","schema","sales_model"],'
    '"semanticModelVersion":3,'
    '"sources":[{"dataset":"orders",'
    '"source":["catalog","schema","orders_table"]}],'
    '"joins":[],'
    '"selections":[{"name":"revenue","role":"METRIC","aggregate":true,'
    '"field":{"dataset":"orders",'
    '"expression":{"ANSI_SQL":"SUM(orders.amount)"},"datatype":"DECIMAL"}}],'
    '"filters":[],"groupBy":[],"limit":10,"warnings":[]}'
)


def expression(text: str) -> Expression:
    return Expression([DialectExpression("ANSI_SQL", text)])


def plan() -> SemanticQueryPlan:
    revenue = PlanFieldRef.of_expression(
        "orders", expression("SUM(orders.amount)"), DataType.DECIMAL
    )
    return SemanticQueryPlan(
        semantic_model=NameIdentifier.of(
            "metalake", "catalog", "schema", "sales_model"
        ),
        semantic_model_version=3,
        sources=[
            PlanSource("orders", NameIdentifier.of("catalog", "schema", "orders_table"))
        ],
        joins=[],
        selections=[
            PlanSelection.of("revenue", PlanSelection.Role.METRIC, True, revenue)
        ],
        filters=[],
        group_by=[],
        limit=10,
        warnings=[],
    )


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
            metrics=["revenue"],
            dimensions=["region"],
            filters=[
                SemanticQueryFilter.of(
                    "order_date", SemanticQueryFilter.Operator.GE, "2024-01-01"
                )
            ],
            limit=100,
        )

        request = PlanSemanticQueryRequest.from_query(query)

        self.assertEqual(
            {
                "metrics": ["revenue"],
                "dimensions": ["region"],
                "filters": [
                    {"field": "order_date", "operator": "ge", "value": "2024-01-01"}
                ],
                "limit": 100,
            },
            request.to_dict(),
        )
        request.validate()
        self.assertEqual(query, request.to_query())

    def test_from_json_and_omitted_collections(self):
        request = PlanSemanticQueryRequest.from_json(
            '{"metrics":["revenue"],"limit":25}', infer_missing=True
        )
        request.validate()

        query = request.to_query()
        self.assertEqual(["revenue"], query.metrics())
        self.assertEqual([], query.dimensions())
        self.assertEqual([], query.filters())
        self.assertEqual(25, query.limit())

    def test_validation(self):
        with self.assertRaisesRegex(
            ValueError, '"metrics"\\[0\\] field cannot be null or empty'
        ):
            PlanSemanticQueryRequest(_metrics=[""]).validate()
        with self.assertRaisesRegex(
            ValueError, '"dimensions"\\[0\\] field cannot be null or empty'
        ):
            PlanSemanticQueryRequest(_dimensions=[" "]).validate()
        with self.assertRaisesRegex(
            ValueError, '"filters"\\[0\\] field cannot be null'
        ):
            PlanSemanticQueryRequest(_filters=[None]).validate()
        with self.assertRaisesRegex(
            ValueError, '"limit" field must be null or non-negative'
        ):
            PlanSemanticQueryRequest(_metrics=["revenue"], _limit=-1).validate()
        with self.assertRaisesRegex(
            ValueError, "At least one metric or dimension must be selected"
        ):
            PlanSemanticQueryRequest().validate()


class TestSemanticQueryPlanDTO(unittest.TestCase):
    def test_from_json_and_to_plan(self):
        dto = SemanticQueryPlanDTO.from_json(PLAN_JSON, infer_missing=True)
        dto.validate()

        self.assertEqual(plan(), dto.to_plan())

    def test_from_plan_and_round_trip(self):
        dto = SemanticQueryPlanDTO.from_plan(plan())

        self.assertEqual(
            {
                "semanticModel": ["metalake", "catalog", "schema", "sales_model"],
                "semanticModelVersion": 3,
                "sources": [
                    {
                        "dataset": "orders",
                        "source": ["catalog", "schema", "orders_table"],
                    }
                ],
                "joins": [],
                "selections": [
                    {
                        "name": "revenue",
                        "role": "METRIC",
                        "aggregate": True,
                        "field": {
                            "dataset": "orders",
                            "expression": {"ANSI_SQL": "SUM(orders.amount)"},
                            "datatype": "DECIMAL",
                        },
                    }
                ],
                "filters": [],
                "groupBy": [],
                "limit": 10,
                "warnings": [],
            },
            dto.to_dict(),
        )
        self.assertEqual(plan(), dto.to_plan())

    def test_validation(self):
        cases = [
            ({"_semantic_model": None}, '"semanticModel" field is required'),
            ({"_semantic_model_version": 0}, '"semanticModelVersion" must be positive'),
            ({"_sources": []}, '"sources" field is required'),
            ({"_joins": None}, '"joins" field is required'),
            ({"_selections": []}, '"selections" field is required'),
            ({"_filters": None}, '"filters" field is required'),
            ({"_group_by": None}, '"groupBy" field is required'),
            ({"_limit": -1}, '"limit" must be null or non-negative'),
            ({"_warnings": None}, '"warnings" field is required'),
        ]
        for fields, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError, message):
                    _dto_with(fields).validate()

    def test_join_and_filter_wire_types(self):
        join = PlanJoinDTO.from_json(
            '{"type":"INNER","relationship":"orders_to_regions",'
            '"fromDataset":"orders","fromColumns":["region_id"],'
            '"toDataset":"regions","toColumns":["id"]}',
            infer_missing=True,
        )
        self.assertEqual("orders_to_regions", join.to_plan_join().relationship())
        self.assertEqual(
            join,
            PlanJoinDTO.from_plan_join(join.to_plan_join()),
        )

        field_ref = plan().selections()[0].field()
        self.assertEqual(
            field_ref,
            PlanFieldRefDTO.from_plan_field_ref(field_ref).to_plan_field_ref(),
        )
        selection = plan().selections()[0]
        self.assertEqual(
            selection,
            PlanSelectionDTO.from_plan_selection(selection).to_plan_selection(),
        )
        query_filter = PlanFilter.of(
            field_ref,
            SemanticQueryFilter.Operator.GE,
            "1",
        )
        self.assertEqual(
            query_filter,
            PlanFilterDTO.from_plan_filter(query_filter).to_plan_filter(),
        )


class TestSemanticQueryPlanResponse(unittest.TestCase):
    def test_from_json_and_to_plan(self):
        response = SemanticQueryPlanResponse.from_json(
            '{"code":0,"plan":' + PLAN_JSON + "}", infer_missing=True
        )
        response.validate()

        self.assertEqual(plan(), response.to_plan())

    def test_round_trip(self):
        response = SemanticQueryPlanResponse(
            _code=0, _plan=SemanticQueryPlanDTO.from_plan(plan())
        )

        data = response.to_dict()
        self.assertEqual(0, data["code"])
        self.assertEqual(3, data["plan"]["semanticModelVersion"])
        self.assertNotIn("sql", data)
        self.assertNotIn("dialect", data)
        response.validate()
        self.assertEqual(plan(), response.to_plan())

    def test_validation(self):
        with self.assertRaisesRegex(ValueError, "plan must not be null"):
            SemanticQueryPlanResponse(_code=0, _plan=None).validate()
        with self.assertRaisesRegex(
            ValueError, '"semanticModelVersion" must be positive'
        ):
            SemanticQueryPlanResponse(
                _code=0,
                _plan=_dto_with({"_semantic_model_version": 0}),
            ).validate()


def _dto_with(fields: dict) -> SemanticQueryPlanDTO:
    values = {
        "_semantic_model": ["metalake", "catalog", "schema", "sales_model"],
        "_semantic_model_version": 3,
        "_sources": [
            PlanSourceDTO(
                _dataset="orders",
                _source=["catalog", "schema", "orders_table"],
            )
        ],
        "_joins": [],
        "_selections": [
            PlanSelectionDTO(
                _name="revenue",
                _role="METRIC",
                _aggregate=True,
                _field=PlanFieldRefDTO(
                    _dataset="orders",
                    _expression={"ANSI_SQL": "SUM(orders.amount)"},
                    _datatype="DECIMAL",
                ),
            )
        ],
        "_filters": [],
        "_group_by": [],
        "_limit": 10,
        "_warnings": [],
    }
    values.update(fields)
    return SemanticQueryPlanDTO(**values)
