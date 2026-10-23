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
from unittest.mock import patch
from urllib.error import HTTPError

import simplejson

from gravitino.api.semantic.ai_context import AIContext
from gravitino.api.semantic.ai_context_object import AIContextObject
from gravitino.api.semantic.custom_extension import CustomExtension
from gravitino.api.semantic.data_type import DataType
from gravitino.api.semantic.dialect_expression import DialectExpression
from gravitino.api.semantic.expression import Expression
from gravitino.api.semantic.metric import Metric
from gravitino.api.semantic.semantic_model_metric import SemanticModelMetric
from gravitino.client.relational_catalog import RelationalCatalog
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.responses.metric_info_list_response import MetricInfoListResponse
from gravitino.exceptions.base import (
    ForbiddenException,
    IllegalArgumentException,
    IllegalNamespaceException,
    InternalError,
    NoSuchCatalogException,
    NoSuchMetalakeException,
    NoSuchSchemaException,
    UnauthorizedException,
    UnsupportedOperationException,
)
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace
from gravitino.utils.http_client import HTTPClient


class TestSemanticModelMetricDiscovery(unittest.TestCase):
    def setUp(self):
        self.models = RelationalCatalog(
            catalog_namespace=Namespace.of("metalake /雪"),
            name="catalog +%",
            catalog_type=RelationalCatalog.Type.RELATIONAL,
            provider="lakehouse-generic",
            audit=AuditDTO("test"),
            rest_client=HTTPClient("http://localhost:8090"),
        ).as_semantic_model_catalog()
        self.namespace = Namespace.of("schema #?")
        self.full_namespace = ["metalake /雪", "catalog +%", "schema #?", "a.model"]
        self.path = (
            "http://localhost:8090/api/metalakes/metalake%20%2F%E9%9B%AA"
            "/catalogs/catalog%20%2B%25/schemas/schema%20%23%3F/semantic-model-metrics"
        )
        opener_patch = patch("gravitino.utils.http_client.build_opener")
        self.opener = opener_patch.start().return_value
        self.addCleanup(opener_patch.stop)
        self.opener.open.return_value.getcode.return_value = 200

    def test_list_metrics_uses_one_get_and_preserves_namespaces_and_order(self):
        identifiers = [
            {"namespace": self.full_namespace, "name": name}
            for name in ("Revenue", "revenue", "收入 /é+%")
        ] + [{"namespace": self.full_namespace[:-1] + ["b"], "name": "Revenue"}]
        self._respond({"code": 0, "identifiers": identifiers})

        actual = self.models.list_metrics(self.namespace)

        self._assert_request("false")
        self.assertEqual(
            [
                NameIdentifier.of(*item["namespace"][2:], item["name"])
                for item in identifiers
            ],
            actual,
        )

    def test_list_metric_infos_preserves_complete_metrics_and_precision(self):
        metric = self._metric_json()
        text_metric = {**metric, "name": "revenue", "aiContext": "Use certified values"}
        minimal_metric = {"name": "Revenue", "expression": metric["expression"]}
        self._respond(
            {
                "code": 0,
                "metrics": [
                    {"namespace": self.full_namespace, "metric": metric},
                    {"namespace": self.full_namespace, "metric": text_metric},
                    {
                        "namespace": self.full_namespace[:-1] + ["b"],
                        "metric": minimal_metric,
                    },
                ],
            }
        )

        actual = self.models.list_metric_infos(self.namespace)

        self._assert_request("true")
        expression = Expression(
            [
                DialectExpression("ANSI_SQL", "SUM(orders.amount)"),
                DialectExpression("SPARK_SQL", "sum(orders.amount)"),
            ]
        )
        expected = Metric(
            "Revenue",
            expression,
            "Revenue - café 雪",
            DataType.DECIMAL,
            AIContext.of(
                AIContextObject(
                    instructions="Use certified values",
                    synonyms=["Sales"],
                    examples=["Revenue by year"],
                    additional_properties={
                        "nested": {
                            "precise": Decimal("0.12345678901234567890123456789"),
                            "large": 1234567890123456789012345678901234567890,
                            "values": [True, None, "extra"],
                        }
                    },
                )
            ),
            [CustomExtension("vendor", '{"opaque": "雪"}')],
        )
        self.assertEqual(
            SemanticModelMetric(Namespace.of("schema #?", "a.model"), expected),
            actual[0],
        )
        self.assertEqual(
            ["Revenue", "revenue", "Revenue"], [info.metric().name() for info in actual]
        )
        self.assertEqual("Use certified values", actual[1].metric().ai_context().text())
        self.assertEqual(Namespace.of("schema #?", "b"), actual[2].namespace())
        self.assertEqual(Metric("Revenue", expression), actual[2].metric())
        self.assertIsInstance(
            actual[0]
            .metric()
            .ai_context()
            .object()
            .additional_properties()["nested"]["precise"],
            Decimal,
        )

    def test_empty_responses(self):
        for method, field, details in (
            (self.models.list_metrics, "identifiers", "false"),
            (self.models.list_metric_infos, "metrics", "true"),
        ):
            with self.subTest(details=details):
                self.opener.open.reset_mock()
                self._respond({"code": 0, field: []})
                self.assertEqual([], method(self.namespace))
                self._assert_request(details)

    def test_invalid_namespaces_do_not_send_requests(self):
        for method in (self.models.list_metrics, self.models.list_metric_infos):
            for namespace in (
                None,
                Namespace.empty(),
                Namespace.of("catalog", "schema"),
            ):
                with self.subTest(
                    method=method, namespace=namespace
                ), self.assertRaises(IllegalNamespaceException):
                    method(namespace)
        self.opener.open.assert_not_called()

    def test_invalid_response_envelopes(self):
        for method, field in (
            (self.models.list_metrics, "identifiers"),
            (self.models.list_metric_infos, "metrics"),
        ):
            for body in (
                {"code": 0},
                {"code": 0, field: None},
                {"code": -1, field: []},
            ):
                with self.subTest(method=method, body=body), self.assertRaises(
                    IllegalArgumentException
                ):
                    self._respond(body)
                    method(self.namespace)

    def test_invalid_full_namespaces_are_rejected(self):
        for namespace in (
            [],
            ["schema", "model"],
            ["lake", "catalog", "schema"],
            self.full_namespace + ["extra"],
        ):
            with self.subTest(namespace=namespace):
                self._respond(
                    {
                        "code": 0,
                        "identifiers": [{"namespace": namespace, "name": "Revenue"}],
                    }
                )
                with self.assertRaises(IllegalArgumentException):
                    self.models.list_metrics(self.namespace)
                self._respond(
                    {
                        "code": 0,
                        "metrics": [
                            {"namespace": namespace, "metric": self._metric_json()}
                        ],
                    }
                )
                with self.assertRaises(IllegalArgumentException):
                    self.models.list_metric_infos(self.namespace)

    def test_invalid_metric_fails_entire_detailed_request(self):
        valid = {"namespace": self.full_namespace, "metric": self._metric_json()}
        for invalid in (
            None,
            {},
            {"metric": self._metric_json()},
            {"namespace": self.full_namespace},
            {"namespace": self.full_namespace, "metric": {"name": "broken"}},
            {
                "namespace": self.full_namespace,
                "metric": {**self._metric_json(), "name": ""},
            },
            {
                "namespace": self.full_namespace,
                "metric": {**self._metric_json(), "expression": {"dialects": []}},
            },
        ):
            with self.subTest(invalid=invalid), self.assertRaises(
                IllegalArgumentException
            ):
                self._respond({"code": 0, "metrics": [valid, invalid]})
                self.models.list_metric_infos(self.namespace)

    def test_http_error_envelopes_map_to_sdk_exceptions(self):
        cases = (
            (404, 1003, NoSuchMetalakeException),
            (404, 1003, NoSuchCatalogException),
            (404, 1003, NoSuchSchemaException),
            (401, 1011, UnauthorizedException),
            (403, 1008, ForbiddenException),
            (500, 1002, InternalError),
            (501, 1006, UnsupportedOperationException),
        )
        for method in (self.models.list_metrics, self.models.list_metric_infos):
            for status, code, exception in cases:
                with self.subTest(method=method, exception=exception):
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
                        method(self.namespace)

    def test_metric_info_response_json_round_trip(self):
        body = {
            "code": 0,
            "metrics": [
                {"namespace": self.full_namespace, "metric": self._metric_json()}
            ],
        }
        response = MetricInfoListResponse.from_json(simplejson.dumps(body))
        response.validate()
        self.assertEqual(body, simplejson.loads(response.to_json(), use_decimal=True))

    def test_result_value_semantics_and_namespace_copy(self):
        namespace = Namespace.of("schema", "model")
        metric = Metric(
            "Revenue", Expression([DialectExpression("ANSI_SQL", "COUNT(*)")])
        )
        result = SemanticModelMetric(namespace, metric)
        equal = SemanticModelMetric(Namespace.of("schema", "model"), metric)
        original_hash = hash(result)
        namespace.levels().clear()
        result.namespace().levels().clear()
        self.assertEqual(equal, result)
        self.assertEqual(original_hash, hash(result))
        self.assertEqual(hash(equal), hash(result))
        self.assertNotEqual(result, metric)
        self.assertNotEqual(
            result, SemanticModelMetric(Namespace.of("schema", "other"), metric)
        )
        self.assertIn("Revenue", repr(result))
        self.assertIn("model", repr(result))
        for namespace, value in (
            (None, metric),
            (Namespace.of("schema", "model"), None),
        ):
            with self.assertRaises(IllegalArgumentException):
                SemanticModelMetric(namespace, value)

    def _respond(self, body):
        self.opener.open.return_value.read.return_value = simplejson.dumps(body).encode(
            "utf-8"
        )

    def _assert_request(self, details):
        self.opener.open.assert_called_once()
        request = self.opener.open.call_args.args[0]
        self.assertEqual(self.path + "?details=" + details, request.full_url)
        self.assertEqual("GET", request.get_method())
        self.assertIsNone(request.data)
        self.assertEqual(
            "application/vnd.gravitino.v1+json", request.get_header("Accept")
        )

    @staticmethod
    def _metric_json():
        return {
            "name": "Revenue",
            "expression": {
                "dialects": [
                    {"dialect": "ANSI_SQL", "expression": "SUM(orders.amount)"},
                    {"dialect": "SPARK_SQL", "expression": "sum(orders.amount)"},
                ]
            },
            "description": "Revenue - café 雪",
            "datatype": "Decimal",
            "aiContext": {
                "instructions": "Use certified values",
                "synonyms": ["Sales"],
                "examples": ["Revenue by year"],
                "nested": {
                    "precise": Decimal("0.12345678901234567890123456789"),
                    "large": 1234567890123456789012345678901234567890,
                    "values": [True, None, "extra"],
                },
            },
            "customExtensions": [{"vendorName": "vendor", "data": '{"opaque": "雪"}'}],
        }
