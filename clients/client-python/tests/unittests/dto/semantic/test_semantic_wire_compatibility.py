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
import unittest
from decimal import Decimal, localcontext

from gravitino.api.semantic.ai_context import AIContext
from gravitino.api.semantic.ai_context_object import AIContextObject
from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition
from gravitino.dto.semantic.dataset_dto import DatasetDTO
from gravitino.dto.semantic.field_dto import FieldDTO
from gravitino.dto.semantic.metric_dto import MetricDTO
from gravitino.dto.semantic.relationship_dto import RelationshipDTO
from gravitino.dto.semantic.ai_context_object_dto import AIContextObjectDTO
from gravitino.dto.semantic.json_serdes.ai_context_serdes import AIContextSerdes
from gravitino.dto.semantic.semantic_model_definition_dto import (
    SemanticModelDefinitionDTO,
)
from gravitino.dto.semantic.semantic_model_dto import SemanticModelDTO
from tests.unittests.dto.semantic.test_semantic_model_definition_dto import (
    _complete_definition,
)


class TestSemanticWireCompatibility(unittest.TestCase):
    def test_java_wire_names(self):
        # Mirror the camelCase contract asserted by Java's
        # TestSemanticModelDefinitionDTO.testDefinitionConversionAndJsonRoundTrip.
        wire = {
            "aiContext": "Governed sales definitions",
            "datasets": [
                {
                    "name": "orders",
                    "source": {"namespace": ["sales", "mart"], "name": "orders"},
                    "primaryKey": ["order_id"],
                    "uniqueKeys": [["order_id"], ["customer_id", "order_date"]],
                    "description": "Order facts",
                    "aiContext": {
                        "instructions": "Use certified metrics only",
                        "synonyms": ["sales"],
                        "examples": ["total revenue by month"],
                        "audience": "finance",
                    },
                    "fields": [
                        {
                            "name": "order_amount",
                            "expression": {
                                "dialects": [
                                    {
                                        "dialect": "ANSI_SQL",
                                        "expression": "order_amount",
                                    }
                                ]
                            },
                            "dimension": {"isTime": False},
                            "label": "Order amount",
                            "description": "The order amount",
                            "datatype": "Decimal",
                            "aiContext": "Amount charged for the order",
                            "customExtensions": [
                                {"vendorName": "acme", "data": '{"unit": "usd"}'}
                            ],
                        }
                    ],
                    "customExtensions": [{"vendorName": "acme", "data": "{}"}],
                },
                {
                    "name": "customers",
                    "source": {"namespace": ["sales", "mart"], "name": "customers"},
                },
            ],
            "relationships": [
                {
                    "name": "orders_to_customers",
                    "from": "orders",
                    "to": "customers",
                    "fromColumns": ["customer_id"],
                    "toColumns": ["id"],
                    "aiContext": "Order to customer join",
                    "customExtensions": [{"vendorName": "acme", "data": "{}"}],
                }
            ],
            "metrics": [
                {
                    "name": "total_revenue",
                    "expression": {
                        "dialects": [
                            {
                                "dialect": "ANSI_SQL",
                                "expression": "SUM(orders.order_amount)",
                            },
                            {
                                "dialect": "SNOWFLAKE",
                                "expression": "SUM(ORDERS.ORDER_AMOUNT)",
                            },
                        ]
                    },
                    "description": "Total revenue across all orders",
                    "datatype": "Decimal",
                    "aiContext": "Certified revenue metric",
                    "customExtensions": [{"vendorName": "acme", "data": "{}"}],
                }
            ],
            "customExtensions": [
                {"vendorName": "acme", "data": '{"owner": "finance"}'}
            ],
        }
        definition = _complete_definition()
        restored = SemanticModelDefinitionDTO.from_json(json.dumps(wire))
        self.assertEqual(definition, restored.to_definition())
        self.assertEqual(
            wire,
            json.loads(
                SemanticModelDefinitionDTO.from_definition(definition).to_json()
            ),
        )

    def test_decimal_json_numbers(self):
        # Same precise JSON number used by Java's AI-context serde regression.
        number = "0.123456789012345678901234567890"
        context_json = (
            '{"nested": [{"value": NUMBER}], '
            '"integer": 123456789012345678901234567890, '
            '"flag": true, "empty": null}'
        )
        for dto_type in (
            DatasetDTO,
            FieldDTO,
            MetricDTO,
            RelationshipDTO,
            SemanticModelDefinitionDTO,
            SemanticModelDTO,
        ):
            with self.subTest(dto=dto_type), localcontext() as context:
                context.prec = 6
                wire = '{"aiContext": ' + context_json.replace("NUMBER", number) + "}"
                if dto_type is SemanticModelDTO:
                    wire = '{"name": "sales", "definition": ' + wire + "}"
                dto = dto_type.from_json(wire)
                self.assertEqual(
                    json.loads(wire, parse_float=Decimal),
                    json.loads(dto.to_json(), parse_float=Decimal),
                )

    def test_decimal_encoding_from_api(self):
        value = Decimal("0.123456789012345678901234567890")
        definition = SemanticModelDefinition(
            datasets=_complete_definition().datasets(),
            ai_context=AIContext.of(
                AIContextObject(additional_properties={"nested": [{"value": value}]})
            ),
        )
        dto = SemanticModelDefinitionDTO.from_definition(definition)
        wire = dto.to_json()
        self.assertEqual(
            value,
            json.loads(wire, parse_float=Decimal)["aiContext"]["nested"][0]["value"],
        )
        self.assertEqual(
            dto.to_definition(),
            SemanticModelDefinitionDTO.from_json(wire).to_definition(),
        )

    def test_explicit_null_optional_properties(self):
        value = {
            "instructions": None,
            "synonyms": None,
            "examples": None,
            "unknown": None,
        }
        context = AIContextSerdes.deserialize(value)
        self.assertEqual({"unknown": None}, AIContextSerdes.serialize(context))
        for name in ("synonyms", "examples"):
            with self.subTest(name=name):
                self.assertEqual(
                    {name: []},
                    AIContextSerdes.serialize(AIContextSerdes.deserialize({name: []})),
                )
                with self.assertRaises(ValueError):
                    AIContextSerdes.deserialize({name: [None]})
        with self.assertRaises(ValueError):
            AIContextSerdes.deserialize({"instructions": 42})

    def test_nested_value_equality(self):
        for left, right, equal in (
            (True, 1, False),
            (1, 1.0, False),
            (0.1, Decimal("0.1"), True),
        ):
            with self.subTest(left=left, right=right):
                first = AIContextObjectDTO(
                    additional_properties={"nested": [{"value": left}]}
                )
                second = AIContextObjectDTO(
                    additional_properties={"nested": [{"value": right}]}
                )
                self.assertEqual(equal, first == second)
                self.assertEqual(
                    first.to_ai_context_object() == second.to_ai_context_object(),
                    first == second,
                )
                if equal:
                    self.assertEqual(hash(first), hash(second))
                    self.assertEqual("found", {first: "found"}[second])
                definitions = [
                    SemanticModelDefinitionDTO(
                        _ai_context=AIContextSerdes.deserialize(
                            {"nested": [{"value": value}]}
                        )
                    )
                    for value in (left, right)
                ]
                self.assertEqual(equal, definitions[0] == definitions[1])
                self.assertEqual(
                    equal,
                    SemanticModelDTO(_definition=definitions[0])
                    == SemanticModelDTO(_definition=definitions[1]),
                )

    def test_json_formatting_and_decoder_options(self):
        dto = SemanticModelDefinitionDTO.from_dict(
            {"aiContext": {"value": Decimal("1.25")}}
        )
        wire = dto.to_json(indent=2, sort_keys=True, ensure_ascii=False)
        self.assertIn("\n", wire)
        restored = SemanticModelDefinitionDTO.from_json(
            wire.encode(), parse_float=float
        )
        self.assertIsInstance(
            restored.ai_context().object().additional_properties()["value"], float
        )
        self.assertEqual(dto, restored)

    def test_whole_decimals_keep_their_type_through_json(self):
        for text in (
            "1",
            "0",
            "-1",
            "-0",
            "123456789012345678901234567890",
            "1E+30",
            "1.00",
        ):
            with self.subTest(value=text), localcontext() as context:
                context.prec = 6
                value = Decimal(text)
                definition = SemanticModelDefinition(
                    datasets=_complete_definition().datasets(),
                    ai_context=AIContext.of(
                        AIContextObject(
                            additional_properties={
                                "value": value,
                                "nested": [
                                    {"value": value, "integer": 1, "flag": True}
                                ],
                            }
                        )
                    ),
                )
                dto = SemanticModelDefinitionDTO.from_definition(definition)
                for original in (dto, SemanticModelDTO(_name="sales", _definition=dto)):
                    wire = original.to_json()
                    restored = type(original).from_json(wire)
                    restored_definition = (
                        restored.definition()
                        if isinstance(restored, SemanticModelDTO)
                        else restored.to_definition()
                    )
                    properties = (
                        restored_definition.ai_context()
                        .object()
                        .additional_properties()
                    )
                    self.assertIsInstance(properties["value"], Decimal)
                    self.assertIsInstance(properties["nested"][0]["value"], Decimal)
                    self.assertIs(type(properties["nested"][0]["integer"]), int)
                    self.assertIs(type(properties["nested"][0]["flag"]), bool)
                    self.assertEqual(value, properties["value"])
                    self.assertEqual(value.is_signed(), properties["value"].is_signed())
                    self.assertEqual(definition, restored_definition)
                    self.assertEqual(hash(definition), hash(restored_definition))
                    self.assertEqual(
                        "found", {definition: "found"}[restored_definition]
                    )
                    self.assertEqual(original, restored)
                self.assertEqual(
                    value.as_tuple(),
                    dto.ai_context()
                    .object()
                    .additional_properties()["value"]
                    .as_tuple(),
                )
