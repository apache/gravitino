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
from gravitino.api.semantic.dimension import Dimension
from gravitino.api.semantic.expression import Expression
from gravitino.api.semantic.field import Field
from gravitino.api.semantic.metric import Metric
from gravitino.api.semantic.relationship import Relationship
from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition
from gravitino.dto.semantic.semantic_model_definition_dto import (
    SemanticModelDefinitionDTO,
)
from gravitino.name_identifier import NameIdentifier

_MINIMAL_DEFINITION_JSON = {
    "datasets": [
        {"name": "orders", "source": {"namespace": ["sales", "mart"], "name": "orders"}}
    ]
}


def _minimal_definition() -> SemanticModelDefinition:
    return SemanticModelDefinition(
        datasets=[Dataset("orders", NameIdentifier.of("sales", "mart", "orders"))]
    )


def _complete_definition() -> SemanticModelDefinition:
    order_amount = Field(
        name="order_amount",
        expression=Expression([DialectExpression(Dialects.ANSI_SQL, "order_amount")]),
        dimension=Dimension(is_time=False),
        label="Order amount",
        description="The order amount",
        datatype=DataType.DECIMAL,
        ai_context=AIContext.of("Amount charged for the order"),
        custom_extensions=[CustomExtension("acme", '{"unit": "usd"}')],
    )
    orders = Dataset(
        name="orders",
        source=NameIdentifier.of("sales", "mart", "orders"),
        primary_key=["order_id"],
        unique_keys=[["order_id"], ["customer_id", "order_date"]],
        description="Order facts",
        ai_context=AIContext.of(
            AIContextObject(
                instructions="Use certified metrics only",
                synonyms=["sales"],
                examples=["total revenue by month"],
                additional_properties={"audience": "finance"},
            )
        ),
        fields=[order_amount],
        custom_extensions=[CustomExtension("acme", "{}")],
    )
    customers = Dataset("customers", NameIdentifier.of("sales", "mart", "customers"))
    relationship = Relationship(
        name="orders_to_customers",
        from_dataset="orders",
        to_dataset="customers",
        from_columns=["customer_id"],
        to_columns=["id"],
        ai_context=AIContext.of("Order to customer join"),
        custom_extensions=[CustomExtension("acme", "{}")],
    )
    total_revenue = Metric(
        name="total_revenue",
        expression=Expression(
            [
                DialectExpression(Dialects.ANSI_SQL, "SUM(orders.order_amount)"),
                DialectExpression(Dialects.SNOWFLAKE, "SUM(ORDERS.ORDER_AMOUNT)"),
            ]
        ),
        description="Total revenue across all orders",
        datatype=DataType.DECIMAL,
        ai_context=AIContext.of("Certified revenue metric"),
        custom_extensions=[CustomExtension("acme", "{}")],
    )
    return SemanticModelDefinition(
        datasets=[orders, customers],
        ai_context=AIContext.of("Governed sales definitions"),
        relationships=[relationship],
        metrics=[total_revenue],
        custom_extensions=[CustomExtension("acme", '{"owner": "finance"}')],
    )


class TestSemanticModelDefinitionDTO(unittest.TestCase):
    def test_minimal_definition_serializes_without_optional_fields(self):
        dto = SemanticModelDefinitionDTO.from_definition(_minimal_definition())

        self.assertEqual(_MINIMAL_DEFINITION_JSON, dto.to_dict())

    def test_minimal_definition_deserializes(self):
        dto = SemanticModelDefinitionDTO.from_dict(_MINIMAL_DEFINITION_JSON)

        self.assertEqual(_minimal_definition(), dto.to_definition())

    def test_complete_definition_round_trips_through_json(self):
        definition = _complete_definition()

        dto = SemanticModelDefinitionDTO.from_definition(definition)
        restored = SemanticModelDefinitionDTO.from_json(dto.to_json())

        self.assertEqual(definition, restored.to_definition())

    def test_ai_context_string_serializes_as_a_bare_string(self):
        definition = SemanticModelDefinition(
            datasets=_minimal_definition().datasets(),
            ai_context=AIContext.of("Governed sales definitions"),
        )

        serialized = SemanticModelDefinitionDTO.from_definition(definition).to_dict()

        self.assertEqual("Governed sales definitions", serialized["ai_context"])

    def test_ai_context_object_flattens_additional_properties(self):
        ai_context = AIContext.of(
            AIContextObject(
                instructions="Use certified metrics only",
                synonyms=["sales"],
                additional_properties={"audience": "finance", "priority": 1},
            )
        )
        definition = SemanticModelDefinition(
            datasets=_minimal_definition().datasets(), ai_context=ai_context
        )

        serialized = SemanticModelDefinitionDTO.from_definition(definition).to_dict()

        self.assertEqual(
            {
                "instructions": "Use certified metrics only",
                "synonyms": ["sales"],
                "audience": "finance",
                "priority": 1,
            },
            serialized["ai_context"],
        )

    def test_ai_context_object_round_trips_additional_properties(self):
        ai_context = AIContext.of(
            AIContextObject(
                additional_properties={
                    "audience": "finance",
                    "thresholds": {"warn": 10, "limits": [1, 2.5, True, None]},
                }
            )
        )
        definition = SemanticModelDefinition(
            datasets=_minimal_definition().datasets(), ai_context=ai_context
        )

        dto = SemanticModelDefinitionDTO.from_definition(definition)
        restored = SemanticModelDefinitionDTO.from_json(dto.to_json())

        self.assertEqual(definition, restored.to_definition())

    def test_datatype_uses_ossie_wire_values(self):
        definition = _complete_definition()

        serialized = SemanticModelDefinitionDTO.from_definition(definition).to_dict()

        self.assertEqual("Decimal", serialized["datasets"][0]["fields"][0]["datatype"])
        self.assertEqual("Decimal", serialized["metrics"][0]["datatype"])

    def test_relationship_uses_ossie_field_names(self):
        definition = _complete_definition()

        serialized = SemanticModelDefinitionDTO.from_definition(definition).to_dict()
        relationship = serialized["relationships"][0]

        self.assertEqual("orders", relationship["from"])
        self.assertEqual("customers", relationship["to"])
        self.assertEqual(["customer_id"], relationship["from_columns"])
        self.assertEqual(["id"], relationship["to_columns"])

    def test_dataset_source_uses_name_identifier_encoding(self):
        definition = _complete_definition()

        serialized = SemanticModelDefinitionDTO.from_definition(definition).to_dict()

        self.assertEqual(
            {"namespace": ["sales", "mart"], "name": "orders"},
            serialized["datasets"][0]["source"],
        )

    def test_to_definition_rejects_an_empty_definition(self):
        dto = SemanticModelDefinitionDTO.from_dict({"datasets": []})

        with self.assertRaisesRegex(ValueError, "datasets must not be null or empty"):
            dto.to_definition()

    def test_to_definition_rejects_an_unknown_datatype(self):
        with self.assertRaisesRegex(
            ValueError, "Unknown Semantic Model data type: Money"
        ):
            SemanticModelDefinitionDTO.from_dict(
                {
                    "datasets": [
                        {
                            "name": "orders",
                            "source": {
                                "namespace": ["sales", "mart"],
                                "name": "orders",
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
                                    "datatype": "Money",
                                }
                            ],
                        }
                    ]
                }
            )
