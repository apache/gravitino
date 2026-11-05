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

from gravitino.api.semantic.dataset import Dataset
from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.semantic.semantic_model_definition_dto import (
    SemanticModelDefinitionDTO,
)
from gravitino.dto.semantic.semantic_model_dto import SemanticModelDTO
from gravitino.name_identifier import NameIdentifier

_SEMANTIC_MODEL_JSON = {
    "name": "sales_model",
    "comment": "Governed sales definitions",
    "definition": {
        "datasets": [
            {
                "name": "orders",
                "source": {"namespace": ["sales", "mart"], "name": "orders"},
            }
        ]
    },
    "properties": {"owner": "finance"},
    "audit": {"creator": "gravitino"},
}


def _definition() -> SemanticModelDefinition:
    return SemanticModelDefinition(
        datasets=[Dataset("orders", NameIdentifier.of("sales", "mart", "orders"))]
    )


class TestSemanticModelDTO(unittest.TestCase):
    def test_deserializes_a_semantic_model(self):
        dto = SemanticModelDTO.from_dict(_SEMANTIC_MODEL_JSON)

        self.assertEqual("sales_model", dto.name())
        self.assertEqual("Governed sales definitions", dto.comment())
        self.assertEqual(_definition(), dto.definition())
        self.assertEqual({"owner": "finance"}, dto.properties())
        self.assertEqual("gravitino", dto.audit_info().creator())

    def test_serializes_a_semantic_model(self):
        dto = SemanticModelDTO(
            _name="sales_model",
            _comment="Governed sales definitions",
            _definition=SemanticModelDefinitionDTO.from_definition(_definition()),
            _properties={"owner": "finance"},
            _audit=AuditDTO(_creator="gravitino"),
        )

        serialized = dto.to_dict()

        self.assertEqual("sales_model", serialized["name"])
        self.assertEqual("Governed sales definitions", serialized["comment"])
        self.assertEqual(_SEMANTIC_MODEL_JSON["definition"], serialized["definition"])
        self.assertEqual({"owner": "finance"}, serialized["properties"])
        self.assertEqual("gravitino", serialized["audit"]["creator"])

    def test_round_trips_through_json(self):
        dto = SemanticModelDTO.from_dict(_SEMANTIC_MODEL_JSON)

        restored = SemanticModelDTO.from_json(dto.to_json())

        self.assertEqual(dto, restored)

    def test_omits_an_unset_comment(self):
        dto = SemanticModelDTO(
            _name="sales_model",
            _definition=SemanticModelDefinitionDTO.from_definition(_definition()),
            _properties={},
            _audit=AuditDTO(_creator="gravitino"),
        )

        self.assertNotIn("comment", dto.to_dict())
        self.assertIsNone(dto.comment())

    def test_properties_default_to_an_empty_mapping(self):
        dto = SemanticModelDTO(
            _name="sales_model",
            _definition=SemanticModelDefinitionDTO.from_definition(_definition()),
            _audit=AuditDTO(_creator="gravitino"),
        )

        self.assertEqual({}, dto.properties())

    def test_definition_requires_a_definition(self):
        dto = SemanticModelDTO(_name="sales_model", _audit=AuditDTO(_creator="a"))

        self.assertIsNone(dto.definition_dto())
        with self.assertRaisesRegex(ValueError, "definition must not be null"):
            dto.definition()
