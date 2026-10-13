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
from decimal import Decimal

from gravitino.api.semantic.ai_context import AIContext
from gravitino.api.semantic.ai_context_object import AIContextObject
from gravitino.api.semantic.dataset import Dataset
from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.requests.semantic_model_create_request import (
    SemanticModelCreateRequest,
)
from gravitino.dto.requests.semantic_model_update_request import (
    SemanticModelUpdateRequest,
)
from gravitino.dto.requests.semantic_model_updates_request import (
    SemanticModelUpdatesRequest,
)
from gravitino.dto.responses.semantic_model_response import SemanticModelResponse
from gravitino.dto.semantic.semantic_model_definition_dto import (
    SemanticModelDefinitionDTO,
)
from gravitino.dto.semantic.semantic_model_dto import SemanticModelDTO
from gravitino.name_identifier import NameIdentifier


class TestSemanticModelREST(unittest.TestCase):
    def setUp(self):
        self.definition = SemanticModelDefinition(
            datasets=[Dataset("orders", NameIdentifier.of("sales", "mart", "orders"))],
            ai_context=AIContext.of(
                AIContextObject(
                    additional_properties={
                        "nested": [
                            Decimal("1"),
                            Decimal("0.123456789012345678901234567890"),
                        ]
                    }
                )
            ),
        )
        self.dto = SemanticModelDefinitionDTO.from_definition(self.definition)

    def test_create_json_preserves_definition(self):
        request = SemanticModelCreateRequest(
            _name="sales", _definition=self.dto, _properties={}
        )
        request.validate()
        restored = SemanticModelCreateRequest.from_json(request.to_json())
        restored.validate()
        self.assertEqual(self.definition, restored.to_definition())
        self.assertNotIn("comment", json.loads(request.to_json()))

    def test_replace_json_preserves_definition(self):
        update = SemanticModelUpdateRequest.ReplaceSemanticModelDefinitionRequest(
            _definition=self.dto
        )
        request = SemanticModelUpdatesRequest(_updates=[update])
        request.validate()
        payload = json.loads(request.to_json(), parse_float=Decimal)
        self.assertEqual("replaceDefinition", payload["updates"][0]["@type"])
        restored = SemanticModelDefinitionDTO.from_dict(
            payload["updates"][0]["definition"]
        )
        self.assertEqual(self.definition, restored.to_definition())
        self.assertEqual(
            update.semantic_model_change(),
            type(update).from_json(update.to_json()).semantic_model_change(),
        )

    def test_response_json_preserves_definition(self):
        response = SemanticModelResponse(
            _code=0,
            _semantic_model=SemanticModelDTO(
                _name="sales", _definition=self.dto, _audit=AuditDTO("creator")
            ),
        )
        restored = SemanticModelResponse.from_json(response.to_json())
        restored.validate()
        self.assertEqual(self.definition, restored.semantic_model().definition())

    def test_update_wire_contract(self):
        for update, expected in (
            (
                SemanticModelUpdateRequest.RenameSemanticModelRequest("new"),
                {"@type": "rename", "newName": "new"},
            ),
            (
                SemanticModelUpdateRequest.UpdateSemanticModelCommentRequest(None),
                {"@type": "updateComment", "newComment": None},
            ),
            (
                SemanticModelUpdateRequest.SetSemanticModelPropertyRequest(
                    "owner", "team"
                ),
                {"@type": "setProperty", "property": "owner", "value": "team"},
            ),
            (
                SemanticModelUpdateRequest.RemoveSemanticModelPropertyRequest("owner"),
                {"@type": "removeProperty", "property": "owner"},
            ),
        ):
            with self.subTest(update=type(update)):
                update.validate()
                self.assertEqual(expected, json.loads(update.to_json()))
                self.assertEqual(
                    update.semantic_model_change(),
                    type(update).from_json(update.to_json()).semantic_model_change(),
                )

    def test_invalid_envelopes(self):
        for request in (
            SemanticModelCreateRequest(_name="", _definition=self.dto, _properties={}),
            SemanticModelCreateRequest(_name="sales", _properties={}),
            SemanticModelUpdatesRequest(_updates=[]),
            SemanticModelUpdatesRequest(_updates=[None]),
            SemanticModelResponse(_code=0, _semantic_model=None),
        ):
            with self.subTest(request=type(request)), self.assertRaises(ValueError):
                request.validate()
