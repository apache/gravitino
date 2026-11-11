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
from unittest.mock import Mock

from gravitino.api.authorization.owner import Owner
from gravitino.api.metadata_object import MetadataObject
from gravitino.api.metadata_objects import MetadataObjects
from gravitino.api.semantic.dataset import Dataset
from gravitino.api.semantic.semantic_model_change import SemanticModelChange
from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition
from gravitino.api.tag.supports_tags import SupportsTags
from gravitino.client.generic_semantic_model import GenericSemanticModel
from gravitino.client.gravitino_metalake import GravitinoMetalake
from gravitino.client.semantic_model_catalog_operations import (
    SemanticModelCatalogOperations,
)
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.metalake_dto import MetalakeDTO
from gravitino.dto.responses.semantic_model_response import SemanticModelResponse
from gravitino.dto.semantic.semantic_model_definition_dto import (
    SemanticModelDefinitionDTO,
)
from gravitino.dto.semantic.semantic_model_dto import SemanticModelDTO
from gravitino.exceptions.base import (
    IllegalArgumentException,
    UnsupportedOperationException,
)
from gravitino.exceptions.handlers.owner_error_handler import OWNER_ERROR_HANDLER
from gravitino.exceptions.handlers.tag_error_handler import TAG_ERROR_HANDLER
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace
from gravitino.utils import HTTPClient


class TestSemanticModelTagsAndOwner(unittest.TestCase):
    tag_path = "api/metalakes/lake%20%2F/objects/semantic_model/catalog%20%2F.schema%20%2F.returned%20%2F/tags"

    def setUp(self):
        self.rest = Mock(spec=HTTPClient)
        self.namespace = Namespace.of("lake /", "catalog /", "schema /")
        self.definition = SemanticModelDefinition(
            [Dataset("orders", NameIdentifier.of("catalog", "schema", "orders"))]
        )
        self.dto = SemanticModelDTO(
            _name="returned /",
            _definition=SemanticModelDefinitionDTO.from_definition(self.definition),
            _audit=AuditDTO("creator", "2022-01-01T00:00:00Z"),
        )

    def test_default_tag_support_is_unsupported(self):
        with self.assertRaises(UnsupportedOperationException):
            self.dto.supports_tags()

    def test_model_operations_preserve_tag_context(self):
        catalog = SemanticModelCatalogOperations(
            self.rest, Namespace.of("lake /"), "catalog /"
        )
        ident = NameIdentifier.of("schema /", "original")
        response = Mock(body=SemanticModelResponse(0, self.dto).to_json())
        for verb, operation in (
            ("post", lambda: catalog.create_semantic_model(ident, "", self.definition)),
            ("get", lambda: catalog.load_semantic_model(ident)),
            (
                "post",
                lambda: catalog.import_ossie_document(
                    Namespace.of("schema /"), '{"name":"returned /"}', "json"
                ),
            ),
            (
                "put",
                lambda: catalog.alter_semantic_model(
                    ident, SemanticModelChange.rename("returned /")
                ),
            ),
        ):
            with self.subTest(operation=verb):
                self.rest.reset_mock()
                getattr(self.rest, verb).return_value = response
                model = operation()
                self.assertIsInstance(model, GenericSemanticModel)
                self.assertIsInstance(model.supports_tags(), SupportsTags)
                self.assertIs(model, model.supports_tags())
                self.rest.get.return_value = Mock(
                    body='{"code":0,"names":["reviewed"]}'
                )
                self.assertEqual(["reviewed"], model.supports_tags().list_tags())
                self.rest.get.assert_called_with(
                    self.tag_path, params={}, error_handler=TAG_ERROR_HANDLER
                )

    def test_tag_operations_use_the_shared_client_and_encoded_identity(self):
        tags = GenericSemanticModel(self.dto, self.rest, self.namespace).supports_tags()
        self.rest.get.return_value = Mock(
            body='{"code":0,"tags":[{"name":"reviewed","comment":"","properties":{},'
            '"audit":{"creator":"creator","createTime":"2022-01-01T00:00:00Z"}}]}'
        )
        self.assertEqual(["reviewed"], [tag.name() for tag in tags.list_tags_info()])
        self.rest.get.assert_called_with(
            self.tag_path, params={"details": "true"}, error_handler=TAG_ERROR_HANDLER
        )
        self.rest.get.return_value = Mock(
            body='{"code":0,"tag":{"name":"reviewed /","comment":"","properties":{},'
            '"audit":{"creator":"creator","createTime":"2022-01-01T00:00:00Z"}}}'
        )
        self.assertEqual("reviewed /", tags.get_tag("reviewed /").name())
        self.rest.get.assert_called_with(
            self.tag_path + "/reviewed%20%2F",
            params={},
            error_handler=TAG_ERROR_HANDLER,
        )
        self.rest.post.return_value = Mock(body='{"code":0,"names":["reviewed"]}')
        self.assertEqual(["reviewed"], tags.associate_tags(["reviewed"], ["old"]))
        call = self.rest.post.call_args
        self.assertEqual(self.tag_path, call.args[0])
        self.assertEqual(
            {"tagsToAdd": ["reviewed"], "tagsToRemove": ["old"]},
            call.kwargs["json"].to_dict(),
        )
        self.assertIs(TAG_ERROR_HANDLER, call.kwargs["error_handler"])
        self.rest.post.return_value = Mock(body='{"code":0,"names":[]}')
        self.assertEqual([], tags.associate_tags([], ["reviewed"]))
        self.assertEqual(
            {"tagsToAdd": [], "tagsToRemove": ["reviewed"]},
            self.rest.post.call_args.kwargs["json"].to_dict(),
        )

    def test_tag_value_assignment(self):
        tags = GenericSemanticModel(self.dto, self.rest, self.namespace).supports_tags()
        self.rest.post.return_value = Mock(body='{"code":0,"names":["reviewed"]}')
        self.assertEqual(
            ["reviewed"], tags.assign_tags([{"name": "reviewed", "value": "yes"}])
        )
        call = self.rest.post.call_args
        self.assertEqual(self.tag_path, call.args[0])
        self.assertEqual(
            {"tagsToAdd": [{"name": "reviewed", "value": "yes"}], "tagsToRemove": None},
            call.kwargs["json"].to_dict(),
        )
        self.assertEqual(
            "application/vnd.gravitino.v2+json", call.kwargs["headers"]["Content-Type"]
        )
        self.assertIs(TAG_ERROR_HANDLER, call.kwargs["error_handler"])
        self.rest.post.return_value = Mock(body='{"code":0,"names":[]}')
        self.assertEqual([], tags.assign_tags(tags_to_remove=["reviewed"]))
        self.assertEqual(
            {"tagsToAdd": None, "tagsToRemove": [{"name": "reviewed", "value": None}]},
            self.rest.post.call_args.kwargs["json"].to_dict(),
        )

    def test_invalid_tag_names_do_not_send_requests(self):
        tags = GenericSemanticModel(self.dto, self.rest, self.namespace).supports_tags()
        for name in (None, "", " "):
            with self.subTest(name=name), self.assertRaises(IllegalArgumentException):
                tags.get_tag(name)
        self.rest.get.assert_not_called()

    def test_client_context_does_not_change_value_equality(self):
        model = GenericSemanticModel(self.dto, self.rest, self.namespace)
        other = GenericSemanticModel(self.dto, Mock(spec=HTTPClient), self.namespace)
        self.assertEqual(model, other)
        self.assertEqual(hash(model), hash(other))

    def test_generic_owner_api_uses_semantic_model_type(self):
        metalake = GravitinoMetalake(
            MetalakeDTO("lake /", "", {}, AuditDTO("creator")), self.rest
        )
        obj = MetadataObjects.of(
            ["catalog /", "schema /", "returned /"], MetadataObject.Type.SEMANTIC_MODEL
        )
        path = "api/metalakes/lake%20%2F/owners/semantic_model/catalog%20%2F.schema%20%2F.returned%20%2F"
        for owner_type in (Owner.Type.USER, Owner.Type.GROUP):
            with self.subTest(owner_type=owner_type):
                self.rest.put.return_value = Mock(body='{"code":0,"set":true}')
                metalake.set_owner(obj, "finance", owner_type)
                call = self.rest.put.call_args
                self.assertEqual(path, call.args[0])
                self.assertEqual(
                    {"name": "finance", "type": owner_type},
                    call.kwargs["json"].to_dict(),
                )
                self.assertIs(OWNER_ERROR_HANDLER, call.kwargs["error_handler"])
                self.rest.get.return_value = Mock(
                    body='{"code":0,"owner":{"name":"finance","type":"'
                    + owner_type.name
                    + '"}}'
                )
                owner = metalake.get_owner(obj)
                self.assertEqual("finance", owner.name())
                self.assertEqual(owner_type, owner.type())
                self.rest.get.assert_called_with(
                    path, error_handler=OWNER_ERROR_HANDLER
                )
