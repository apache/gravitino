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
from unittest.mock import Mock, patch

from gravitino.api.semantic.dataset import Dataset
from gravitino.api.semantic.semantic_model_catalog import SemanticModelCatalog
from gravitino.api.semantic.semantic_model_change import SemanticModelChange
from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition
from gravitino.client.relational_catalog import RelationalCatalog
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.entity_list_response import EntityListResponse
from gravitino.dto.responses.semantic_model_response import SemanticModelResponse
from gravitino.dto.semantic.semantic_model_definition_dto import (
    SemanticModelDefinitionDTO,
)
from gravitino.dto.semantic.semantic_model_dto import SemanticModelDTO
from gravitino.exceptions.base import (
    IllegalArgumentException,
    IllegalNameIdentifierException,
    IllegalNamespaceException,
    NoSuchSchemaException,
    NoSuchSemanticModelException,
    SemanticModelAlreadyExistsException,
)
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace
from gravitino.utils import HTTPClient, Response


def _definition() -> SemanticModelDefinition:
    return SemanticModelDefinition(
        datasets=[Dataset("orders", NameIdentifier.of("sales", "mart", "orders"))]
    )


class TestSemanticModelCatalog(unittest.TestCase):
    metalake_name = "test_metalake"
    catalog_name = "test_catalog"
    schema_name = "test_schema"
    model_name = "sales_model"
    catalog_namespace = Namespace.of(metalake_name)
    request_path = (
        f"api/metalakes/{metalake_name}/catalogs/{catalog_name}"
        f"/schemas/{schema_name}/semantic-models"
    )

    @classmethod
    def setUpClass(cls) -> None:
        cls.rest_client = HTTPClient("http://localhost:8090")
        cls.catalog = RelationalCatalog(
            catalog_namespace=cls.catalog_namespace,
            name=cls.catalog_name,
            catalog_type=RelationalCatalog.Type.RELATIONAL,
            provider="test_provider",
            audit=AuditDTO("anonymous"),
            rest_client=cls.rest_client,
        )

    def _get_mock_http_resp(self, json_str: str, return_code: int = 200) -> Response:
        mock_http_resp = Mock()
        mock_http_resp.getcode.return_value = return_code
        mock_http_resp.read.return_value = json_str.encode("utf-8")
        mock_http_resp.info.return_value = None
        mock_http_resp.url = None
        return Response(mock_http_resp)

    def _mock_semantic_model_response(self, comment: str = "comment") -> Response:
        semantic_model = SemanticModelDTO(
            _name=self.model_name,
            _comment=comment,
            _definition=SemanticModelDefinitionDTO.from_definition(_definition()),
            _properties={"owner": "finance"},
            _audit=AuditDTO("creator", "2022-01-01T00:00:00Z"),
        )
        return self._get_mock_http_resp(
            SemanticModelResponse(_code=0, _semantic_model=semantic_model).to_json()
        )

    def test_relational_catalog_exposes_semantic_models(self):
        self.assertIsInstance(
            self.catalog.as_semantic_model_catalog(), SemanticModelCatalog
        )

    def test_list_semantic_models(self):
        first = NameIdentifier.of(
            self.metalake_name, self.catalog_name, self.schema_name, "sales_model"
        )
        second = NameIdentifier.of(
            self.metalake_name, self.catalog_name, self.schema_name, "orders_model"
        )
        mock_resp = self._get_mock_http_resp(
            EntityListResponse(_code=0, _idents=[first, second]).to_json()
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ) as mock_get:
            models = self.catalog.as_semantic_model_catalog().list_semantic_models(
                Namespace.of(self.schema_name)
            )

        self.assertEqual([self.request_path], [mock_get.call_args.args[0]])
        self.assertEqual(["sales_model", "orders_model"], [m.name() for m in models])
        self.assertEqual(
            [self.schema_name, self.schema_name],
            [m.namespace().level(0) for m in models],
        )

    def test_list_semantic_models_maps_a_missing_schema(self):
        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=NoSuchSchemaException("schema not found"),
        ):
            with self.assertRaises(NoSuchSchemaException):
                self.catalog.as_semantic_model_catalog().list_semantic_models(
                    Namespace.of(self.schema_name)
                )

    def test_load_semantic_model(self):
        identifier = NameIdentifier.of(self.schema_name, self.model_name)

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=self._mock_semantic_model_response(),
        ) as mock_get:
            model = self.catalog.as_semantic_model_catalog().load_semantic_model(
                identifier
            )

        self.assertEqual(
            f"{self.request_path}/{self.model_name}", mock_get.call_args.args[0]
        )
        self.assertEqual(self.model_name, model.name())
        self.assertEqual("comment", model.comment())
        self.assertEqual(_definition(), model.definition())
        self.assertEqual({"owner": "finance"}, model.properties())
        self.assertEqual("creator", model.audit_info().creator())

    def test_load_semantic_model_maps_a_missing_model(self):
        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=NoSuchSemanticModelException("semantic model not found"),
        ):
            with self.assertRaises(NoSuchSemanticModelException):
                self.catalog.as_semantic_model_catalog().load_semantic_model(
                    NameIdentifier.of(self.schema_name, self.model_name)
                )

    def test_semantic_model_exists(self):
        identifier = NameIdentifier.of(self.schema_name, self.model_name)

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=self._mock_semantic_model_response(),
        ):
            self.assertTrue(
                self.catalog.as_semantic_model_catalog().semantic_model_exists(
                    identifier
                )
            )

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=NoSuchSemanticModelException("semantic model not found"),
        ):
            self.assertFalse(
                self.catalog.as_semantic_model_catalog().semantic_model_exists(
                    identifier
                )
            )

    def test_create_semantic_model(self):
        identifier = NameIdentifier.of(self.schema_name, self.model_name)

        with patch(
            "gravitino.utils.http_client.HTTPClient.post",
            return_value=self._mock_semantic_model_response(),
        ) as mock_post:
            model = self.catalog.as_semantic_model_catalog().create_semantic_model(
                identifier, "comment", _definition(), {"owner": "finance"}
            )

        self.assertEqual(self.request_path, mock_post.call_args.args[0])
        request = mock_post.call_args.kwargs["json"]
        self.assertEqual(
            {
                "name": self.model_name,
                "comment": "comment",
                "definition": {
                    "datasets": [
                        {
                            "name": "orders",
                            "source": {
                                "name": "orders",
                                "namespace": ["sales", "mart"],
                            },
                        }
                    ]
                },
                "properties": {"owner": "finance"},
            },
            request.to_dict(),
        )
        self.assertEqual(self.model_name, model.name())

    def test_create_semantic_model_defaults_properties(self):
        identifier = NameIdentifier.of(self.schema_name, self.model_name)

        with patch(
            "gravitino.utils.http_client.HTTPClient.post",
            return_value=self._mock_semantic_model_response(),
        ) as mock_post:
            self.catalog.as_semantic_model_catalog().create_semantic_model(
                identifier, None, _definition()
            )

        request = mock_post.call_args.kwargs["json"]
        self.assertEqual({}, request.to_dict()["properties"])
        self.assertNotIn("comment", request.to_dict())

    def test_create_semantic_model_requires_a_definition(self):
        with self.assertRaisesRegex(
            IllegalArgumentException, "Semantic Model definition must not be null"
        ):
            self.catalog.as_semantic_model_catalog().create_semantic_model(
                NameIdentifier.of(self.schema_name, self.model_name), "comment", None
            )

    def test_create_semantic_model_maps_an_existing_model(self):
        with patch(
            "gravitino.utils.http_client.HTTPClient.post",
            side_effect=SemanticModelAlreadyExistsException("already exists"),
        ):
            with self.assertRaises(SemanticModelAlreadyExistsException):
                self.catalog.as_semantic_model_catalog().create_semantic_model(
                    NameIdentifier.of(self.schema_name, self.model_name),
                    "comment",
                    _definition(),
                    {},
                )

    def test_alter_semantic_model_sends_every_change(self):
        identifier = NameIdentifier.of(self.schema_name, self.model_name)

        with patch(
            "gravitino.utils.http_client.HTTPClient.put",
            return_value=self._mock_semantic_model_response("new comment"),
        ) as mock_put:
            model = self.catalog.as_semantic_model_catalog().alter_semantic_model(
                identifier,
                SemanticModelChange.rename("renamed_model"),
                SemanticModelChange.update_comment("new comment"),
                SemanticModelChange.set_property("owner", "finance"),
                SemanticModelChange.remove_property("legacy"),
                SemanticModelChange.replace_definition(_definition()),
            )

        self.assertEqual(
            f"{self.request_path}/{self.model_name}", mock_put.call_args.args[0]
        )
        updates = mock_put.call_args.kwargs["json"].to_dict()["updates"]
        self.assertEqual(
            [
                "rename",
                "updateComment",
                "setProperty",
                "removeProperty",
                "replaceDefinition",
            ],
            [update["@type"] for update in updates],
        )
        self.assertEqual("renamed_model", updates[0]["newName"])
        self.assertEqual("new comment", updates[1]["newComment"])
        self.assertEqual("owner", updates[2]["property"])
        self.assertEqual("finance", updates[2]["value"])
        self.assertEqual("legacy", updates[3]["property"])
        self.assertIn("datasets", updates[4]["definition"])
        self.assertEqual(self.model_name, model.name())

    def test_alter_semantic_model_can_clear_the_comment(self):
        identifier = NameIdentifier.of(self.schema_name, self.model_name)

        with patch(
            "gravitino.utils.http_client.HTTPClient.put",
            return_value=self._mock_semantic_model_response(),
        ) as mock_put:
            self.catalog.as_semantic_model_catalog().alter_semantic_model(
                identifier, SemanticModelChange.update_comment(None)
            )

        updates = mock_put.call_args.kwargs["json"].to_dict()["updates"]
        self.assertIsNone(updates[0]["newComment"])

    def test_alter_semantic_model_rejects_no_changes(self):
        with self.assertRaisesRegex(
            IllegalArgumentException, "updates cannot be empty"
        ):
            self.catalog.as_semantic_model_catalog().alter_semantic_model(
                NameIdentifier.of(self.schema_name, self.model_name)
            )

    def test_alter_semantic_model_rejects_an_unknown_change(self):
        with self.assertRaisesRegex(IllegalArgumentException, "Unknown change type"):
            self.catalog.as_semantic_model_catalog().alter_semantic_model(
                NameIdentifier.of(self.schema_name, self.model_name), object()
            )

    def test_drop_semantic_model(self):
        identifier = NameIdentifier.of(self.schema_name, self.model_name)
        mock_resp = self._get_mock_http_resp(
            DropResponse(_code=0, _dropped=True).to_json()
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.delete", return_value=mock_resp
        ) as mock_delete:
            self.assertTrue(
                self.catalog.as_semantic_model_catalog().drop_semantic_model(identifier)
            )

        self.assertEqual(
            f"{self.request_path}/{self.model_name}", mock_delete.call_args.args[0]
        )

    def test_drop_missing_semantic_model_returns_false(self):
        mock_resp = self._get_mock_http_resp(
            DropResponse(_code=0, _dropped=False).to_json()
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.delete", return_value=mock_resp
        ):
            self.assertFalse(
                self.catalog.as_semantic_model_catalog().drop_semantic_model(
                    NameIdentifier.of(self.schema_name, self.model_name)
                )
            )

    def test_rejects_an_invalid_namespace(self):
        catalog = self.catalog.as_semantic_model_catalog()

        with self.assertRaises(IllegalNamespaceException):
            catalog.list_semantic_models(Namespace.of(self.catalog_name, "extra"))

        with self.assertRaises(IllegalNamespaceException):
            catalog.load_semantic_model(
                NameIdentifier.of(self.catalog_name, self.schema_name, self.model_name)
            )

    def test_rejects_an_invalid_identifier(self):
        catalog = self.catalog.as_semantic_model_catalog()

        with self.assertRaises(IllegalNameIdentifierException):
            catalog.load_semantic_model(NameIdentifier.of(self.schema_name, ""))

    def test_encodes_names_in_the_request_path(self):
        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=self._mock_semantic_model_response(),
        ) as mock_get:
            self.catalog.as_semantic_model_catalog().load_semantic_model(
                NameIdentifier.of("schema with space", "model/with/slash")
            )

        self.assertEqual(
            f"api/metalakes/{self.metalake_name}/catalogs/{self.catalog_name}"
            "/schemas/schema%20with%20space/semantic-models/model%2Fwith%2Fslash",
            mock_get.call_args.args[0],
        )
