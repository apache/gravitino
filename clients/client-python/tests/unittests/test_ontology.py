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

from __future__ import annotations

import copy
import unittest
from unittest.mock import patch

from gravitino import GravitinoClient
from gravitino.api.ontology.ontology import Ontology
from gravitino.api.ontology.ontology_change import OntologyChange
from gravitino.client.dto_converters import DTOConverters
from gravitino.constants.error import ErrorConstants
from gravitino.dto.ontology.ontology_dto import OntologyDTO
from gravitino.dto.requests.ontology_updates_request import OntologyUpdatesRequest
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.error_response import ErrorResponse
from gravitino.dto.responses.ontology_response import (
    OntologyListResponse,
    OntologyNamesListResponse,
    OntologyResponse,
)
from gravitino.exceptions.base import (
    IllegalArgumentException,
    IllegalOntologyException,
    NoSuchMetalakeException,
    NoSuchOntologyException,
    NonEmptyOntologyException,
    OntologyAlreadyExistsException,
)
from gravitino.exceptions.handlers.ontology_error_handler import ONTOLOGY_ERROR_HANDLER
from tests.unittests import mock_base

_ONTOLOGY_NAMES_PATH = "api/metalakes/metalake_demo/ontologies"
_ONTOLOGY_PATH = "api/metalakes/metalake_demo/ontologies/Finance"

_ONTOLOGY_PAYLOAD = {
    "name": "Finance",
    "comment": "Finance vocabulary",
    "properties": {"ossie-version": "0.2.0.dev0"},
    "status": "DRAFT",
    "definition": {
        "aiContext": "Certified",
        "requires": ["Account IS NOT NULL"],
        "concepts": [
            {
                "concept": "Account",
                "type": "ENTITY_TYPE",
                "extends": ["Party"],
                "relationships": [
                    {
                        "name": "holds",
                        "verbalizes": ["an account holds a balance"],
                        "roles": [{"concept": "Balance"}],
                        "multiplicity": "MANY_TO_ONE",
                        "iri": "ex:holds",
                    }
                ],
                "iri": "ex:Account",
            }
        ],
        "prefixes": {"ex": "http://example.com/finance/"},
    },
    "audit": {
        "creator": "u",
        "createTime": "2026-01-01T00:00:00Z",
        "lastModifier": "u",
        "lastModifiedTime": "2026-01-01T00:00:00Z",
    },
}


def _ontology_payload() -> dict:
    """Builds a fresh ontology JSON payload, so tests cannot mutate each other."""
    return copy.deepcopy(_ONTOLOGY_PAYLOAD)


def _ontology_dto() -> OntologyDTO:
    """Builds the ontology DTO the mocked server returns."""
    return OntologyDTO.from_dict(_ontology_payload())


@mock_base.mock_data
class TestOntology(unittest.TestCase):
    _metalake_name: str = "metalake_demo"

    def _client(self) -> GravitinoClient:
        return GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
            check_version=False,
        )

    def _mock_error(self, code: int, error_type: str, message: str):
        """Patches the transport so the real REST error handler runs."""
        error_response = ErrorResponse(code, error_type, message, None)
        return patch(
            "gravitino.utils.http_client.HTTPClient._make_request",
            return_value=(False, error_response),
        )

    def test_dto_json_round_trip(self, *mock_method) -> None:
        payload = _ontology_payload()

        dto = OntologyDTO.from_dict(payload)

        self.assertIsInstance(dto, Ontology)
        self.assertEqual(payload, dto.to_dict())
        self.assertEqual(payload, OntologyDTO.from_json(dto.to_json()).to_dict())

        self.assertEqual("Finance", dto.name())
        self.assertEqual("Finance vocabulary", dto.comment())
        self.assertEqual({"ossie-version": "0.2.0.dev0"}, dto.properties())
        self.assertEqual("DRAFT", dto.status())
        self.assertEqual("u", dto.audit_info().creator())

        definition = dto.definition()
        self.assertEqual(["Account IS NOT NULL"], definition.requires())
        self.assertEqual(["Certified"], [definition.ai_context().text()])
        self.assertEqual({"ex": "http://example.com/finance/"}, definition.prefixes())

        concept = definition.concepts()[0]
        self.assertEqual("Account", concept.concept())
        self.assertEqual("ENTITY_TYPE", concept.type())
        self.assertEqual(["Party"], concept.extends_concepts())
        self.assertEqual("ex:Account", concept.iri())
        self.assertIsNone(concept.derived_by())

        relationship = concept.relationships()[0]
        self.assertEqual("holds", relationship.name())
        self.assertEqual(["an account holds a balance"], relationship.verbalizes())
        self.assertEqual("MANY_TO_ONE", relationship.multiplicity())
        self.assertEqual("ex:holds", relationship.iri())
        self.assertEqual("Balance", relationship.roles()[0].concept())
        self.assertIsNone(relationship.roles()[0].name())

    def test_lower_snake_case_values_are_normalized(self, *mock_method) -> None:
        payload = _ontology_payload()
        payload["status"] = "published"
        payload["definition"]["concepts"][0]["type"] = "entity_type"
        payload["definition"]["concepts"][0]["relationships"][0][
            "multiplicity"
        ] = "many_to_one"

        dto = OntologyDTO.from_dict(payload)

        self.assertEqual("PUBLISHED", dto.status())
        self.assertEqual("ENTITY_TYPE", dto.definition().concepts()[0].type())
        self.assertEqual(
            "MANY_TO_ONE",
            dto.definition().concepts()[0].relationships()[0].multiplicity(),
        )
        self.assertEqual("PUBLISHED", dto.to_dict().get("status"))
        self.assertEqual(
            "ENTITY_TYPE",
            dto.to_dict()["definition"]["concepts"][0]["type"],
        )

    def test_list_ontologies(self, *mock_method) -> None:
        mock_resp = mock_base.mock_http_response(
            OntologyNamesListResponse(0, ["Finance"]).to_json()
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ) as mock_get:
            names = self._client().list_ontologies()

        self.assertEqual(["Finance"], names)
        args, kwargs = mock_get.call_args
        self.assertEqual(_ONTOLOGY_NAMES_PATH, args[0])
        self.assertEqual(ONTOLOGY_ERROR_HANDLER, kwargs.get("error_handler"))
        self.assertIsNone(kwargs.get("params"))

    def test_list_ontologies_info(self, *mock_method) -> None:
        mock_resp = mock_base.mock_http_response(
            OntologyListResponse(0, [_ontology_dto()]).to_json()
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ) as mock_get:
            ontologies = self._client().list_ontologies_info()

        self.assertEqual(1, len(ontologies))
        self.assertEqual("Finance", ontologies[0].name())
        self.assertEqual("DRAFT", ontologies[0].status())
        self.assertEqual("Account", ontologies[0].definition().concepts()[0].concept())
        args, kwargs = mock_get.call_args
        self.assertEqual(_ONTOLOGY_NAMES_PATH, args[0])
        self.assertEqual({"details": "true"}, kwargs.get("params"))

    def test_get_ontology(self, *mock_method) -> None:
        mock_resp = mock_base.mock_http_response(
            OntologyResponse(0, _ontology_dto()).to_json()
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ) as mock_get:
            ontology = self._client().get_ontology("Finance")

        self.assertEqual("Finance", ontology.name())
        self.assertEqual({"ossie-version": "0.2.0.dev0"}, ontology.properties())
        args, _ = mock_get.call_args
        self.assertEqual(_ONTOLOGY_PATH, args[0])

    def test_get_ontology_encodes_name(self, *mock_method) -> None:
        mock_resp = mock_base.mock_http_response(
            OntologyResponse(0, _ontology_dto()).to_json()
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ) as mock_get:
            self._client().get_ontology("Finance/2026")

        args, _ = mock_get.call_args
        self.assertEqual(
            "api/metalakes/metalake_demo/ontologies/Finance%2F2026", args[0]
        )

    def test_get_ontology_rejects_blank_name(self, *mock_method) -> None:
        with self.assertRaises(IllegalArgumentException):
            self._client().get_ontology("  ")

    def test_create_ontology(self, *mock_method) -> None:
        payload = _ontology_payload()
        mock_resp = mock_base.mock_http_response(
            OntologyResponse(0, _ontology_dto()).to_json()
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.post", return_value=mock_resp
        ) as mock_post:
            ontology = self._client().create_ontology(
                "Finance",
                "Finance vocabulary",
                {"ossie-version": "0.2.0.dev0"},
                payload["definition"],
            )

        self.assertEqual("Finance", ontology.name())
        self.assertEqual("DRAFT", ontology.status())

        args, kwargs = mock_post.call_args
        self.assertEqual(_ONTOLOGY_NAMES_PATH, args[0])
        self.assertEqual(ONTOLOGY_ERROR_HANDLER, kwargs.get("error_handler"))
        self.assertEqual(
            {
                "name": "Finance",
                "comment": "Finance vocabulary",
                "properties": {"ossie-version": "0.2.0.dev0"},
                "definition": payload["definition"],
            },
            kwargs.get("json").to_dict(),
        )

    def test_create_ontology_with_definition_dto(self, *mock_method) -> None:
        mock_resp = mock_base.mock_http_response(
            OntologyResponse(0, _ontology_dto()).to_json()
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.post", return_value=mock_resp
        ) as mock_post:
            self._client().create_ontology(
                "Finance", None, None, _ontology_dto().definition()
            )

        body = mock_post.call_args.kwargs.get("json").to_dict()
        self.assertEqual(_ontology_payload()["definition"], body.get("definition"))
        self.assertIsNone(body.get("comment"))

    def test_create_ontology_requires_definition(self, *mock_method) -> None:
        with self.assertRaises(IllegalArgumentException):
            self._client().create_ontology("Finance", None, None, None)

    def test_create_ontology_requires_concepts(self, *mock_method) -> None:
        with self.assertRaises(IllegalArgumentException):
            self._client().create_ontology(
                "Finance",
                None,
                None,
                {"concepts": [], "prefixes": {"ex": "http://example.com/"}},
            )

    def test_alter_change_requests(self, *mock_method) -> None:
        definition = _ontology_payload()["definition"]
        cases = [
            (
                OntologyChange.rename("Finance2"),
                {"@type": "rename", "newName": "Finance2"},
            ),
            (
                OntologyChange.update_comment("new comment"),
                {"@type": "updateComment", "newComment": "new comment"},
            ),
            (
                OntologyChange.set_property("k", "v"),
                {"@type": "setProperty", "property": "k", "value": "v"},
            ),
            (
                OntologyChange.remove_property("k"),
                {"@type": "removeProperty", "property": "k"},
            ),
            (
                OntologyChange.replace_definition(definition),
                {"@type": "replaceDefinition", "definition": definition},
            ),
            (
                OntologyChange.update_status(Ontology.Status.PUBLISHED),
                {"@type": "updateStatus", "status": "PUBLISHED"},
            ),
        ]

        for change, expected in cases:
            with self.subTest(change=str(change)):
                update_request = DTOConverters.to_ontology_update_request(change)
                updates_request = OntologyUpdatesRequest([update_request])
                updates_request.validate()
                self.assertEqual(expected, updates_request.to_dict().get("updates")[0])

    def test_alter_ontology(self, *mock_method) -> None:
        mock_resp = mock_base.mock_http_response(
            OntologyResponse(0, _ontology_dto()).to_json()
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.put", return_value=mock_resp
        ) as mock_put:
            ontology = self._client().alter_ontology(
                "Finance",
                OntologyChange.rename("Finance2"),
                OntologyChange.set_property("k", "v"),
            )

        self.assertEqual("Finance", ontology.name())
        args, kwargs = mock_put.call_args
        self.assertEqual(_ONTOLOGY_PATH, args[0])
        self.assertEqual(ONTOLOGY_ERROR_HANDLER, kwargs.get("error_handler"))
        self.assertEqual(
            {
                "updates": [
                    {"@type": "rename", "newName": "Finance2"},
                    {"@type": "setProperty", "property": "k", "value": "v"},
                ]
            },
            kwargs.get("json").to_dict(),
        )

    def test_alter_ontology_requires_change(self, *mock_method) -> None:
        with self.assertRaises(IllegalArgumentException):
            self._client().alter_ontology("Finance")

    def test_alter_ontology_rejects_unknown_status(self, *mock_method) -> None:
        with self.assertRaises(IllegalArgumentException):
            OntologyChange.update_status("ARCHIVED")

    def test_alter_ontology_rejects_unknown_change(self, *mock_method) -> None:
        with self.assertRaises(IllegalArgumentException):
            DTOConverters.to_ontology_update_request("rename")

    def test_drop_ontology(self, *mock_method) -> None:
        mock_resp = mock_base.mock_http_response(DropResponse(0, True).to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.delete", return_value=mock_resp
        ) as mock_delete:
            dropped = self._client().drop_ontology("Finance")

        self.assertTrue(dropped)
        args, kwargs = mock_delete.call_args
        self.assertEqual(_ONTOLOGY_PATH, args[0])
        self.assertEqual(ONTOLOGY_ERROR_HANDLER, kwargs.get("error_handler"))

    def test_drop_ontology_returns_false(self, *mock_method) -> None:
        mock_resp = mock_base.mock_http_response(DropResponse(0, False).to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.delete", return_value=mock_resp
        ):
            self.assertFalse(self._client().drop_ontology("Finance"))

    def test_error_handler_mappings(self, *mock_method) -> None:
        cases = [
            (
                ErrorConstants.NOT_FOUND_CODE,
                "NoSuchOntologyException",
                NoSuchOntologyException,
            ),
            (
                ErrorConstants.NOT_FOUND_CODE,
                "NoSuchMetalakeException",
                NoSuchMetalakeException,
            ),
            (
                ErrorConstants.ALREADY_EXISTS_CODE,
                "OntologyAlreadyExistsException",
                OntologyAlreadyExistsException,
            ),
            (
                ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
                "IllegalOntologyException",
                IllegalOntologyException,
            ),
            (
                ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
                "IllegalArgumentException",
                IllegalArgumentException,
            ),
            (
                ErrorConstants.NON_EMPTY_CODE,
                "NonEmptyOntologyException",
                NonEmptyOntologyException,
            ),
        ]

        for code, error_type, expected in cases:
            with self.subTest(code=code, error_type=error_type):
                error_response = ErrorResponse(code, error_type, "mock error", None)
                with self.assertRaises(expected):
                    ONTOLOGY_ERROR_HANDLER.handle(error_response)

    def test_get_ontology_not_found(self, *mock_method) -> None:
        with self._mock_error(
            ErrorConstants.NOT_FOUND_CODE,
            "NoSuchOntologyException",
            "no such ontology",
        ):
            with self.assertRaisesRegex(NoSuchOntologyException, "no such ontology"):
                self._client().get_ontology("Finance")

    def test_create_ontology_already_exists(self, *mock_method) -> None:
        with self._mock_error(
            ErrorConstants.ALREADY_EXISTS_CODE,
            "OntologyAlreadyExistsException",
            "ontology already exists",
        ):
            with self.assertRaisesRegex(
                OntologyAlreadyExistsException, "ontology already exists"
            ):
                self._client().create_ontology(
                    "Finance", None, None, _ontology_payload()["definition"]
                )

    def test_create_ontology_illegal_ontology(self, *mock_method) -> None:
        with self._mock_error(
            ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
            "IllegalOntologyException",
            "invalid ontology",
        ):
            with self.assertRaisesRegex(IllegalOntologyException, "invalid ontology"):
                self._client().create_ontology(
                    "Finance", None, None, _ontology_payload()["definition"]
                )

    def test_drop_ontology_not_empty(self, *mock_method) -> None:
        with self._mock_error(
            ErrorConstants.NON_EMPTY_CODE,
            "NonEmptyOntologyException",
            "ontology is not empty",
        ):
            with self.assertRaisesRegex(
                NonEmptyOntologyException, "ontology is not empty"
            ):
                self._client().drop_ontology("Finance")
