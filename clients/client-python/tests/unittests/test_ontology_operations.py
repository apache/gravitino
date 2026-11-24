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


"""Checks the Python ontology and mapping REST contracts."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
import simplejson

from gravitino.api.authorization.privileges import Privilege, Privileges
from gravitino.api.metadata_object import MetadataObject
from gravitino.api.ontology.ontology_change import OntologyChange, OntologyMappingChange
from gravitino.client.gravitino_client import GravitinoClient
from gravitino.client.gravitino_client_base import GravitinoClientBase
from gravitino.client.gravitino_metalake import GravitinoMetalake
from gravitino.client.ontology_operations import SupportsOntologyMappings
from gravitino.dto.metalake_dto import MetalakeDTO
from gravitino.dto.responses.error_response import ErrorResponse
from gravitino.exceptions.base import OntologyInUseException
from gravitino.exceptions.handlers.ontology_error_handler import ONTOLOGY_ERROR_HANDLER
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace


def response(**fields):
    return SimpleNamespace(body=simplejson.dumps({"code": 0, **fields}).encode())


def test_ontology_client_routes_and_governance(request):
    rest = Mock()
    metalake = GravitinoMetalake(MetalakeDTO("sample", None, {}, None), rest)
    with patch.object(GravitinoClientBase, "load_metalake", return_value=metalake):
        ops = GravitinoClient("http://localhost:8090", "sample", check_version=False)
    request.addfinalizer(ops.close)
    definition = {
        "ontology": [{"concept": "Airport", "type": "EntityType"}],
        "annotations": {"score": Decimal("1.234567890123456789")},
    }
    payload = {
        "name": "Flight Terms",
        "definition": definition,
        "properties": {},
        "audit": {"creator": "steward", "createTime": "2026-11-24T00:00:00Z"},
    }
    rest.post.return_value = response(ontology=payload)
    created = ops.create_ontology("Flight Terms", None, definition)
    assert rest.post.call_args.args == ("api/metalakes/sample/ontologies",)
    assert (
        simplejson.loads(rest.post.call_args.kwargs["data"], use_decimal=True)[
            "definition"
        ]
        == definition
    )
    assert created.definition() == definition
    assert created.audit_info().creator() == "steward"
    assert created.supports_tags() is not None
    assert created.supports_policies() is not None
    created.definition()["ontology"].clear()
    assert created.definition() == definition

    rest.put.return_value = response(ontology=payload)
    ops.alter_ontology(
        "Flight Terms",
        OntologyChange.update_comment(None),
        OntologyChange.remove_property("team"),
    )
    assert rest.put.call_args.args == (
        "api/metalakes/sample/ontologies/Flight%20Terms",
    )
    assert simplejson.loads(rest.put.call_args.kwargs["data"]) == {
        "updates": [
            {"@type": "updateComment", "newComment": None},
            {"@type": "removeProperty", "property": "team"},
        ]
    }

    rest.get.return_value = response(names=["Flight Terms"])
    assert ops.list_ontologies() == ["Flight Terms"]
    rest.get.return_value = response(ontology=payload)
    assert ops.load_ontology("Flight Terms").definition() == definition
    for namespace in (["sample", "c", "s", "flights"], ["c", "s", "flights"]):
        rest.get.return_value = response(
            identifiers=[{"namespace": namespace, "name": "flight_map"}]
        )
        assert ops.list_ontology_mappings("Flight Terms") == [
            NameIdentifier.of("c", "s", "flights", "flight_map")
        ]

    document = "name: Flight Terms\n"
    assert ops.import_ontology(document).definition() == definition
    assert rest.post.call_args.args == ("api/metalakes/sample/ontologies/ossie",)
    assert rest.post.call_args.kwargs["data"] == document
    rest.get.return_value = SimpleNamespace(body=b'{"name":"Flight Terms"}')
    assert ops.export_ontology("Flight Terms", "json") == '{"name":"Flight Terms"}'
    assert rest.get.call_args.kwargs["params"] == {"format": "json"}
    rest.delete.return_value = response(dropped=True)
    assert ops.drop_ontology("Flight Terms")


def test_mapping_raw_import_export_and_update():
    rest = Mock()
    ops = SupportsOntologyMappings(rest, Namespace.of("m", "c", "s"), "Flight Model")
    path = "api/metalakes/m/catalogs/c/schemas/s/semantic-models/Flight%20Model/ontology-mappings"
    document = "name: flight_map\n"
    definition = {"conceptMappings": []}
    payload = {
        "name": "flight_map",
        "ontology": "Flights",
        "definition": definition,
        "properties": {},
        "audit": {"creator": "author"},
    }
    rest.post.return_value = response(ontologyMapping=payload)
    assert ops.import_ontology_mapping(document).ontology() == "Flights"
    assert rest.post.call_args.args == (path + "/ossie",)
    assert rest.post.call_args.kwargs["data"] == document
    assert rest.post.call_args.kwargs["headers"]["Content-Type"] == "application/yaml"

    rest.get.return_value = SimpleNamespace(body=b'{"name":"flight_map"}')
    assert ops.export_ontology_mapping("flight_map", "json") == '{"name":"flight_map"}'
    assert rest.get.call_args.args == (path + "/flight_map/ossie",)
    assert rest.get.call_args.kwargs["params"] == {"format": "json"}

    rest.put.return_value = response(ontologyMapping=payload)
    ops.alter_ontology_mapping(
        "flight_map", OntologyMappingChange.replace_definition(definition)
    )
    assert (
        simplejson.loads(rest.put.call_args.kwargs["data"])["updates"][0]["definition"]
        == definition
    )

    rest.delete.return_value = response(dropped=True)
    assert ops.drop_ontology_mapping("flight_map")


def test_typed_conflict_and_ontology_privileges():
    error = ErrorResponse.from_json(
        '{"code":1010,"type":"OntologyInUseException","message":"used by a mapping"}',
        infer_missing=True,
    )
    with pytest.raises(OntologyInUseException, match="used by a mapping"):
        ONTOLOGY_ERROR_HANDLER.handle(error)
    assert not Privileges.allow(Privilege.Name.CREATE_ONTOLOGY.name).can_bind_to(
        MetadataObject.Type.ONTOLOGY
    )
    assert Privileges.allow(Privilege.Name.MODIFY_ONTOLOGY.name).can_bind_to(
        MetadataObject.Type.ONTOLOGY
    )
