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


"""REST operations for ontologies and ontology mappings."""

from __future__ import annotations

from typing import Optional

import simplejson

from gravitino.api.metadata_object import MetadataObject
from gravitino.api.metadata_objects import MetadataObjects
from gravitino.api.ontology.ontology import Ontology, OntologyMapping
from gravitino.api.ontology.ontology_change import OntologyChange, OntologyMappingChange
from gravitino.client.metadata_object_policy_operations import (
    MetadataObjectPolicyOperations,
)
from gravitino.client.metadata_object_tag_operations import MetadataObjectTagOperations
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.entity_list_response import EntityListResponse
from gravitino.dto.responses.name_list_response import NameListResponse
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.exceptions.handlers.ontology_error_handler import ONTOLOGY_ERROR_HANDLER
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace
from gravitino.rest.rest_utils import encode_string
from gravitino.utils.http_client import HTTPClient


class _OntologyREST:
    _FORMATS = {"yaml": "application/yaml", "json": "application/json"}

    def __init__(self, client: HTTPClient, path: str):
        self.client = client
        self.path = path

    def get(self, suffix: str = ""):
        return self.client.get(self.path + suffix, error_handler=ONTOLOGY_ERROR_HANDLER)

    def send(self, request, suffix: str, body: dict) -> dict:
        result = request(
            self.path + suffix,
            data=simplejson.dumps(body, use_decimal=True),
            headers={"Content-Type": "application/json"},
            error_handler=ONTOLOGY_ERROR_HANDLER,
        )
        return simplejson.loads(result.body, use_decimal=True)

    def imported(self, document: str, document_format: str) -> dict:
        if not isinstance(document, str) or not document.strip():
            raise IllegalArgumentException("Ossie document must be non-blank text")
        result = self.client.post(
            f"{self.path}/ossie",
            data=document,
            headers={"Content-Type": self.media_type(document_format)},
            error_handler=ONTOLOGY_ERROR_HANDLER,
        )
        return simplejson.loads(result.body, use_decimal=True)

    def exported(self, name: str, document_format: str) -> str:
        response = self.client.get(
            f"{self.path}{self.item(name)}/ossie",
            params={"format": document_format},
            headers={"Accept": self.media_type(document_format)},
            error_handler=ONTOLOGY_ERROR_HANDLER,
        )
        return response.body.decode("utf-8")

    def dropped(self, name: str) -> bool:
        result = self.client.delete(
            self.path + self.item(name), error_handler=ONTOLOGY_ERROR_HANDLER
        )
        response = DropResponse.from_json(result.body, infer_missing=True)
        response.validate()
        return response.dropped()

    @staticmethod
    def item(name: str) -> str:
        if not isinstance(name, str) or not name.strip():
            raise IllegalArgumentException("Name must be non-blank")
        return f"/{encode_string(name)}"

    @classmethod
    def media_type(cls, document_format: str) -> str:
        if document_format not in cls._FORMATS:
            raise IllegalArgumentException("Ossie document format must be yaml or json")
        return cls._FORMATS[document_format]


class SupportsOntologies:
    """Operations for a metalake's ontologies, exposed by the metalake client."""

    def __init__(self, client: HTTPClient, metalake: str):
        self._metalake = metalake
        self._rest = _OntologyREST(
            client, f"api/metalakes/{encode_string(metalake)}/ontologies"
        )

    def list_ontologies(self) -> list[str]:
        """Lists the ontologies visible to the caller."""
        result = NameListResponse.from_json(self._rest.get().body, infer_missing=True)
        result.validate()
        return result.names

    def load_ontology(self, name: str) -> Ontology:
        """Loads the latest ontology snapshot."""
        body = self._rest.get(self._rest.item(name)).body
        return self._ontology(simplejson.loads(body, use_decimal=True))

    def create_ontology(
        self,
        name: str,
        comment: Optional[str],
        definition: dict,
        properties: Optional[dict[str, str]] = None,
    ) -> Ontology:
        """Creates an ontology from a native definition."""
        return self._ontology(
            self._rest.send(
                self._rest.client.post,
                "",
                {
                    "name": name,
                    "comment": comment,
                    "definition": definition,
                    "properties": properties,
                },
            )
        )

    def import_ontology(self, document: str, document_format: str = "yaml") -> Ontology:
        """Imports an Ossie ontology document."""
        return self._ontology(self._rest.imported(document, document_format))

    def export_ontology(self, name: str, document_format: str = "yaml") -> str:
        """Exports the ontology as an Ossie document."""
        return self._rest.exported(name, document_format)

    def alter_ontology(self, name: str, *changes: OntologyChange) -> Ontology:
        """Applies an ordered batch of changes."""
        if not changes:
            raise IllegalArgumentException("At least one change is required")
        return self._ontology(
            self._rest.send(
                self._rest.client.put,
                self._rest.item(name),
                {
                    "updates": [change.to_dict() for change in changes],
                },
            )
        )

    def drop_ontology(self, name: str) -> bool:
        """Drops an ontology without live mappings."""
        return self._rest.dropped(name)

    def list_ontology_mappings(self, name: str) -> list[NameIdentifier]:
        """Lists readable mappings of an ontology, relative to the metalake."""
        body = self._rest.get(f"{self._rest.item(name)}/mappings").body
        result = EntityListResponse.from_json(body, infer_missing=True)
        result.validate()
        return [
            NameIdentifier.of(*identifier.namespace().levels()[1:], identifier.name())
            if identifier.namespace().length() == 4
            else identifier
            for identifier in result.identifiers()
        ]

    def _ontology(self, response: dict) -> Ontology:
        payload = response["ontology"]
        metadata = MetadataObjects.of([payload["name"]], MetadataObject.Type.ONTOLOGY)
        return Ontology(
            payload,
            MetadataObjectTagOperations(self._metalake, metadata, self._rest.client),
            MetadataObjectPolicyOperations(self._metalake, metadata, self._rest.client),
        )


class SupportsOntologyMappings:
    """Operations for mappings under a loaded semantic model."""

    def __init__(self, client: HTTPClient, namespace: Namespace, model: str):
        metalake, catalog, schema, name = map(
            encode_string, (*namespace.levels(), model)
        )
        self._rest = _OntologyREST(
            client,
            f"api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}"
            f"/semantic-models/{name}/ontology-mappings",
        )

    def list_ontology_mappings(self) -> list[str]:
        """Lists mapping names within the current semantic model."""
        result = EntityListResponse.from_json(self._rest.get().body, infer_missing=True)
        result.validate()
        return [identifier.name() for identifier in result.identifiers()]

    def load_ontology_mapping(self, name: str) -> OntologyMapping:
        """Loads the latest mapping snapshot."""
        body = self._rest.get(self._rest.item(name)).body
        return OntologyMapping(
            simplejson.loads(body, use_decimal=True)["ontologyMapping"]
        )

    # pylint: disable=too-many-positional-arguments
    def create_ontology_mapping(
        self,
        name: str,
        comment: Optional[str],
        ontology: str,
        definition: dict,
        properties: Optional[dict[str, str]] = None,
    ) -> OntologyMapping:
        """Creates a mapping with a fixed ontology reference."""
        result = self._rest.send(
            self._rest.client.post,
            "",
            {
                "name": name,
                "comment": comment,
                "ontology": ontology,
                "definition": definition,
                "properties": properties,
            },
        )
        return OntologyMapping(result["ontologyMapping"])

    def import_ontology_mapping(
        self, document: str, document_format: str = "yaml"
    ) -> OntologyMapping:
        """Imports an Ossie mapping document under this semantic model."""
        return OntologyMapping(
            self._rest.imported(document, document_format)["ontologyMapping"]
        )

    def export_ontology_mapping(self, name: str, document_format: str = "yaml") -> str:
        """Exports the mapping with the current model and ontology names."""
        return self._rest.exported(name, document_format)

    def alter_ontology_mapping(
        self, name: str, *changes: OntologyMappingChange
    ) -> OntologyMapping:
        """Applies an ordered batch of mapping changes."""
        if not changes:
            raise IllegalArgumentException("At least one change is required")
        result = self._rest.send(
            self._rest.client.put,
            self._rest.item(name),
            {
                "updates": [change.to_dict() for change in changes],
            },
        )
        return OntologyMapping(result["ontologyMapping"])

    def drop_ontology_mapping(self, name: str) -> bool:
        """Drops a mapping under this semantic model."""
        return self._rest.dropped(name)
