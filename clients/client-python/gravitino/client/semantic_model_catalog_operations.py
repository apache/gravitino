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

from typing import Optional

from gravitino.api.semantic.semantic_model import SemanticModel
from gravitino.api.semantic.semantic_model_catalog import SemanticModelCatalog
from gravitino.api.semantic.semantic_model_change import (
    RemoveProperty,
    RenameSemanticModel,
    ReplaceDefinition,
    SemanticModelChange,
    SetProperty,
    UpdateComment,
)
from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition
from gravitino.client.generic_semantic_model import GenericSemanticModel
from gravitino.dto.requests.semantic_model_create_request import (
    SemanticModelCreateRequest,
)
from gravitino.dto.requests.semantic_model_update_request import (
    SemanticModelUpdateRequest,
    SemanticModelUpdateRequestBase,
)
from gravitino.dto.requests.semantic_model_updates_request import (
    SemanticModelUpdatesRequest,
)
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.entity_list_response import EntityListResponse
from gravitino.dto.responses.semantic_model_response import SemanticModelResponse
from gravitino.dto.semantic.semantic_model_definition_dto import (
    SemanticModelDefinitionDTO,
)
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.exceptions.handlers.semantic_model_error_handler import (
    SEMANTIC_MODEL_ERROR_HANDLER,
)
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace
from gravitino.rest.rest_utils import encode_string


class SemanticModelCatalogOperations(SemanticModelCatalog):
    """Implements schema-scoped Semantic Model operations through the Gravitino REST API.

    This class is used by catalogs that support Semantic Model operations.
    """

    def __init__(self, rest_client, catalog_namespace: Namespace, catalog_name: str):
        """Create a SemanticModelCatalogOperations instance.

        Args:
            rest_client: The REST client for making API calls.
            catalog_namespace (Namespace): The namespace of the catalog.
            catalog_name (str): The name of the catalog.
        """
        self._rest_client = rest_client
        self._catalog_namespace = catalog_namespace
        self._catalog_name = catalog_name

    def list_semantic_models(self, namespace: Namespace) -> list[NameIdentifier]:
        self._check_semantic_model_namespace(namespace)
        full_namespace = self._get_semantic_model_full_namespace(namespace)
        resp = self._rest_client.get(
            self._format_semantic_model_request_path(full_namespace),
            error_handler=SEMANTIC_MODEL_ERROR_HANDLER,
        )
        entity_list_resp = EntityListResponse.from_json(resp.body, infer_missing=True)
        entity_list_resp.validate()
        return [
            NameIdentifier.of(ident.namespace().level(2), ident.name())
            for ident in entity_list_resp.identifiers()
        ]

    def load_semantic_model(self, identifier: NameIdentifier) -> SemanticModel:
        self._check_semantic_model_name_identifier(identifier)
        full_namespace = self._get_semantic_model_full_namespace(identifier.namespace())
        resp = self._rest_client.get(
            f"{self._format_semantic_model_request_path(full_namespace)}"
            f"/{encode_string(identifier.name())}",
            error_handler=SEMANTIC_MODEL_ERROR_HANDLER,
        )
        return self._to_semantic_model(resp)

    def create_semantic_model(
        self,
        identifier: NameIdentifier,
        comment: Optional[str],
        definition: SemanticModelDefinition,
        properties: Optional[dict[str, str]] = None,
    ) -> SemanticModel:
        self._check_semantic_model_name_identifier(identifier)
        if definition is None:
            raise IllegalArgumentException("Semantic Model definition must not be null")

        req = SemanticModelCreateRequest(
            _name=identifier.name(),
            _comment=comment,
            _definition=SemanticModelDefinitionDTO.from_definition(definition),
            _properties=properties or {},
        )
        req.validate()

        full_namespace = self._get_semantic_model_full_namespace(identifier.namespace())
        resp = self._rest_client.post(
            self._format_semantic_model_request_path(full_namespace),
            json=req,
            error_handler=SEMANTIC_MODEL_ERROR_HANDLER,
        )
        return self._to_semantic_model(resp)

    def alter_semantic_model(
        self, identifier: NameIdentifier, *changes: SemanticModelChange
    ) -> SemanticModel:
        self._check_semantic_model_name_identifier(identifier)
        updates_request = SemanticModelUpdatesRequest(
            _updates=[
                self._to_semantic_model_update_request(change) for change in changes
            ]
        )
        updates_request.validate()

        full_namespace = self._get_semantic_model_full_namespace(identifier.namespace())
        resp = self._rest_client.put(
            f"{self._format_semantic_model_request_path(full_namespace)}"
            f"/{encode_string(identifier.name())}",
            json=updates_request,
            error_handler=SEMANTIC_MODEL_ERROR_HANDLER,
        )
        return self._to_semantic_model(resp)

    def drop_semantic_model(self, identifier: NameIdentifier) -> bool:
        self._check_semantic_model_name_identifier(identifier)
        full_namespace = self._get_semantic_model_full_namespace(identifier.namespace())
        resp = self._rest_client.delete(
            f"{self._format_semantic_model_request_path(full_namespace)}"
            f"/{encode_string(identifier.name())}",
            error_handler=SEMANTIC_MODEL_ERROR_HANDLER,
        )
        drop_resp = DropResponse.from_json(resp.body, infer_missing=True)
        drop_resp.validate()
        return drop_resp.dropped()

    @staticmethod
    def _to_semantic_model(resp) -> SemanticModel:
        semantic_model_resp = SemanticModelResponse.from_json(
            resp.body, infer_missing=True
        )
        semantic_model_resp.validate()
        return GenericSemanticModel(semantic_model_resp.semantic_model())

    @staticmethod
    def _to_semantic_model_update_request(
        change: SemanticModelChange,
    ) -> SemanticModelUpdateRequestBase:
        if isinstance(change, RenameSemanticModel):
            return SemanticModelUpdateRequest.RenameSemanticModelRequest(
                _new_name=change.new_name()
            )

        if isinstance(change, UpdateComment):
            return SemanticModelUpdateRequest.UpdateSemanticModelCommentRequest(
                _new_comment=change.new_comment()
            )

        if isinstance(change, SetProperty):
            return SemanticModelUpdateRequest.SetSemanticModelPropertyRequest(
                _property=change.property(), _value=change.value()
            )

        if isinstance(change, RemoveProperty):
            return SemanticModelUpdateRequest.RemoveSemanticModelPropertyRequest(
                _property=change.property()
            )

        if isinstance(change, ReplaceDefinition):
            return SemanticModelUpdateRequest.ReplaceSemanticModelDefinitionRequest(
                _definition=SemanticModelDefinitionDTO.from_definition(
                    change.definition()
                )
            )

        raise IllegalArgumentException(
            f"Unknown change type: {change.__class__.__name__}"
        )

    @staticmethod
    def _check_semantic_model_namespace(namespace: Namespace) -> None:
        """Check whether the namespace of a Semantic Model is valid, which should be "schema".

        Args:
            namespace (Namespace): The namespace to check.

        Raises:
            IllegalNamespaceException: If the Namespace is not valid.
        """
        Namespace.check(
            namespace is not None and namespace.length() == 1,
            "Semantic Model namespace must be non-null and have 1 level, "
            f"the input namespace is {namespace}",
        )

    @staticmethod
    def _check_semantic_model_name_identifier(identifier: NameIdentifier) -> None:
        """Check whether the `NameIdentifier` of a Semantic Model is valid.

        Args:
            identifier (NameIdentifier):
                The NameIdentifier to check, which should be "schema.semanticModel" format.

        Raises:
            IllegalNameIdentifierException: If the NameIdentifier is not valid.
        """
        NameIdentifier.check(identifier is not None, "NameIdentifier must not be null")
        NameIdentifier.check(
            identifier.name() is not None and identifier.name() != "",
            "NameIdentifier name must not be empty",
        )
        SemanticModelCatalogOperations._check_semantic_model_namespace(
            identifier.namespace()
        )

    @staticmethod
    def _format_semantic_model_request_path(namespace: Namespace) -> str:
        """Format the request path for Semantic Model operations.

        Args:
            namespace (Namespace): The full namespace (metalake.catalog.schema).

        Returns:
            str: The request path.
        """
        return (
            f"api/metalakes/{encode_string(namespace.level(0))}"
            f"/catalogs/{encode_string(namespace.level(1))}"
            f"/schemas/{encode_string(namespace.level(2))}"
            "/semantic-models"
        )

    def _get_semantic_model_full_namespace(
        self, semantic_model_namespace: Namespace
    ) -> Namespace:
        """Get the full namespace of a Semantic Model with the given short namespace.

        Args:
            semantic_model_namespace (Namespace):
                The Semantic Model's short namespace, which is the schema name.

        Returns:
            Namespace: The full namespace in "metalake.catalog.schema" format.
        """
        return Namespace.of(
            self._catalog_namespace.level(0),
            self._catalog_name,
            semantic_model_namespace.level(0),
        )
