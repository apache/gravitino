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

from typing import List, Optional

from gravitino.api.function.function import Function
from gravitino.api.function.function_catalog import FunctionCatalog
from gravitino.api.function.function_change import (
    AddDefinition,
    AddImpl,
    FunctionChange,
    RemoveDefinition,
    RemoveImpl,
    UpdateComment,
    UpdateImpl,
)
from gravitino.api.function.function_definition import FunctionDefinition
from gravitino.api.function.function_type import FunctionType
from gravitino.client.generic_function import GenericFunction
from gravitino.dto.function.function_definition_dto import FunctionDefinitionDTO
from gravitino.dto.function.function_impl_dto import (
    function_impl_dto_from_function_impl,
)
from gravitino.dto.function.function_param_dto import FunctionParamDTO
from gravitino.dto.requests.function_register_request import FunctionRegisterRequest
from gravitino.dto.requests.function_update_request import (
    AddDefinitionRequest,
    AddImplRequest,
    FunctionUpdateRequest,
    RemoveDefinitionRequest,
    RemoveImplRequest,
    UpdateCommentRequest,
    UpdateImplRequest,
)
from gravitino.dto.requests.function_updates_request import FunctionUpdatesRequest
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.entity_list_response import EntityListResponse
from gravitino.dto.responses.function_list_response import FunctionListResponse
from gravitino.dto.responses.function_response import FunctionResponse
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.exceptions.handlers.function_error_handler import (
    FUNCTION_ERROR_HANDLER,
)
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace
from gravitino.rest import rest_utils


class FunctionCatalogOperations(FunctionCatalog):
    """Function catalog operations helper class that provides implementations for function management.

    This class is used by catalogs that support function operations.
    """

    def __init__(self, rest_client, catalog_namespace: Namespace, catalog_name: str):
        """Create a FunctionCatalogOperations instance.

        Args:
            rest_client: The REST client for making API calls.
            catalog_namespace: The namespace of the catalog.
            catalog_name: The name of the catalog.
        """
        self._rest_client = rest_client
        self._catalog_namespace = catalog_namespace
        self._catalog_name = catalog_name

    def list_functions(self, namespace: Namespace) -> List[NameIdentifier]:
        """List the functions in a schema namespace from the catalog.

        Args:
            namespace: A schema namespace. This namespace should have 1 level,
                       which is the schema name.

        Returns:
            A list of function identifiers in the namespace.

        Raises:
            NoSuchSchemaException: If the schema does not exist.
        """
        self._check_function_namespace(namespace)

        full_namespace = self._get_function_full_namespace(namespace)
        resp = self._rest_client.get(
            self._format_function_request_path(full_namespace),
            error_handler=FUNCTION_ERROR_HANDLER,
        )

        entity_list_response = EntityListResponse.from_json(resp.body)
        entity_list_response.validate()

        return [
            NameIdentifier.of(ident.namespace().level(2), ident.name())
            for ident in entity_list_response.identifiers()
        ]

    def list_function_infos(self, namespace: Namespace) -> List[Function]:
        """List the functions with details in a schema namespace from the catalog.

        Args:
            namespace: A namespace.

        Returns:
            A list of functions in the namespace.

        Raises:
            NoSuchSchemaException: If the schema does not exist.
        """
        self._check_function_namespace(namespace)

        full_namespace = self._get_function_full_namespace(namespace)
        params = {"details": "true"}
        resp = self._rest_client.get(
            self._format_function_request_path(full_namespace),
            params=params,
            error_handler=FUNCTION_ERROR_HANDLER,
        )

        function_list_response = FunctionListResponse.from_json(resp.body)
        function_list_response.validate()

        return [GenericFunction(func) for func in function_list_response.functions()]

    def get_function(self, ident: NameIdentifier) -> Function:
        """Get a function by NameIdentifier from the catalog.

        Args:
            ident: A function identifier, which should be "schema.function" format.

        Returns:
            The function metadata.

        Raises:
            NoSuchFunctionException: If the function does not exist.
        """
        self._check_function_name_identifier(ident)

        full_namespace = self._get_function_full_namespace(ident.namespace())
        resp = self._rest_client.get(
            f"{self._format_function_request_path(full_namespace)}/"
            f"{rest_utils.encode_string(ident.name())}",
            error_handler=FUNCTION_ERROR_HANDLER,
        )

        function_response = FunctionResponse.from_json(resp.body)
        function_response.validate()

        return GenericFunction(function_response.function())

    def register_function(
        self,
        ident: NameIdentifier,
        comment: Optional[str],
        function_type: FunctionType,
        deterministic: bool,
        definitions: List[FunctionDefinition],
    ) -> Function:
        """Register a function with one or more definitions (overloads).

        Args:
            ident: The function identifier.
            comment: The optional function comment.
            function_type: The function type.
            deterministic: Whether the function is deterministic.
            definitions: The function definitions.

        Returns:
            The registered function.

        Raises:
            NoSuchSchemaException: If the schema does not exist.
            FunctionAlreadyExistsException: If the function already exists.
        """
        self._check_function_name_identifier(ident)

        full_namespace = self._get_function_full_namespace(ident.namespace())

        # Convert definitions to DTOs
        definition_dtos = [
            FunctionDefinitionDTO.from_function_definition(d) for d in definitions
        ]

        req = FunctionRegisterRequest(
            name=ident.name(),
            function_type=function_type,
            deterministic=deterministic,
            definitions=definition_dtos,
            comment=comment,
        )
        req.validate()

        resp = self._rest_client.post(
            self._format_function_request_path(full_namespace),
            req,
            error_handler=FUNCTION_ERROR_HANDLER,
        )

        function_response = FunctionResponse.from_json(resp.body)
        function_response.validate()

        return GenericFunction(function_response.function())

    def alter_function(
        self, ident: NameIdentifier, *changes: FunctionChange
    ) -> Function:
        """Applies FunctionChange changes to a function in the catalog.

        Args:
            ident: The NameIdentifier instance of the function to alter.
            changes: The FunctionChange instances to apply to the function.

        Returns:
            The updated Function instance.

        Raises:
            NoSuchFunctionException: If the function does not exist.
            IllegalArgumentException: If the change is rejected by the implementation.
        """
        self._check_function_name_identifier(ident)

        full_namespace = self._get_function_full_namespace(ident.namespace())
        updates = [self._to_function_update_request(change) for change in changes]
        req = FunctionUpdatesRequest(updates)
        req.validate()

        resp = self._rest_client.put(
            f"{self._format_function_request_path(full_namespace)}/"
            f"{rest_utils.encode_string(ident.name())}",
            req,
            error_handler=FUNCTION_ERROR_HANDLER,
        )

        function_response = FunctionResponse.from_json(resp.body)
        function_response.validate()

        return GenericFunction(function_response.function())

    def drop_function(self, ident: NameIdentifier) -> bool:
        """Drop a function from the catalog.

        Args:
            ident: A function identifier, which should be "schema.function" format.

        Returns:
            True if the function is dropped, False if the function did not exist.
        """
        self._check_function_name_identifier(ident)

        full_namespace = self._get_function_full_namespace(ident.namespace())
        resp = self._rest_client.delete(
            f"{self._format_function_request_path(full_namespace)}/"
            f"{rest_utils.encode_string(ident.name())}",
            error_handler=FUNCTION_ERROR_HANDLER,
        )

        drop_response = DropResponse.from_json(resp.body)
        drop_response.validate()

        return drop_response.dropped()

    def _format_function_request_path(self, ns: Namespace) -> str:
        """Format the request path for function operations.

        Args:
            ns: The full namespace (metalake.catalog.schema).

        Returns:
            The request path.
        """
        return (
            f"api/metalakes/{rest_utils.encode_string(ns.level(0))}"
            f"/catalogs/{rest_utils.encode_string(ns.level(1))}"
            f"/schemas/{rest_utils.encode_string(ns.level(2))}"
            f"/functions"
        )

    def _check_function_namespace(self, namespace: Namespace):
        """Check whether the namespace of a function is valid.

        Args:
            namespace: The namespace to check.

        Raises:
            IllegalArgumentException: If the namespace is invalid.
        """
        if namespace is None or namespace.length() != 1:
            raise IllegalArgumentException(
                f"Function namespace must be non-null and have 1 level, "
                f"the input namespace is {namespace}"
            )

    def _check_function_name_identifier(self, ident: NameIdentifier):
        """Check whether the NameIdentifier of a function is valid.

        Args:
            ident: The NameIdentifier to check.

        Raises:
            IllegalArgumentException: If the identifier is invalid.
        """
        if ident is None:
            raise IllegalArgumentException("NameIdentifier must not be null")
        if not ident.name():
            raise IllegalArgumentException("NameIdentifier name must not be empty")
        self._check_function_namespace(ident.namespace())

    def _get_function_full_namespace(self, function_namespace: Namespace) -> Namespace:
        """Get the full namespace of the function with the given function's short namespace.

        Args:
            function_namespace: The function's short namespace (schema name).

        Returns:
            The full namespace (metalake.catalog.schema).
        """
        return Namespace.of(
            self._catalog_namespace.level(0),
            self._catalog_name,
            function_namespace.level(0),
        )

    def _to_function_update_request(
        self, change: FunctionChange
    ) -> FunctionUpdateRequest:
        """Convert a FunctionChange to a FunctionUpdateRequest.

        Args:
            change: The function change.

        Returns:
            The corresponding update request.

        Raises:
            IllegalArgumentException: If the change type is not supported.
        """
        if isinstance(change, UpdateComment):
            return UpdateCommentRequest(change.new_comment())
        if isinstance(change, AddDefinition):
            definition_dto = FunctionDefinitionDTO.from_function_definition(
                change.definition()
            )
            return AddDefinitionRequest(definition_dto)
        if isinstance(change, RemoveDefinition):
            param_dtos = [
                FunctionParamDTO.from_function_param(p) for p in change.parameters()
            ]
            return RemoveDefinitionRequest(param_dtos)
        if isinstance(change, AddImpl):
            param_dtos = [
                FunctionParamDTO.from_function_param(p) for p in change.parameters()
            ]
            impl_dto = function_impl_dto_from_function_impl(change.implementation())
            return AddImplRequest(param_dtos, impl_dto)
        if isinstance(change, UpdateImpl):
            param_dtos = [
                FunctionParamDTO.from_function_param(p) for p in change.parameters()
            ]
            impl_dto = function_impl_dto_from_function_impl(change.implementation())
            return UpdateImplRequest(param_dtos, change.runtime().name, impl_dto)
        if isinstance(change, RemoveImpl):
            param_dtos = [
                FunctionParamDTO.from_function_param(p) for p in change.parameters()
            ]
            return RemoveImplRequest(param_dtos, change.runtime().name)
        raise IllegalArgumentException(f"Unknown function change type: {type(change)}")
