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
from gravitino.api.function.function_definition import FunctionDefinition
from gravitino.api.function.function_type import FunctionType
from gravitino.api.metadata_object import MetadataObject
from gravitino.api.metadata_objects import MetadataObjects
from gravitino.api.tag.supports_tags import SupportsTags
from gravitino.api.tag.tag import Tag
from gravitino.client.metadata_object_tag_operations import MetadataObjectTagOperations
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.function.function_dto import FunctionDTO
from gravitino.namespace import Namespace
from gravitino.utils.http_client import HTTPClient


class GenericFunction(Function, SupportsTags):
    """A generic implementation of the Function interface."""

    def __init__(
        self,
        function_dto: FunctionDTO,
        rest_client: HTTPClient,
        function_namespace: Namespace,
    ):
        """Create a GenericFunction from a FunctionDTO.

        Args:
            function_dto: The function DTO.
            rest_client: The REST client for tag operations.
            function_namespace: The full function namespace in metalake.catalog.schema format.
        """
        self._function_dto = function_dto
        function_object: MetadataObject = MetadataObjects.of(
            [
                function_namespace.level(1),
                function_namespace.level(2),
                function_dto.name(),
            ],
            MetadataObject.Type.FUNCTION,
        )
        self._object_tag_operations = MetadataObjectTagOperations(
            function_namespace.level(0),
            function_object,
            rest_client,
        )

    def name(self) -> str:
        """Returns the function name."""
        return self._function_dto.name()

    def function_type(self) -> FunctionType:
        """Returns the function type."""
        return self._function_dto.function_type()

    def deterministic(self) -> bool:
        """Returns whether the function is deterministic."""
        return self._function_dto.deterministic()

    def comment(self) -> Optional[str]:
        """Returns the optional comment of the function."""
        return self._function_dto.comment()

    def definitions(self) -> List[FunctionDefinition]:
        """Returns the definitions of the function."""
        return self._function_dto.definitions()

    def audit_info(self) -> Optional[AuditDTO]:
        """Returns the audit information."""
        return self._function_dto.audit_info()

    def list_tags(self) -> list[str]:
        """List the tag names associated with the function."""
        return self._object_tag_operations.list_tags()

    def list_tags_info(self) -> list[Tag]:
        """List the tags associated with the function."""
        return self._object_tag_operations.list_tags_info()

    def get_tag(self, name: str) -> Tag:
        """Get an associated tag by name."""
        return self._object_tag_operations.get_tag(name)

    def associate_tags(
        self, tags_to_add: list[str], tags_to_remove: list[str]
    ) -> list[str]:
        """Associate or disassociate tags with the function."""
        return self._object_tag_operations.associate_tags(
            tags_to_add,
            tags_to_remove,
        )

    def supports_tags(self) -> SupportsTags:
        """Return the function's tag operations."""
        return self
