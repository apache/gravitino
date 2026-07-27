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

from gravitino.api.audit import Audit
from gravitino.api.metadata_object import MetadataObject
from gravitino.api.metadata_objects import MetadataObjects
from gravitino.api.rel.column import Column
from gravitino.api.rel.representation import Representation
from gravitino.api.rel.view import View
from gravitino.api.tag.supports_tags import SupportsTags
from gravitino.api.tag.tag import Tag
from gravitino.client.metadata_object_tag_operations import MetadataObjectTagOperations
from gravitino.dto.rel.view_dto import ViewDTO
from gravitino.namespace import Namespace
from gravitino.utils.http_client import HTTPClient


class GenericView(View, SupportsTags):
    """A generic implementation of the View interface."""

    def __init__(
        self,
        view_dto: ViewDTO,
        rest_client: HTTPClient,
        view_namespace: Namespace,
    ):
        """Create a GenericView from a ViewDTO.

        Args:
            view_dto: The view DTO.
            rest_client: The REST client for tag operations.
            view_namespace: The full view namespace in metalake.catalog.schema format.
        """
        self._view_dto = view_dto
        view_object: MetadataObject = MetadataObjects.of(
            [
                view_namespace.level(1),
                view_namespace.level(2),
                view_dto.name(),
            ],
            MetadataObject.Type.VIEW,
        )
        self._object_tag_operations = MetadataObjectTagOperations(
            view_namespace.level(0),
            view_object,
            rest_client,
        )

    def name(self) -> str:
        return self._view_dto.name()

    def comment(self) -> Optional[str]:
        return self._view_dto.comment()

    def columns(self) -> list[Column]:
        return self._view_dto.columns()

    def representations(self) -> list[Representation]:
        return self._view_dto.representations()

    def default_catalog(self) -> Optional[str]:
        return self._view_dto.default_catalog()

    def default_schema(self) -> Optional[str]:
        return self._view_dto.default_schema()

    def properties(self) -> dict[str, str]:
        return self._view_dto.properties()

    def audit_info(self) -> Audit:
        return self._view_dto.audit_info()

    def list_tags(self) -> list[str]:
        return self._object_tag_operations.list_tags()

    def list_tags_info(self) -> list[Tag]:
        return self._object_tag_operations.list_tags_info()

    def get_tag(self, name: str) -> Tag:
        return self._object_tag_operations.get_tag(name)

    def associate_tags(
        self, tags_to_add: list[str], tags_to_remove: list[str]
    ) -> list[str]:
        return self._object_tag_operations.associate_tags(
            tags_to_add,
            tags_to_remove,
        )

    def supports_tags(self) -> SupportsTags:
        return self
