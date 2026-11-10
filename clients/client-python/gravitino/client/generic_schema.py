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

from typing import Dict, Optional

from gravitino.api.audit import Audit
from gravitino.api.authorization.supports_roles import SupportsRoles
from gravitino.api.metadata_object import MetadataObject
from gravitino.api.metadata_objects import MetadataObjects
from gravitino.api.policy.policy import Policy
from gravitino.api.policy.policy_list import PolicyList
from gravitino.api.policy.supports_policies import SupportsPolicies
from gravitino.api.schema import Schema
from gravitino.api.secret.supports_secrets import SupportsSecrets
from gravitino.api.tag.supports_tags import SupportsTags
from gravitino.api.tag.tag import Tag
from gravitino.api.tag.tag_list import TagList
from gravitino.client.metadata_object_policy_operations import (
    MetadataObjectPolicyOperations,
)
from gravitino.client.metadata_object_role_operations import (
    MetadataObjectRoleOperations,
)
from gravitino.client.metadata_object_secret_operations import (
    MetadataObjectSecretOperations,
)
from gravitino.client.metadata_object_tag_operations import MetadataObjectTagOperations
from gravitino.dto.schema_dto import SchemaDTO
from gravitino.utils.http_client import HTTPClient
from gravitino.api.pagination import Page


class GenericSchema(
    Schema,
    SupportsRoles,
    SupportsTags,
    SupportsPolicies,
    SupportsSecrets,
):
    def __init__(
        self,
        schema_dto: SchemaDTO,
        rest_client: HTTPClient,
        metalake: str,
        catalog: str,
    ) -> None:
        self._schema_dto = schema_dto
        metadata_object: MetadataObject = MetadataObjects.of(
            [
                catalog,
                schema_dto.name(),
            ],
            MetadataObject.Type.SCHEMA,
        )
        self._metadata_object_tag_operations = MetadataObjectTagOperations(
            metalake,
            metadata_object,
            rest_client,
        )
        self._metadata_object_policy_operations = MetadataObjectPolicyOperations(
            metalake,
            metadata_object,
            rest_client,
        )
        self._metadata_object_role_operations = MetadataObjectRoleOperations(
            metalake,
            metadata_object,
            rest_client,
        )
        self._object_secret_operations = MetadataObjectSecretOperations(
            metalake,
            metadata_object,
            rest_client,
        )

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, GenericSchema):
            return False

        return self._schema_dto == value._schema_dto

    def __hash__(self) -> int:
        return hash(self._schema_dto)

    def name(self) -> str:
        return self._schema_dto.name()

    def comment(self) -> str | None:
        return self._schema_dto.comment()

    def properties(self) -> Dict[str, str]:
        return self._schema_dto.properties()

    def audit_info(self) -> Audit:
        return self._schema_dto.audit_info()

    def list_tags(self) -> list[str]:
        return self._metadata_object_tag_operations.list_tags()

    def list_tags_page(
        self, *, page_size: int, page_token: Optional[str] = None
    ) -> Page[str]:
        """Paged variant of ``list_tags``: one page, in ascending order.

        Pass the ``next_page_token`` of the previous page to read the next one.
        """
        return self._metadata_object_tag_operations.list_tags_page(
            page_size=page_size, page_token=page_token
        )

    def list_tags_with_visibility(self) -> TagList[str]:
        return self._metadata_object_tag_operations.list_tags_with_visibility()

    def list_tags_info(self) -> list[Tag]:
        return self._metadata_object_tag_operations.list_tags_info()

    def list_tags_info_page(
        self, *, page_size: int, page_token: Optional[str] = None
    ) -> Page[Tag]:
        """Paged variant of ``list_tags_info``: one page, in ascending order.

        Pass the ``next_page_token`` of the previous page to read the next one.
        """
        return self._metadata_object_tag_operations.list_tags_info_page(
            page_size=page_size, page_token=page_token
        )

    def list_tags_info_with_visibility(self) -> TagList[Tag]:
        return self._metadata_object_tag_operations.list_tags_info_with_visibility()

    def get_tag(self, name: str) -> Tag:
        return self._metadata_object_tag_operations.get_tag(name)

    def assign_tags(
        self,
        tags_to_add: list[str | dict[str, str | None]] | None = None,
        tags_to_remove: list[str | dict[str, str | None]] | None = None,
    ) -> list[str]:
        return self._metadata_object_tag_operations.assign_tags(
            tags_to_add, tags_to_remove
        )

    def associate_tags(
        self, tags_to_add: list[str], tags_to_remove: list[str]
    ) -> list[str]:
        return self._metadata_object_tag_operations.associate_tags(
            tags_to_add, tags_to_remove
        )

    def supports_tags(self) -> SupportsTags:
        return self

    def list_policies(self) -> list[str]:
        return self._metadata_object_policy_operations.list_policies()

    def list_policies_page(
        self, *, page_size: int, page_token: Optional[str] = None
    ) -> Page[str]:
        """Paged variant of ``list_policies``: one page, in ascending order.

        Pass the ``next_page_token`` of the previous page to read the next one.
        """
        return self._metadata_object_policy_operations.list_policies_page(
            page_size=page_size, page_token=page_token
        )

    def list_policies_with_visibility(self) -> PolicyList[str]:
        return self._metadata_object_policy_operations.list_policies_with_visibility()

    def list_policy_infos(self) -> list[Policy]:
        return self._metadata_object_policy_operations.list_policy_infos()

    def list_policy_infos_page(
        self, *, page_size: int, page_token: Optional[str] = None
    ) -> Page[Policy]:
        """Paged variant of ``list_policy_infos``: one page, in ascending order.

        Pass the ``next_page_token`` of the previous page to read the next one.
        """
        return self._metadata_object_policy_operations.list_policy_infos_page(
            page_size=page_size, page_token=page_token
        )

    def list_policy_infos_with_visibility(self) -> PolicyList[Policy]:
        return (
            self._metadata_object_policy_operations.list_policy_infos_with_visibility()
        )

    def supports_policies(self) -> SupportsPolicies:
        return self

    def supports_roles(self) -> SupportsRoles:
        return self

    def list_binding_role_names(self) -> list[str]:
        return self._metadata_object_role_operations.list_binding_role_names()

    def list_binding_role_names_page(
        self, *, page_size: int, page_token: Optional[str] = None
    ) -> Page[str]:
        """Paged variant of ``list_binding_role_names``: one page, in ascending order.

        Pass the ``next_page_token`` of the previous page to read the next one.
        """
        return self._metadata_object_role_operations.list_binding_role_names_page(
            page_size=page_size, page_token=page_token
        )

    def support_secrets(self) -> SupportsSecrets:
        return self

    def get_secrets(self) -> Dict[str, str]:
        return self._object_secret_operations.get_secrets()
