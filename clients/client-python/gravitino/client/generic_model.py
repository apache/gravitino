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

from typing import Optional

from gravitino.api.authorization.supports_roles import SupportsRoles
from gravitino.api.metadata_object import MetadataObject
from gravitino.api.metadata_objects import MetadataObjects
from gravitino.api.model.model import Model
from gravitino.api.policy.policy import Policy
from gravitino.api.policy.policy_list import PolicyList
from gravitino.api.policy.supports_policies import SupportsPolicies
from gravitino.api.secret.supports_secrets import SupportsSecrets
from gravitino.api.tag import Tag, TagList
from gravitino.api.tag.supports_tags import SupportsTags
from gravitino.client.metadata_object_policy_operations import (
    MetadataObjectPolicyOperations,
)
from gravitino.client.metadata_object_secret_operations import (
    MetadataObjectSecretOperations,
)
from gravitino.client.metadata_object_tag_operations import MetadataObjectTagOperations
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.model_dto import ModelDTO
from gravitino.exceptions.base import UnsupportedOperationException
from gravitino.namespace import Namespace
from gravitino.utils import HTTPClient
from gravitino.api.pagination import Page


class GenericModel(Model, SupportsTags, SupportsPolicies, SupportsSecrets):
    _model_dto: ModelDTO
    """The model DTO object."""

    def __init__(
        self, model_dto: ModelDTO, rest_client: HTTPClient, model_ns: Namespace
    ) -> None:
        self._model_dto = model_dto
        model_full_name = [model_ns.level(1), model_ns.level(2), model_dto.name()]
        model_object: MetadataObject = MetadataObjects.of(
            model_full_name, MetadataObject.Type.MODEL
        )
        self._model_tag_operations = MetadataObjectTagOperations(
            model_ns.level(0), model_object, rest_client
        )
        self._model_policy_operations = MetadataObjectPolicyOperations(
            model_ns.level(0), model_object, rest_client
        )
        self._object_secret_operations = MetadataObjectSecretOperations(
            model_ns.level(0), model_object, rest_client
        )

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, GenericModel):
            return False

        return self._model_dto == value._model_dto

    def __hash__(self) -> int:
        return hash(self._model_dto)

    def name(self) -> str:
        return self._model_dto.name()

    def comment(self) -> Optional[str]:
        return self._model_dto.comment()

    def properties(self) -> dict:
        return self._model_dto.properties()

    def latest_version(self) -> int:
        return self._model_dto.latest_version()

    def audit_info(self) -> AuditDTO:
        return self._model_dto.audit_info()

    def list_tags(self) -> list[str]:
        return self._model_tag_operations.list_tags()

    def list_tags_page(
        self, *, page_size: int, page_token: Optional[str] = None
    ) -> Page[str]:
        """Paged variant of ``list_tags``: one page, in ascending order.

        Pass the ``next_page_token`` of the previous page to read the next one.
        """
        return self._model_tag_operations.list_tags_page(
            page_size=page_size, page_token=page_token
        )

    def list_tags_with_visibility(self) -> TagList[str]:
        return self._model_tag_operations.list_tags_with_visibility()

    def list_tags_info(self) -> list[Tag]:
        return self._model_tag_operations.list_tags_info()

    def list_tags_info_page(
        self, *, page_size: int, page_token: Optional[str] = None
    ) -> Page[Tag]:
        """Paged variant of ``list_tags_info``: one page, in ascending order.

        Pass the ``next_page_token`` of the previous page to read the next one.
        """
        return self._model_tag_operations.list_tags_info_page(
            page_size=page_size, page_token=page_token
        )

    def list_tags_info_with_visibility(self) -> TagList[Tag]:
        return self._model_tag_operations.list_tags_info_with_visibility()

    def get_tag(self, name: str) -> Tag:
        return self._model_tag_operations.get_tag(name)

    def assign_tags(
        self,
        tags_to_add: list[str | dict[str, str | None]] | None = None,
        tags_to_remove: list[str | dict[str, str | None]] | None = None,
    ) -> list[str]:
        return self._model_tag_operations.assign_tags(tags_to_add, tags_to_remove)

    def associate_tags(
        self, tags_to_add: list[str], tags_to_remove: list[str]
    ) -> list[str]:
        return self._model_tag_operations.associate_tags(tags_to_add, tags_to_remove)

    def supports_tags(self) -> SupportsTags:
        return self

    def list_policies(self) -> list[str]:
        return self._model_policy_operations.list_policies()

    def list_policies_page(
        self, *, page_size: int, page_token: Optional[str] = None
    ) -> Page[str]:
        """Paged variant of ``list_policies``: one page, in ascending order.

        Pass the ``next_page_token`` of the previous page to read the next one.
        """
        return self._model_policy_operations.list_policies_page(
            page_size=page_size, page_token=page_token
        )

    def list_policies_with_visibility(self) -> PolicyList[str]:
        return self._model_policy_operations.list_policies_with_visibility()

    def list_policy_infos(self) -> list[Policy]:
        return self._model_policy_operations.list_policy_infos()

    def list_policy_infos_page(
        self, *, page_size: int, page_token: Optional[str] = None
    ) -> Page[Policy]:
        """Paged variant of ``list_policy_infos``: one page, in ascending order.

        Pass the ``next_page_token`` of the previous page to read the next one.
        """
        return self._model_policy_operations.list_policy_infos_page(
            page_size=page_size, page_token=page_token
        )

    def list_policy_infos_with_visibility(self) -> PolicyList[Policy]:
        return self._model_policy_operations.list_policy_infos_with_visibility()

    def supports_policies(self) -> SupportsPolicies:
        return self

    def support_secrets(self) -> SupportsSecrets:
        return self

    def get_secrets(self) -> dict:
        return self._object_secret_operations.get_secrets()

    def supports_roles(self) -> SupportsRoles:
        raise UnsupportedOperationException("Not supported yet.")
