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

from copy import deepcopy
from typing import Optional

from gravitino.api.domain.domain import Domain
from gravitino.api.domain.domain_member import DomainMember
from gravitino.api.domain.domain_member_filter import DomainMemberFilter
from gravitino.api.domain.domain_relation import DomainRelation
from gravitino.api.tag.tag_value import TagValue
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.domain_dto import DomainBackingTagDTO, DomainDTO
from gravitino.dto.requests.domain_create_request import (
    BackingTagCreate,
    DomainCreateRequest,
)
from gravitino.dto.requests.domain_set_request import (
    DomainBackingTagSetRequest,
    DomainParentSetRequest,
    DomainSetRequest,
)
from gravitino.dto.responses.domain_response import (
    DomainListResponse,
    DomainMemberListResponse,
    DomainNamesListResponse,
    DomainResponse,
)
from gravitino.exceptions.handlers.domain_error_handler import DOMAIN_ERROR_HANDLER
from gravitino.rest.rest_utils import encode_string
from gravitino.utils.http_client import HTTPClient, Response
from gravitino.utils.precondition import Precondition


class GenericDomain(Domain):
    """A domain snapshot whose operations address current server state by name."""

    def __init__(
        self, metalake: str, domain_dto: DomainDTO, client: HTTPClient
    ) -> None:
        self._metalake = metalake
        self._domain = deepcopy(domain_dto)
        self._client = client
        self._path = f"api/metalakes/{encode_string(metalake)}/domains/{encode_string(self.name())}"

    def name(self) -> str:
        return self._domain.name()

    def comment(self) -> Optional[str]:
        return self._domain.comment()

    def properties(self) -> dict[str, str]:
        return self._domain.properties()

    def status(self) -> Domain.Status:
        return self._domain.status()

    def parent(self) -> Optional[str]:
        return self._domain.parent()

    def backing_tag(self) -> Optional[TagValue]:
        return self._domain.backing_tag()

    def audit_info(self) -> AuditDTO:
        return deepcopy(self._domain.audit_info())

    def relations(self) -> list[DomainRelation]:
        return deepcopy(self._domain.relations())

    def publish(self) -> Domain:
        return self._snapshot(
            self._client.patch(
                self._path,
                json=DomainSetRequest(Domain.Status.PUBLISHED),
                error_handler=DOMAIN_ERROR_HANDLER,
            )
        )

    def unpublish(self) -> Domain:
        return self._snapshot(
            self._client.patch(
                self._path,
                json=DomainSetRequest(Domain.Status.DRAFT),
                error_handler=DOMAIN_ERROR_HANDLER,
            )
        )

    def attach_to_parent(self, parent: str) -> Domain:
        request = DomainParentSetRequest(parent)
        request.validate()
        return self._snapshot(
            self._client.put(
                self._path + "/parent",
                json=request,
                error_handler=DOMAIN_ERROR_HANDLER,
            )
        )

    def detach_from_parent(self) -> Domain:
        return self._snapshot(
            self._client.delete(
                self._path + "/parent",
                error_handler=DOMAIN_ERROR_HANDLER,
            )
        )

    def update_backing_tag(self, backing_tag: Optional[TagValue]) -> Domain:
        request = DomainBackingTagSetRequest(
            DomainBackingTagDTO(backing_tag.name(), backing_tag.value())
            if backing_tag is not None
            else None
        )
        request.validate()
        return self._snapshot(
            self._client.put(
                self._path + "/backingTag",
                json=request,
                error_handler=DOMAIN_ERROR_HANDLER,
            )
        )

    def create_subdomain(
        self,
        name: str,
        comment: Optional[str],
        properties: Optional[dict[str, str]],
        backing_tag: TagValue,
        create_backing_tag: bool,
    ) -> Domain:
        Precondition.check_argument(
            backing_tag is not None, "backing_tag must not be None"
        )
        request = DomainCreateRequest(
            name,
            comment,
            properties,
            self.name(),
            BackingTagCreate(
                backing_tag.name(), backing_tag.value(), create_backing_tag
            ),
        )
        request.validate()
        return self._snapshot(
            self._client.post(
                f"api/metalakes/{encode_string(self._metalake)}/domains",
                json=request,
                error_handler=DOMAIN_ERROR_HANDLER,
            )
        )

    def list_subdomains(self) -> list[str]:
        response = self._client.get(
            self._path + "/subdomains", error_handler=DOMAIN_ERROR_HANDLER
        )
        result = DomainNamesListResponse.from_json(response.body, infer_missing=True)
        result.validate()
        return result.names()

    def list_subdomains_info(self) -> list[Domain]:
        response = self._client.get(
            self._path + "/subdomains",
            params={"details": "true"},
            error_handler=DOMAIN_ERROR_HANDLER,
        )
        result = DomainListResponse.from_json(response.body, infer_missing=True)
        result.validate()
        return [
            GenericDomain(self._metalake, dto, self._client) for dto in result.domains()
        ]

    def list_members(self, member_filter: DomainMemberFilter) -> list[DomainMember]:
        Precondition.check_argument(
            member_filter is not None, "member_filter must not be None"
        )
        params = {}
        if member_filter.types:
            params["types"] = ",".join(t.name for t in member_filter.types)
        if member_filter.name_contains is not None:
            params["nameContains"] = member_filter.name_contains
        if member_filter.domains:
            # HTTPClient uses urlencode(doseq=True), preserving commas within names.
            params["domains"] = list(member_filter.domains)
        response = self._client.get(
            self._path + "/objects", params=params, error_handler=DOMAIN_ERROR_HANDLER
        )
        result = DomainMemberListResponse.from_json(response.body, infer_missing=True)
        result.validate()
        return [dto.member() for dto in result.members()]

    def _snapshot(self, response: Response) -> Domain:
        result = DomainResponse.from_json(response.body, infer_missing=True)
        result.validate()
        return GenericDomain(self._metalake, result.domain(), self._client)
