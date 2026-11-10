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

from gravitino.api.metadata_object import MetadataObject
from gravitino.api.policy.policy import Policy
from gravitino.api.policy.policy_list import PolicyList
from gravitino.api.policy.supports_policies import SupportsPolicies
from gravitino.client.generic_policy import GenericPolicy
from gravitino.dto.responses.policy_response import (
    MetadataObjectPolicyListResponse,
    PolicyNameListResponse,
)
from gravitino.exceptions.handlers.policy_error_handler import POLICY_ERROR_HANDLER
from gravitino.rest.rest_utils import encode_string
from gravitino.utils.http_client import HTTPClient
from gravitino.api.pagination import Page, page_params


class MetadataObjectPolicyOperations(SupportsPolicies):
    """
    The implementation of SupportsPolicies. This helper is composed into
    metadata objects to provide policy operations for these objects.
    """

    POLICY_REQUEST_PATH = "api/metalakes/{}/objects/{}/{}/policies"

    def __init__(
        self,
        metalake_name: str,
        metadata_object: MetadataObject,
        rest_client: HTTPClient,
    ):
        super().__init__()
        self.metalake_name = metalake_name
        self.metadata_object = metadata_object
        self.rest_client = rest_client
        self.policy_request_path = (
            MetadataObjectPolicyOperations.POLICY_REQUEST_PATH.format(
                encode_string(metalake_name),
                metadata_object.type().name.lower(),
                encode_string(metadata_object.full_name()),
            )
        )

    def list_policies(self) -> list[str]:
        return self.list_policies_with_visibility().policies()

    def list_policies_with_visibility(self) -> PolicyList[str]:
        response = self.rest_client.get(
            self.policy_request_path,
            params={},
            error_handler=POLICY_ERROR_HANDLER,
        )
        list_policies_resp = PolicyNameListResponse.from_json(
            response.body, infer_missing=True
        )
        list_policies_resp.validate()

        return PolicyList(
            list_policies_resp.names,
            list_policies_resp.has_inaccessible_policies(),
        )

    def list_policies_page(
        self, *, page_size: int, page_token: Optional[str] = None
    ) -> Page[str]:
        """Paged variant of ``list_policies``: one page, in ascending order.

        Pass the ``next_page_token`` of the previous page to read the next one.
        """
        response = self.rest_client.get(
            self.policy_request_path,
            params={**{}, **page_params(page_size, page_token)},
            error_handler=POLICY_ERROR_HANDLER,
        )
        list_policies_resp = PolicyNameListResponse.from_json(
            response.body, infer_missing=True
        )
        list_policies_resp.validate()

        return Page(
            items=list(list_policies_resp.names),
            next_page_token=list_policies_resp.next_page_token(),
        )

    def list_policy_infos(self) -> list[Policy]:
        return self.list_policy_infos_with_visibility().policies()

    def list_policy_infos_with_visibility(self) -> PolicyList[Policy]:
        response = self.rest_client.get(
            self.policy_request_path,
            params={"details": "true"},
            error_handler=POLICY_ERROR_HANDLER,
        )

        list_info_resp = MetadataObjectPolicyListResponse.from_json(
            response.body, infer_missing=True
        )
        list_info_resp.validate()

        return PolicyList(
            [GenericPolicy(policy_dto) for policy_dto in list_info_resp.policies()],
            list_info_resp.has_inaccessible_policies(),
        )

    def list_policy_infos_page(
        self, *, page_size: int, page_token: Optional[str] = None
    ) -> Page[Policy]:
        """Paged variant of ``list_policy_infos``: one page, in ascending order.

        Pass the ``next_page_token`` of the previous page to read the next one.
        """
        response = self.rest_client.get(
            self.policy_request_path,
            params={**{"details": "true"}, **page_params(page_size, page_token)},
            error_handler=POLICY_ERROR_HANDLER,
        )

        list_info_resp = MetadataObjectPolicyListResponse.from_json(
            response.body, infer_missing=True
        )
        list_info_resp.validate()

        return Page(
            items=[
                GenericPolicy(policy_dto) for policy_dto in list_info_resp.policies()
            ],
            next_page_token=list_info_resp.next_page_token(),
        )
