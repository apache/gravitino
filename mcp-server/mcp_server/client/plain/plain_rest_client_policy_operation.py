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

from mcp_server.client import PolicyOperation
from mcp_server.client.plain.utils import (
    encode_path_segment,
    extract_content_from_response,
    extract_response,
)


class PlainRESTClientPolicyOperation(PolicyOperation):
    """
    Implementation of PolicyOperation using a plain REST client.
    """

    def __init__(self, metalake_name: str, rest_client):
        self.metalake_name = metalake_name
        self.rest_client = rest_client

    # pylint: disable=R0917
    async def create_policy(
        self,
        policy_name: str,
        policy_type: str,
        comment: str,
        enabled: bool,
        content: dict,
    ) -> str:
        response = await self.rest_client.post(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}/policies",
            json={
                "name": policy_name,
                "policyType": policy_type,
                "comment": comment,
                "enabled": enabled,
                "content": content,
            },
        )
        return extract_content_from_response(response, "policy", {})

    async def alter_policy(self, policy_name: str, updates: list) -> str:
        response = await self.rest_client.put(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/policies/{encode_path_segment(policy_name)}",
            json={
                "updates": updates,
            },
        )
        return extract_content_from_response(response, "policy", {})

    async def delete_policy(self, policy_name: str) -> str:
        response = await self.rest_client.delete(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/policies/{encode_path_segment(policy_name)}"
        )
        return extract_content_from_response(response, "dropped", False)

    async def set_policy(self, policy_name: str, enable: bool) -> str:
        response = await self.rest_client.patch(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/policies/{encode_path_segment(policy_name)}",
            json={"enable": enable},
        )
        return extract_response(response)

    async def get_list_of_policies(self) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}/policies?details=true"
        )
        return extract_content_from_response(response, "policies", [])

    async def load_policy(
        self,
        policy_name: str,
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}/policies/{encode_path_segment(policy_name)}"
        )
        return extract_content_from_response(response, "policy", {})

    async def associate_policy_with_metadata(
        self,
        metadata_full_name: str,
        metadata_type: str,
        policies_to_add: list,
        policies_to_remove: list,
    ) -> str:
        response = await self.rest_client.post(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/objects/{encode_path_segment(metadata_type)}"
            f"/{encode_path_segment(metadata_full_name)}/policies",
            json={
                "policiesToAdd": policies_to_add,
                "policiesToRemove": policies_to_remove,
            },
        )
        return extract_content_from_response(response, "names", [])

    async def get_policy_for_metadata(
        self, metadata_full_name: str, metadata_type: str, policy_name: str
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/objects/{encode_path_segment(metadata_type)}"
            f"/{encode_path_segment(metadata_full_name)}"
            f"/policies/{encode_path_segment(policy_name)}",
        )
        return extract_content_from_response(response, "policy", {})

    async def list_policies_for_metadata(
        self, metadata_full_name: str, metadata_type: str
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/objects/{encode_path_segment(metadata_type)}"
            f"/{encode_path_segment(metadata_full_name)}/policies?details=true",
        )
        return extract_content_from_response(response, "policies", [])

    async def list_metadata_by_policy(self, policy_name: str) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/policies/{encode_path_segment(policy_name)}/objects"
        )
        return extract_content_from_response(response, "metadataObjects", [])
