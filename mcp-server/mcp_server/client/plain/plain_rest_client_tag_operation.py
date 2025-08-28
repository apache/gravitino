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

from mcp_server.client.plain.exception import GravitinoException
from mcp_server.client.plain.utils import extract_content_from_response
from mcp_server.client.tag_operation import TagOperation


class PlainRESTClientTagOperation(TagOperation):
    def __init__(self, metalake_name: str, rest_client):
        self.metalake_name = metalake_name
        self.rest_client = rest_client

    async def list_of_tags(self) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/tags?details=true"
        )
        return extract_content_from_response(response, "tags", [])

    async def get_tag_by_name(self, tag_name: str) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/tags/{tag_name}"
        )
        return extract_content_from_response(response, "tag", {})

    async def create_tag(
        self, tag_name: str, tag_comment: str, tag_properties: dict
    ) -> str:
        response = await self.rest_client.post(
            f"/api/metalakes/{self.metalake_name}/tags",
            json={
                "name": tag_name,
                "comment": tag_comment,
                "properties": tag_properties,
            },
        )
        return extract_content_from_response(response, "tag", {})

    async def alter_tag(self, tag_name: str, updates: list) -> str:
        response = await self.rest_client.put(
            f"/api/metalakes/{self.metalake_name}/tags/{tag_name}",
            json={
                "updates": updates,
            },
        )
        return extract_content_from_response(response, "tag", {})

    async def delete_tag(self, name: str) -> None:
        response = await self.rest_client.delete(
            f"/api/metalakes/{self.metalake_name}/tags/{name}"
        )
        if response.status_code != 200:
            raise GravitinoException(
                f"Failed to delete tag {name}: {response.text}"
            )
        return None

    async def associate_tag_with_metadata(
        self,
        metadata_full_name: str,
        metadata_type: str,
        tags_to_associate: list,
        tags_to_disassociate: list,
    ) -> str:
        response = await self.rest_client.post(
            f"/api/metalakes/{self.metalake_name}/objects/{metadata_type}/{metadata_full_name}/tags",
            json={
                "tagsToAdd": tags_to_associate,
                "tagsToRemove": tags_to_disassociate,
            },
        )
        return extract_content_from_response(response, "names", [])

    async def list_tags_for_metadata(
        self, metadata_full_name: str, metadata_type: str
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/objects/{metadata_type}/{metadata_full_name}/tags?details=true"
        )
        return extract_content_from_response(response, "names", [])

    async def list_metadata_by_tag(self, tag_name: str) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/tags/{tag_name}/objects"
        )
        return extract_content_from_response(response, "metadataObjects", [])
