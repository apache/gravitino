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

from gravitino.api.metadata_object import MetadataObject
from gravitino.api.tag.supports_tags import SupportsTags
from gravitino.api.tag.tag import Tag
from gravitino.client.generic_tag import GenericTag
from gravitino.dto.requests.tag_associate_request import TagsAssociateRequest
from gravitino.dto.responses.tag_response import (
    TagListResponse,
    TagNamesListResponse,
    TagResponse,
)
from gravitino.exceptions.handlers.tag_error_handler import TAG_ERROR_HANDLER
from gravitino.rest.rest_utils import encode_string
from gravitino.utils.http_client import HTTPClient


class MetadataObjectTagOperations(SupportsTags):
    """
    The implementation of SupportsTags. This interface will be composited into catalog, schema,
    table, fileset and topic to provide tag operations for these metadata objects
    """

    TAG_REQUEST_PATH = "api/metalakes/{}/objects/{}/{}/tags"

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
        self.tag_request_path = MetadataObjectTagOperations.TAG_REQUEST_PATH.format(
            encode_string(metalake_name),
            metadata_object.type().name.lower(),
            encode_string(metadata_object.full_name()),
        )

    def list_tags(self) -> list[str]:
        response = self.rest_client.get(
            self.tag_request_path,
            params={},
            error_handler=TAG_ERROR_HANDLER,
        )
        list_tags_resp = TagNamesListResponse.from_json(
            response.body, infer_missing=True
        )
        list_tags_resp.validate()

        return list_tags_resp.tag_names()

    def list_tags_info(self) -> list[Tag]:
        response = self.rest_client.get(
            self.tag_request_path,
            params={"details": "true"},
            error_handler=TAG_ERROR_HANDLER,
        )

        list_info_resp = TagListResponse.from_json(response.body, infer_missing=True)
        list_info_resp.validate()

        return [
            GenericTag(
                self.metalake_name,
                tag_dto,
                self.rest_client,
            )
            for tag_dto in list_info_resp.tags()
        ]

    def get_tag(self, name) -> Tag:
        response = self.rest_client.get(
            f"{self.tag_request_path}/{encode_string(name)}",
            params={},
            error_handler=TAG_ERROR_HANDLER,
        )

        get_resp = TagResponse.from_json(response.body, infer_missing=True)
        get_resp.validate()

        return GenericTag(
            self.metalake_name,
            get_resp.tag(),
            self.rest_client,
        )

    def associate_tags(
        self, tags_to_add: list[str], tags_to_remove: list[str]
    ) -> list[str]:
        associate_resp = TagsAssociateRequest(tags_to_add, tags_to_remove)
        associate_resp.validate()

        response = self.rest_client.post(
            self.tag_request_path,
            json=associate_resp,
            error_handler=TAG_ERROR_HANDLER,
        )

        associate_resp = TagNamesListResponse.from_json(
            response.body, infer_missing=True
        )
        associate_resp.validate()

        return associate_resp.tag_names()
