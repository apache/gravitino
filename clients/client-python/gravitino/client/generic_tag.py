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

from gravitino.api.metadata_object import MetadataObject
from gravitino.api.tag.tag import Tag
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.responses.metadata_object_list_response import (
    MetadataObjectListResponse,
)
from gravitino.dto.tag_dto import TagDTO
from gravitino.exceptions.handlers.error_handler import ErrorHandler
from gravitino.exceptions.handlers.error_handlers import ErrorHandlers
from gravitino.rest.rest_utils import encode_string
from gravitino.utils import HTTPClient
from gravitino.utils.http_client import Response


class GenericTag(Tag, Tag.AssociatedObjects):
    """Represents a generic tag."""

    API_LIST_OBJECTS_ENDPOINT = "api/metalakes/{}/tags/{}/objects"

    def __init__(
        self,
        metalake: str,
        tag_dto: TagDTO,
        client: HTTPClient,
    ) -> None:
        self._metalake = metalake
        self._tag_dto = tag_dto
        self._client = client

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GenericTag):
            return False

        return self._tag_dto == other._tag_dto

    def __hash__(self) -> int:
        return hash(self._tag_dto)

    def name(self) -> str:
        """Get the name of the tag.

        Returns:
            str: The name of the tag.
        """
        return self._tag_dto.name()

    def comment(self) -> str:
        """Get the comment of the tag.

        Returns:
            str: The comment of the tag.
        """
        return self._tag_dto.comment()

    def properties(self) -> dict[str, str]:
        """Get the properties of the tag.

        Returns:
            Dict[str, str]: The properties of the tag.
        """
        return self._tag_dto.properties()

    def inherited(self) -> Optional[bool]:
        """Check if the tag is inherited from a parent object or not.

        If the tag is inherited, it will return `True`, if it is owned by the object itself, it will return `False`.

        **Note**. The return value is optional, only when the tag is associated with an object, and called from the
        object, the return value will be present. Otherwise, it will be empty.

        Returns:
            Optional[bool]:
                True if the tag is inherited, false if it is owned by the object itself. Empty if the
                tag is not associated with any object.
        """
        return self._tag_dto.inherited()

    def audit_info(self) -> AuditDTO:
        """
        Retrieve The audit information of the entity.

        Returns:
            AuditDTO: The audit information of the entity.
        """
        return self._tag_dto.audit_info()

    def associated_objects(self) -> Tag.AssociatedObjects:
        """
        The associated objects of the tag.

        Returns:
            AssociatedObjects: The associated objects of the tag.
        """
        return self

    def objects(self) -> list[MetadataObject]:
        """
        Retrieve The list of objects that are associated with this tag.

        Returns:
            list[MetadataObject]: The list of objects that are associated with this tag.
        """
        url = self.API_LIST_OBJECTS_ENDPOINT.format(
            self._metalake,
            encode_string(self.name()),
        )

        response = self.get_response(url, ErrorHandlers.tag_error_handler())
        objects_resp = MetadataObjectListResponse.from_json(
            response.body, infer_missing=True
        )
        objects_resp.validate()

        return objects_resp.metadata_objects()

    def get_response(self, url: str, error_handler: ErrorHandler) -> Response:
        """
        Get the response from the server. for test convenience.

        Args:
            url (str): The url to get the response from.
            error_handler (ErrorHandlers): The error handler to use.

        Returns:
            Response: The response from the server.
        """
        return self._client.get(
            url,
            error_handler=error_handler,
        )
