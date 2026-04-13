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

from gravitino.api.authorization.supports_roles import SupportsRoles
from gravitino.api.metadata_object import MetadataObject
from gravitino.dto.responses.role_response import RoleNamesListResponse
from gravitino.exceptions.handlers.role_error_handler import (
    ROLE_ERROR_HANDLER,
)
from gravitino.rest.rest_utils import encode_string
from gravitino.utils.http_client import HTTPClient


class MetadataObjectRoleOperations(SupportsRoles):
    """
    Represents a response for a list of entity names.
    """

    ROLE_REQUEST_PATH = "api/metalakes/{}/objects/{}/{}/roles"

    def __init__(
        self,
        metalake_name: str,
        metadata_object: MetadataObject,
        rest_client: HTTPClient,
    ) -> None:
        super().__init__()
        self.metalake_name = metalake_name
        self.metadata_object = metadata_object
        self.rest_client = rest_client
        self.role_request_path = MetadataObjectRoleOperations.ROLE_REQUEST_PATH.format(
            encode_string(metalake_name),
            metadata_object.type().name.lower(),
            encode_string(metadata_object.full_name()),
        )

    def list_binding_role_names(self) -> list[str]:
        response = self.rest_client.get(
            self.role_request_path, params={}, error_handler=ROLE_ERROR_HANDLER
        )
        role_names_list_resp = RoleNamesListResponse.from_json(
            response.body, infer_missing=True
        )
        role_names_list_resp.validate()
        return role_names_list_resp.role_names()
