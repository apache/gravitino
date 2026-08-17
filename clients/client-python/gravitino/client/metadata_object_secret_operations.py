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

from typing import Dict

from gravitino.api.metadata_object import MetadataObject
from gravitino.api.secret.supports_secrets import SupportsSecrets
from gravitino.dto.responses.secrets_response import SecretsResponse
from gravitino.exceptions.handlers.credential_error_handler import (
    CREDENTIAL_ERROR_HANDLER,
)
from gravitino.rest.rest_utils import encode_string
from gravitino.utils import HTTPClient


class MetadataObjectSecretOperations(SupportsSecrets):
    _rest_client: HTTPClient
    _request_path: str

    def __init__(
        self,
        metalake_name: str,
        metadata_object: MetadataObject,
        rest_client: HTTPClient,
    ):
        self._rest_client = rest_client
        metadata_object_type = metadata_object.type().value
        metadata_object_fullname = metadata_object.full_name()
        self._request_path = (
            f"api/metalakes/{encode_string(metalake_name)}/objects/{metadata_object_type}/"
            f"{encode_string(metadata_object_fullname)}/secrets"
        )

    def get_secrets(self) -> Dict[str, str]:
        resp = self._rest_client.get(
            self._request_path,
            error_handler=CREDENTIAL_ERROR_HANDLER,
        )
        secret_resp = SecretsResponse.from_json(resp.body, infer_missing=True)
        secret_resp.validate()
        return secret_resp.secrets()
