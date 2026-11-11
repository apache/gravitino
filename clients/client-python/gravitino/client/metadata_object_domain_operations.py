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

from gravitino.api.domain.domain import Domain
from gravitino.api.domain.domain_member import DomainMember
from gravitino.api.domain.supports_domains import SupportsDomains
from gravitino.api.metadata_object import MetadataObject
from gravitino.client.generic_domain import GenericDomain
from gravitino.dto.responses.domain_response import (
    DomainListResponse,
    DomainNamesListResponse,
)
from gravitino.exceptions.handlers.domain_error_handler import DOMAIN_ERROR_HANDLER
from gravitino.rest.rest_utils import encode_string
from gravitino.utils.http_client import HTTPClient
from gravitino.utils.precondition import Precondition


class MetadataObjectDomainOperations(SupportsDomains):
    """Domain lookup for any supported leaf metadata object, including topics."""

    def __init__(
        self,
        metalake_name: str,
        metadata_object: MetadataObject,
        rest_client: HTTPClient,
    ):
        Precondition.check_argument(
            metadata_object.type() in DomainMember.SUPPORTED_TYPES,
            "Only leaf metadata objects support domains",
        )
        self._metalake = metalake_name
        self._client = rest_client
        self._path = (
            f"api/metalakes/{encode_string(metalake_name)}/objects/"
            f"{metadata_object.type().value}/{encode_string(metadata_object.full_name())}/domains"
        )

    def list_domains(self) -> list[str]:
        response = self._client.get(
            self._path, params={}, error_handler=DOMAIN_ERROR_HANDLER
        )
        result = DomainNamesListResponse.from_json(response.body, infer_missing=True)
        result.validate()
        return result.names()

    def list_domains_info(self) -> list[Domain]:
        response = self._client.get(
            self._path, params={"details": "true"}, error_handler=DOMAIN_ERROR_HANDLER
        )
        result = DomainListResponse.from_json(response.body, infer_missing=True)
        result.validate()
        return [
            GenericDomain(self._metalake, dto, self._client) for dto in result.domains()
        ]
