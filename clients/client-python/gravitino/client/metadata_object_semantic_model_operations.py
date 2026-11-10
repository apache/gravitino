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

from gravitino.api.metadata_object import MetadataObject
from gravitino.api.semantic.supports_semantic_models import SupportsSemanticModels
from gravitino.dto.responses.entity_list_response import EntityListResponse
from gravitino.exceptions.handlers.semantic_model_error_handler import (
    SEMANTIC_MODEL_ERROR_HANDLER,
)
from gravitino.name_identifier import NameIdentifier
from gravitino.rest.rest_utils import encode_string
from gravitino.utils.http_client import HTTPClient


class MetadataObjectSemanticModelOperations(SupportsSemanticModels):
    """Shared relationship request handling for tables and Views."""

    def __init__(self, metalake: str, source: MetadataObject, rest_client: HTTPClient):
        self._rest_client = rest_client
        self._request_path = (
            f"api/metalakes/{encode_string(metalake)}/objects/"
            f"{source.type().name.lower()}/{encode_string(source.full_name())}"
            "/semantic-model-datasets"
        )

    def list_semantic_model_datasets(self) -> list[NameIdentifier]:
        response = self._rest_client.get(
            self._request_path,
            params={},
            error_handler=SEMANTIC_MODEL_ERROR_HANDLER,
        )
        result = EntityListResponse.from_json(response.body, infer_missing=True)
        result.validate()
        return result.identifiers()
