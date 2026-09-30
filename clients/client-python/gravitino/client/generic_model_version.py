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
from typing import Dict, List, Optional

from gravitino.api.metadata_object import MetadataObject
from gravitino.api.metadata_objects import MetadataObjects
from gravitino.api.model.model_version import ModelVersion
from gravitino.api.secret.supports_secrets import SupportsSecrets
from gravitino.client.metadata_object_secret_operations import (
    MetadataObjectSecretOperations,
)
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.model_version_dto import ModelVersionDTO
from gravitino.name_identifier import NameIdentifier
from gravitino.utils import HTTPClient


class GenericModelVersion(ModelVersion, SupportsSecrets):
    _model_version_dto: ModelVersionDTO
    """The model version DTO object."""

    def __init__(
        self,
        model_version_dto: ModelVersionDTO,
        rest_client: HTTPClient,
        model_full_ident: NameIdentifier,
    ):
        self._model_version_dto = model_version_dto
        model_version_object: MetadataObject = MetadataObjects.of(
            [
                model_full_ident.namespace().level(1),
                model_full_ident.namespace().level(2),
                model_full_ident.name(),
                str(model_version_dto.version()),
            ],
            MetadataObject.Type.MODEL_VERSION,
        )
        self._object_secret_operations = MetadataObjectSecretOperations(
            model_full_ident.namespace().level(0),
            model_version_object,
            rest_client,
        )

    def version(self) -> int:
        return self._model_version_dto.version()

    def comment(self) -> Optional[str]:
        return self._model_version_dto.comment()

    def aliases(self) -> List[str]:
        return self._model_version_dto.aliases()

    def uris(self) -> Dict[str, str]:
        return self._model_version_dto.uris()

    def properties(self) -> Dict[str, str]:
        return self._model_version_dto.properties()

    def audit_info(self) -> AuditDTO:
        return self._model_version_dto.audit_info()

    def support_secrets(self) -> SupportsSecrets:
        return self

    def get_secrets(self) -> Dict[str, str]:
        return self._object_secret_operations.get_secrets()
