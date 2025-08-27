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

from dataclasses import dataclass, field
from typing import List

from dataclasses_json import config

from gravitino.dto.model_version_dto import ModelVersionDTO
from gravitino.dto.responses.base_response import BaseResponse
from gravitino.exceptions.base import IllegalArgumentException


@dataclass
class ModelVersionListResponse(BaseResponse):
    """Represents a response for a list of model versions."""

    _versions: List[int] = field(metadata=config(field_name="versions"))

    def versions(self) -> List[int]:
        return self._versions

    def validate(self):
        """Validates the response data.

        Raises:
            IllegalArgumentException if versions are not set.
        """
        super().validate()

        if self._versions is None:
            raise IllegalArgumentException("versions must not be null")


@dataclass
class ModelVersionInfoListResponse(BaseResponse):
    """Represents a response for a list of model versions with their information."""

    _versions: List[ModelVersionDTO] = field(metadata=config(field_name="infos"))

    def versions(self) -> List[ModelVersionDTO]:
        return self._versions

    def validate(self):
        """Validates the response data.

        Raises:
            IllegalArgumentException if versions are not set.
        """
        super().validate()

        if self._versions is None:
            raise IllegalArgumentException("versions must not be null")

        for version in self._versions:
            if version is None:
                raise IllegalArgumentException("Model version must not be null")
            if version.version is None:
                raise IllegalArgumentException(
                    "Model version 'version' must not be null"
                )
            if version.uris() is None:
                raise IllegalArgumentException("Model version 'uri' must not be null")
            if version.audit_info() is None:
                raise IllegalArgumentException(
                    "Model version 'auditInfo' must not be null"
                )
