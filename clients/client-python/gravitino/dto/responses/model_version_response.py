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
from dataclasses import field, dataclass

from dataclasses_json import config

from gravitino.dto.model_version_dto import ModelVersionDTO
from gravitino.dto.responses.base_response import BaseResponse
from gravitino.exceptions.base import IllegalArgumentException


@dataclass
class ModelVersionResponse(BaseResponse):
    """Represents a response for a model version."""

    _model_version: ModelVersionDTO = field(metadata=config(field_name="modelVersion"))

    def model_version(self) -> ModelVersionDTO:
        """Returns the model version."""
        return self._model_version

    def validate(self):
        """Validates the response data.

        Raises:
            IllegalArgumentException if the model version is not set.
        """
        super().validate()

        if self._model_version is None:
            raise IllegalArgumentException("Model version must not be null")
        if self._model_version.version() is None:
            raise IllegalArgumentException("Model version 'version' must not be null")
        if self._model_version.uris() is None:
            raise IllegalArgumentException("Model version 'uri' must not be null")
        if self._model_version.audit_info() is None:
            raise IllegalArgumentException("Model version 'auditInfo' must not be null")
