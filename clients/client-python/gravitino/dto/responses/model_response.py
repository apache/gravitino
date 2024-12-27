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

from gravitino.dto.model_dto import ModelDTO
from gravitino.dto.responses.base_response import BaseResponse
from gravitino.exceptions.base import IllegalArgumentException


@dataclass
class ModelResponse(BaseResponse):
    """Response object for model-related operations."""

    _model: ModelDTO = field(metadata=config(field_name="model"))

    def model(self) -> ModelDTO:
        """Returns the model DTO object."""
        return self._model

    def validate(self):
        """Validates the response data.

        Raises:
            IllegalArgumentException if model identifiers are not set.
        """
        super().validate()

        if self._model is None:
            raise IllegalArgumentException("model must not be null")
        if not self._model.name():
            raise IllegalArgumentException("model 'name' must not be null or empty")
        if self._model.latest_version() is None:
            raise IllegalArgumentException("model 'latestVersion' must not be null")
        if self._model.audit_info() is None:
            raise IllegalArgumentException("model 'auditInfo' must not be null")
