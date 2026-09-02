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

from dataclasses_json import config

from gravitino.dto.responses.base_response import BaseResponse
from gravitino.dto.semantic.semantic_model_dto import SemanticModelDTO
from gravitino.exceptions.base import IllegalArgumentException


@dataclass
class SemanticModelResponse(BaseResponse):
    """Represents a response containing one Semantic Model."""

    _semantic_model: SemanticModelDTO = field(
        metadata=config(field_name="semanticModel")
    )

    def semantic_model(self) -> SemanticModelDTO:
        """Returns the Semantic Model DTO."""
        return self._semantic_model

    def validate(self):
        """Validates the response data.

        Raises:
            IllegalArgumentException: If the Semantic Model is not set or incomplete.
        """
        super().validate()

        if self._semantic_model is None:
            raise IllegalArgumentException("semanticModel must not be null")
        if not self._semantic_model.name():
            raise IllegalArgumentException(
                "semanticModel 'name' must not be null or empty"
            )
        self._semantic_model.definition()
        if self._semantic_model.audit_info() is None:
            raise IllegalArgumentException("semanticModel 'audit' must not be null")
