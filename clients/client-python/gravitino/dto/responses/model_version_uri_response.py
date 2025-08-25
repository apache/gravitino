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
from gravitino.exceptions.base import IllegalArgumentException


@dataclass
class ModelVersionUriResponse(BaseResponse):
    """Response for the model version uri."""

    _uri: str = field(metadata=config(field_name="uri"))

    def uri(self) -> str:
        return self._uri

    def validate(self):
        """Validates the response data.

        Raises:
            IllegalArgumentException if model version uri is not set.
        """
        super().validate()
        if self._uri is None or len(self.uri()) == 0:
            raise IllegalArgumentException("Model version uri must not be null")
