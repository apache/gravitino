"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import config

from gravitino.dto.metalake_dto import MetalakeDTO
from gravitino.dto.responses.base_response import BaseResponse
from gravitino.exceptions.base import IllegalArgumentException


@dataclass
class MetalakeResponse(BaseResponse):
    """Represents a response containing metalake information."""

    _metalake: Optional[MetalakeDTO] = field(metadata=config(field_name="metalake"))

    def metalake(self) -> MetalakeDTO:
        return self._metalake

    def validate(self):
        """Validates the response data.
        TODO: @throws IllegalArgumentException if the metalake, name or audit information is not set.
        """
        super().validate()

        if self._metalake is None:
            raise IllegalArgumentException("Metalake must not be null")

        if self._metalake.name() is None:
            raise IllegalArgumentException("Metalake 'name' must not be null and empty")

        if self._metalake.audit_info() is None:
            raise IllegalArgumentException("Metalake 'audit' must not be null")
