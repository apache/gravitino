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
from typing import List

from dataclasses_json import config

from gravitino.dto.metalake_dto import MetalakeDTO
from gravitino.dto.responses.base_response import BaseResponse


@dataclass
class MetalakeListResponse(BaseResponse):
    """Represents a response containing a list of metalakes."""

    _metalakes: List[MetalakeDTO] = field(metadata=config(field_name="metalakes"))

    def metalakes(self) -> List[MetalakeDTO]:
        return self._metalakes

    def validate(self):
        """Validates the response data.

        Raises:
            IllegalArgumentException if catalog identifiers are not set.
        """
        super().validate()

        if self._metalakes is None:
            raise ValueError("metalakes must be non-null")

        for metalake in self._metalakes:
            if not metalake.name():
                raise ValueError("metalake 'name' must not be null and empty")
            if not metalake.audit_info():
                raise ValueError("metalake 'audit' must not be null")
