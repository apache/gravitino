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

from gravitino.dto.function.function_dto import FunctionDTO
from gravitino.dto.responses.base_response import BaseResponse
from gravitino.exceptions.base import IllegalArgumentException


@dataclass
class FunctionResponse(BaseResponse):
    """Response object for function-related operations."""

    _function: FunctionDTO = field(metadata=config(field_name="function"))

    def function(self) -> FunctionDTO:
        """Returns the function DTO object."""
        return self._function

    def validate(self):
        """Validates the response data.

        Raises:
            IllegalArgumentException: If function identifiers are not set.
        """
        super().validate()

        if self._function is None:
            raise IllegalArgumentException("function must not be null")
        if not self._function.name():
            raise IllegalArgumentException("function 'name' must not be null or empty")
        if self._function.function_type() is None:
            raise IllegalArgumentException("function 'functionType' must not be null")
        if self._function.definitions() is None:
            raise IllegalArgumentException("function 'definitions' must not be null")
