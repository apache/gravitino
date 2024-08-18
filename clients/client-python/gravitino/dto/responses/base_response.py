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

from dataclasses_json import config
from gravitino.exceptions.base import IllegalArgumentException

from gravitino.rest.rest_message import RESTResponse


@dataclass
class BaseResponse(RESTResponse):
    """Represents a base response for REST API calls."""

    _code: int = field(metadata=config(field_name="code"))

    def code(self) -> int:
        return self._code

    def validate(self):
        """Validates the response code.
        @throws IllegalArgumentException if code value is negative.
        """
        if self._code < 0:
            raise IllegalArgumentException("code must be >= 0")
