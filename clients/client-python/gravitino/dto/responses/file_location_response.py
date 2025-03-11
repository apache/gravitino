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
class FileLocationResponse(BaseResponse):
    """Response for the actual file location."""

    _file_location: str = field(metadata=config(field_name="fileLocation"))

    def file_location(self) -> str:
        return self._file_location

    def validate(self):
        """Validates the response data.

        Raises:
            IllegalArgumentException if file location is not set.
        """
        super().validate()
        if self._file_location is None or len(self.file_location()) == 0:
            raise IllegalArgumentException("file location must not be null")
