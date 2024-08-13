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

from gravitino.dto.fileset_dto import FilesetDTO
from gravitino.dto.responses.base_response import BaseResponse
from gravitino.exceptions.base import IllegalArgumentException


@dataclass
class FilesetResponse(BaseResponse):
    """Response for fileset creation."""

    _fileset: FilesetDTO = field(metadata=config(field_name="fileset"))

    def fileset(self) -> FilesetDTO:
        return self._fileset

    def validate(self):
        """Validates the response data.

        Raises:
            IllegalArgumentException if catalog identifiers are not set.
        """
        super().validate()
        if self._fileset is None:
            raise IllegalArgumentException("fileset must not be null")
        if not self._fileset.name():
            raise IllegalArgumentException("fileset 'name' must not be null and empty")
        if not self._fileset.storage_location():
            raise IllegalArgumentException(
                "fileset 'storageLocation' must not be null and empty"
            )
        if self._fileset.type() is None:
            raise IllegalArgumentException("fileset 'type' must not be null and empty")
