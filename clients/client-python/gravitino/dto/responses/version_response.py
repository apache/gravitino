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

from gravitino.dto.responses.base_response import BaseResponse
from gravitino.dto.version_dto import VersionDTO


@dataclass
class VersionResponse(BaseResponse):
    """Represents a response containing version of Gravitino."""

    _version: VersionDTO = field(metadata=config(field_name="version"))

    def version(self) -> VersionDTO:
        return self._version

    def validate(self):
        """Validates the response data.

        Raise:
            IllegalArgumentException if name or audit information is not set.
        """
        super().validate()

        assert self._version is not None, "version must be non-null"
        assert (
            self._version.version() is not None
        ), "version 'version' must not be null and empty"
        assert (
            self._version.compile_date() is not None
        ), "version 'compile_date' must not be null and empty"
        assert (
            self._version.git_commit() is not None
        ), "version 'git_commit' must not be null and empty"
