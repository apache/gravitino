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

import re
from dataclasses import dataclass
from enum import Enum

from gravitino.dto.version_dto import VersionDTO
from gravitino.exceptions.base import BadRequestException, GravitinoRuntimeException


class Version(Enum):
    MAJOR = "MAJOR"
    MINOR = "MINOR"
    PATCH = "PATCH"


VERSION_PATTERN: str = (
    rf"(?P<{Version.MAJOR.value}>\d+)\.(?P<{Version.MINOR.value}>\d+)\.(?P<{Version.PATCH.value}>\d+)+?"
)


@dataclass
class GravitinoVersion(VersionDTO):
    """Gravitino version information."""

    major: int
    minor: int
    patch: int

    def __init__(self, versionDTO):
        super().__init__(
            versionDTO.version(), versionDTO.compile_date(), versionDTO.git_commit()
        )

        m = re.match(VERSION_PATTERN, self.version())

        if m is None:
            raise BadRequestException(f"Invalid version string: {self.version()}")

        self.major = int(m.group(Version.MAJOR.value))
        self.minor = int(m.group(Version.MINOR.value))
        self.patch = int(m.group(Version.PATCH.value))

    def __gt__(self, other) -> bool:
        if not isinstance(other, GravitinoVersion):
            raise GravitinoRuntimeException(
                f"{GravitinoVersion.__name__} can't compare with {other.__class__.__name__}"
            )
        if self.major > other.major:
            return True
        if self.minor > other.minor:
            return True
        if self.patch > other.patch:
            return True
        return False

    def __eq__(self, other) -> bool:
        if not isinstance(other, GravitinoVersion):
            raise GravitinoRuntimeException(
                f"{GravitinoVersion.__name__} can't compare with {other.__class__.__name__}"
            )
        if self.major != other.major:
            return False
        if self.minor != other.minor:
            return False
        if self.patch != other.patch:
            return False
        return True
