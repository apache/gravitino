"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import re
from dataclasses import dataclass
from enum import Enum

from gravitino.dto.version_dto import VersionDTO


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

        assert m is not None, "Invalid version string " + self.version()

        self.major = int(m.group(Version.MAJOR.value))
        self.minor = int(m.group(Version.MINOR.value))
        self.patch = int(m.group(Version.PATCH.value))

    def __gt__(self, other) -> bool:
        if self.major > other.major:
            return True
        if self.minor > other.minor:
            return True
        if self.patch > other.patch:
            return True
        return False

    def __eq__(self, other) -> bool:
        if self.major != other.major:
            return False
        if self.minor != other.minor:
            return False
        if self.patch != other.patch:
            return False
        return True
