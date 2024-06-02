"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from dataclasses import dataclass, field
from dataclasses_json import config


@dataclass
class VersionDTO:
    """Represents a Version Data Transfer Object (DTO)."""

    _version: str = field(metadata=config(field_name="version"))
    """The version of the software."""

    _compile_date: str = field(metadata=config(field_name="compileDate"))
    """The date the software was compiled."""

    _git_commit: str = field(metadata=config(field_name="gitCommit"))
    """The git commit of the software."""

    def version(self) -> str:
        return self._version

    def compile_date(self) -> str:
        return self._compile_date

    def git_commit(self) -> str:
        return self._git_commit
