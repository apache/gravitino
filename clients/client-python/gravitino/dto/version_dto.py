"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from dataclasses import dataclass


@dataclass
class VersionDTO:
    """Represents a Version Data Transfer Object (DTO)."""

    version: str = ""
    """The version of the software."""

    compile_date: str = ""
    """The date the software was compiled."""

    git_commit: str = ""
    """The git commit of the software."""
