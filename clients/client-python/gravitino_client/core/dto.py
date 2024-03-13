"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from dataclasses import dataclass

@dataclass
class VersionDTO:
    version: str
    compile_date: str
    git_commit: str
