"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

@dataclass
class VersionDTO:
    version: str
    compile_date: str
    git_commit: str


class AuditDTO:
    creator: str
    create_time: datetime
    last_modifier: str
    last_modified_time: datetime

class Type(Enum):
     MANAGED = 'managed'
     EXTERNAL = 'external'

class FilesetDTO:
    name: str
    comment: str
    type: Type
    storage_location: str
    properties: dict
    audit: AuditDTO

