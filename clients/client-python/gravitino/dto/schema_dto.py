"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from dataclasses import dataclass
from typing import Optional, Dict

from dataclasses_json import DataClassJsonMixin

from gravitino.dto.audit_dto import AuditDTO


@dataclass
class SchemaDTO(DataClassJsonMixin):
    """Represents a Schema DTO (Data Transfer Object)."""

    name: str
    """The name of the Metalake DTO."""

    comment: Optional[str]
    """The comment of the Metalake DTO."""

    properties: Optional[Dict[str, str]] = None
    """The properties of the Metalake DTO."""

    audit: AuditDTO = None
    """The audit information of the Metalake DTO."""

    def __init__(self, name: str = None, comment: str = None, properties: Dict[str, str] = None,
                 audit: AuditDTO = None):
        self.name = name
        self.comment = comment
        self.properties = properties
        self.audit = audit
