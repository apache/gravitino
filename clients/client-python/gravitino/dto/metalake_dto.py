"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from dataclasses import dataclass
from typing import Optional, Dict

from dataclasses_json import DataClassJsonMixin

from gravitino.dto.audit_dto import AuditDTO


@dataclass
class MetalakeDTO(DataClassJsonMixin):
    """Represents a Metalake Data Transfer Object (DTO) that implements the Metalake interface."""

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

    def equals(self, other):
        if self == other:
            return True
        if not isinstance(other, MetalakeDTO):
            return False
        return self.name == other.name and self.comment == other.comment and \
            self.property_equal(self.properties, other.properties) and self.audit == other.audit

    def property_equal(self, p1, p2):
        if p1 is None and p2 is None:
            return True
        if p1 is not None and not p1 and p2 is None:
            return True
        if p2 is not None and not p2 and p1 is None:
            return True
        return p1 == p2
