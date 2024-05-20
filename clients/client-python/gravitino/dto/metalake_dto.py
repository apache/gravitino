"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.audit import Audit
from gravitino.dto.audit_dto import AuditDTO
from gravitino.api.metalake import Metalake


@dataclass
class MetalakeDTO(Metalake, DataClassJsonMixin):
    """Represents a Metalake Data Transfer Object (DTO) that implements the Metalake interface."""

    _name: str = field(metadata=config(field_name="name"))
    """The name of the Metalake DTO."""

    _comment: Optional[str] = field(metadata=config(field_name="comment"))
    """The comment of the Metalake DTO."""

    _properties: Optional[Dict[str, str]] = field(
        metadata=config(field_name="properties")
    )
    """The properties of the Metalake DTO."""

    _audit: Optional[AuditDTO] = field(metadata=config(field_name="audit"))
    """The audit information of the Metalake DTO."""

    def name(self) -> str:
        return self._name

    def comment(self) -> str:
        return self._comment

    def properties(self) -> Dict[str, str]:
        return self._properties

    def audit_info(self) -> Audit:
        return self._audit

    def equals(self, other):
        if self == other:
            return True
        if not isinstance(other, MetalakeDTO):
            return False
        return (
            self._name == other._name
            and self._comment == other._comment
            and self.property_equal(self._properties, other._properties)
            and self._audit == other._audit
        )

    def property_equal(self, p1, p2):
        if p1 is None and p2 is None:
            return True
        if p1 is not None and not p1 and p2 is None:
            return True
        if p2 is not None and not p2 and p1 is None:
            return True
        return p1 == p2
