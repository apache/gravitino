"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict

from dataclasses_json import config, DataClassJsonMixin

from gravitino.api.fileset import Fileset
from gravitino.dto.audit_dto import AuditDTO


@dataclass
class FilesetDTO(Fileset, DataClassJsonMixin):
    """Represents a Fileset DTO (Data Transfer Object)."""

    _name: str = field(metadata=config(field_name="name"))
    _comment: Optional[str] = field(metadata=config(field_name="comment"))
    _type: Fileset.Type = field(metadata=config(field_name="type"))
    _properties: Dict[str, str] = field(metadata=config(field_name="properties"))
    _storage_location: str = field(
        default=None, metadata=config(field_name="storageLocation")
    )
    _audit: AuditDTO = field(default=None, metadata=config(field_name="audit"))

    def name(self) -> str:
        return self._name

    def type(self) -> Fileset.Type:
        return self._type

    def storage_location(self) -> str:
        return self._storage_location

    def comment(self) -> Optional[str]:
        return self._comment

    def properties(self) -> Dict[str, str]:
        return self._properties

    def audit_info(self) -> AuditDTO:
        return self._audit
