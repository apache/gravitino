"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from typing import Dict
from dataclasses import dataclass, field

from dataclasses_json import config

from .audit_dto import AuditDTO
from ..api.catalog import Catalog


@dataclass
class CatalogDTO(Catalog):
    """Data transfer object representing catalog information."""

    _name: str = field(metadata=config(field_name="name"))
    _type: Catalog.Type = field(metadata=config(field_name="type"))
    _provider: str = field(metadata=config(field_name="provider"))
    _comment: str = field(metadata=config(field_name="comment"))
    _properties: Dict[str, str] = field(metadata=config(field_name="properties"))
    _audit: AuditDTO = field(default=None, metadata=config(field_name="audit"))

    def builder(
        self,
        name: str = None,
        type: Catalog.Type = Catalog.Type.UNSUPPORTED,
        provider: str = None,
        comment: str = None,
        properties: Dict[str, str] = None,
        audit: AuditDTO = None,
    ):
        self._name = name
        self._type = type
        self._provider = provider
        self._comment = comment
        self._properties = properties
        self._audit = audit

    def name(self) -> str:
        return self._name

    def type(self) -> Catalog.Type:
        return self._type

    def provider(self) -> str:
        return self._provider

    def comment(self) -> str:
        return self._comment

    def properties(self) -> Dict[str, str]:
        return self._properties

    def audit_info(self) -> AuditDTO:
        return self._audit
