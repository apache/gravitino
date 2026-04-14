# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import config, dataclass_json

from gravitino.api.authorization.role import Role
from gravitino.api.authorization.securable_objects import SecurableObject
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.authorization.securable_object_dto import SecurableObjectDTO


@dataclass_json
@dataclass
class RoleDTO(Role):
    """Represents a Role Data Transfer Object (DTO)."""

    _name: str = field(metadata=config(field_name="name"))
    _properties: Optional[dict[str, str]] = field(
        default=None, metadata=config(field_name="properties")
    )
    _securable_objects: list[SecurableObjectDTO] = field(
        default_factory=list, metadata=config(field_name="securableObjects")
    )
    _audit: Optional[AuditDTO] = field(
        default=None, metadata=config(field_name="audit")
    )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RoleDTO):
            return False
        return (
            self._name == other._name
            and self._properties == other._properties
            and self._audit == other._audit
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._name,
                frozenset(self._properties.items()) if self._properties else None,
                self._audit,
            )
        )

    @staticmethod
    def builder() -> RoleDTO.Builder:
        return RoleDTO.Builder()

    def name(self) -> str:
        return self._name

    def properties(self) -> Optional[dict[str, str]]:
        return self._properties

    def securable_objects(self) -> list[SecurableObject]:
        return list(self._securable_objects) if self._securable_objects else []

    def audit_info(self) -> Optional[AuditDTO]:
        return self._audit

    class Builder:
        """Helper class to build a RoleDTO object."""

        def __init__(self) -> None:
            self._name: str = ""
            self._properties: Optional[dict[str, str]] = None
            self._securable_objects: list[SecurableObjectDTO] = []
            self._audit: Optional[AuditDTO] = None

        def with_name(self, name: str) -> RoleDTO.Builder:
            self._name = name
            return self

        def with_properties(self, properties: dict[str, str]) -> RoleDTO.Builder:
            if properties is not None:
                self._properties = properties
            return self

        def with_securable_objects(
            self, securable_objects: list[SecurableObjectDTO]
        ) -> RoleDTO.Builder:
            self._securable_objects = securable_objects
            return self

        def with_audit(self, audit: AuditDTO) -> RoleDTO.Builder:
            self._audit = audit
            return self

        def build(self) -> RoleDTO:
            if not self._name:
                raise ValueError("name cannot be null or empty")
            return RoleDTO(
                self._name,
                self._properties,
                self._securable_objects,
                self._audit,
            )
