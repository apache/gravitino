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

from gravitino.api.authorization.group import Group
from gravitino.dto.audit_dto import AuditDTO


@dataclass_json
@dataclass
class GroupDTO(Group):
    """Represents a Group Data Transfer Object (DTO)."""

    _name: str = field(metadata=config(field_name="name"))
    _roles: list[str] = field(default_factory=list, metadata=config(field_name="roles"))
    _audit: Optional[AuditDTO] = field(
        default=None, metadata=config(field_name="audit")
    )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GroupDTO):
            return False
        return (
            self._name == other._name
            and self._roles == other._roles
            and self._audit == other._audit
        )

    def __hash__(self) -> int:
        return hash((self._name, tuple(self._roles), self._audit))

    @staticmethod
    def builder() -> GroupDTO.Builder:
        return GroupDTO.Builder()

    def name(self) -> str:
        return self._name

    def roles(self) -> list[str]:
        return self._roles if self._roles else []

    def audit_info(self) -> Optional[AuditDTO]:
        return self._audit

    class Builder:
        """Helper class to build a GroupDTO object."""

        def __init__(self) -> None:
            self._name: str = ""
            self._roles: list[str] = []
            self._audit: Optional[AuditDTO] = None

        def with_name(self, name: str) -> GroupDTO.Builder:
            self._name = name
            return self

        def with_roles(self, roles: list[str]) -> GroupDTO.Builder:
            if roles is not None:
                self._roles = roles
            return self

        def with_audit(self, audit: AuditDTO) -> GroupDTO.Builder:
            self._audit = audit
            return self

        def build(self) -> GroupDTO:
            if not self._name:
                raise ValueError("name cannot be null or empty")
            return GroupDTO(self._name, self._roles, self._audit)
