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

from dataclasses_json import config, dataclass_json

from gravitino.api.audit import Audit
from gravitino.api.authorization.group import Group
from gravitino.dto.audit_dto import AuditDTO
from gravitino.utils.precondition import Precondition


@dataclass_json
@dataclass
class GroupDTO(Group):
    """
    Represents a Group Data Transfer Object (DTO).
    """

    _name: str = field(metadata=config(field_name="name"))
    _audit: AuditDTO = field(metadata=config(field_name="audit"))
    _roles: list[str] = field(default_factory=list, metadata=config(field_name="roles"))

    def __post_init__(self) -> None:
        if self._roles is None:
            self._roles = []
        self.validate()

    def validate(self) -> None:
        Precondition.check_string_not_empty(self._name, "name cannot be null or empty")
        Precondition.check_argument(self._audit is not None, "audit cannot be null")

    def name(self) -> str:
        """
        Retrieve the name of the Group DTO.

        Returns:
            str: The name of the Group DTO.
        """
        return self._name

    def audit_info(self) -> Audit:
        """
        Retrieve the audit information of the Group DTO.

        Returns:
            Audit: The audit information of the Group DTO.
        """
        return self._audit

    def roles(self) -> list[str]:
        """
        The roles of the group. A group can have multiple roles. Every role binds several privileges.

        Returns:
            list[str]: The roles of the group.
        """
        return self._roles
