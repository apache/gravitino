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
from gravitino.api.authorization.role import Role
from gravitino.api.authorization.securable_objects import SecurableObject
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.authorization.securable_object_dto import SecurableObjectDTO
from gravitino.utils.precondition import Precondition


@dataclass_json
@dataclass
class RoleDTO(Role):
    """
    Represents a Role Data Transfer Object (DTO).
    """

    _name: str = field(metadata=config(field_name="name"))
    _audit: AuditDTO = field(metadata=config(field_name="audit"))
    _properties: dict[str, str] = field(metadata=config(field_name="properties"))
    _securable_objects: list[SecurableObjectDTO] = field(
        metadata=config(field_name="securableObjects")
    )

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        Precondition.check_string_not_empty(self._name, "name cannot be null or empty")
        Precondition.check_argument(self._audit is not None, "audit cannot be null")
        Precondition.check_argument(
            self._securable_objects is not None, "securable objects can't null"
        )

    def name(self) -> str:
        """
        Retrieve the name of the role

        Returns:
            str: The name of the Role DTO.
        """
        return self._name

    def audit_info(self) -> Audit:
        """
        Retrieve the audit information of the role

        Returns:
            Audit: The audit information of the Role DTO.
        """
        return self._audit

    def properties(self) -> dict[str, str]:
        """
        Retrieve the properties of the role

        Returns:
            dict[str, str]: The properties of the role. Note,
            this method will return null if the properties are not set.
        """
        return self._properties

    def securable_objects(self) -> list[SecurableObject]:
        """
        The resource represents a special kind of entity with a unique identifier.
        All resources are organized by tree structure. For example: If the resource is a table,
        the identifier may be `catalog1.schema1.table1`.

        Returns:
            list[SecurableObject]: The securable object of the role.
        """
        return self._securable_objects
