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
from typing import List, Optional

from dataclasses_json import config, dataclass_json

from gravitino.api.authorization.privileges import Privilege
from gravitino.api.authorization.securable_objects import SecurableObject
from gravitino.api.metadata_object import MetadataObject
from gravitino.dto.authorization.privilege_dto import PrivilegeDTO


@dataclass_json
@dataclass
class SecurableObjectDTO(SecurableObject):
    """Data transfer object representing a securable object."""

    _full_name: str = field(metadata=config(field_name="fullName"))
    _type: MetadataObject.Type = field(metadata=config(field_name="type"))
    _privileges: List[PrivilegeDTO] = field(
        default_factory=list, metadata=config(field_name="privileges")
    )

    def __post_init__(self):
        # Split fullName into parent and name (matching Java's setFullName logic)
        if self._full_name and "." in self._full_name:
            index = self._full_name.rfind(".")
            self._parent = self._full_name[:index]
            self._name = self._full_name[index + 1 :]
        else:
            self._parent = None
            self._name = self._full_name if self._full_name else ""

    def parent(self) -> Optional[str]:
        return self._parent

    def name(self) -> str:
        return self._name

    def full_name(self) -> str:
        return self._full_name

    def type(self) -> MetadataObject.Type:
        return self._type

    def privileges(self) -> List[Privilege]:
        return list(self._privileges) if self._privileges else []

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SecurableObject):
            return False
        return self._full_name == other.full_name() and self._type == other.type()

    def __hash__(self) -> int:
        return hash((self._full_name, self._type))

    @staticmethod
    def builder() -> SecurableObjectDTO.Builder:
        return SecurableObjectDTO.Builder()

    class Builder:
        """Helper class to build a SecurableObjectDTO object."""

        def __init__(self) -> None:
            self._full_name: str = None
            self._type: MetadataObject.Type = None
            self._privileges: List[PrivilegeDTO] = []

        def with_full_name(self, full_name: str) -> SecurableObjectDTO.Builder:
            self._full_name = full_name
            return self

        def with_type(self, type_: MetadataObject.Type) -> SecurableObjectDTO.Builder:
            self._type = type_
            return self

        def with_privileges(
            self, privileges: List[PrivilegeDTO]
        ) -> SecurableObjectDTO.Builder:
            self._privileges = privileges
            return self

        def build(self) -> SecurableObjectDTO:
            if not self._full_name:
                raise ValueError("fullName cannot be null or empty")
            if self._type is None:
                raise ValueError("type cannot be None")
            return SecurableObjectDTO(self._full_name, self._type, self._privileges)
