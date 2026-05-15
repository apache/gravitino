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

from gravitino.api.authorization.privileges import Privilege
from gravitino.api.authorization.securable_objects import SecurableObject
from gravitino.api.metadata_object import MetadataObject
from gravitino.dto.authorization.privilege_dto import PrivilegeDTO
from gravitino.utils.precondition import Precondition


@dataclass_json
@dataclass
class SecurableObjectDTO(SecurableObject):
    _full_name: str = field(metadata=config(field_name="fullName"))
    _type: MetadataObject.Type = field(metadata=config(field_name="type"))
    _privileges: list[PrivilegeDTO] = field(metadata=config(field_name="privileges"))

    _parent: Optional[str] = field(
        default=None,
        init=False,
        repr=False,
        metadata=config(exclude=lambda _: True),
    )
    _name: str = field(
        default="",
        init=False,
        repr=False,
        metadata=config(exclude=lambda _: True),
    )

    def __post_init__(self) -> None:
        self.validate()
        self.set_full_name(self._full_name)

    def validate(self) -> None:
        Precondition.check_string_not_empty(
            self._full_name, "full name cannot be null or empty"
        )
        Precondition.check_argument(self._type is not None, "type cannot be null")
        Precondition.check_argument(
            self._privileges is not None and len(self._privileges) != 0,
            "privileges can't be null or empty",
        )

    def full_name(self) -> str:
        return self._full_name

    def set_full_name(self, full_name: str) -> None:
        self._full_name = full_name
        index = full_name.rfind(".")
        self._parent, self._name = (
            (None, full_name)
            if index == -1
            else (full_name[:index], full_name[index + 1 :])
        )

    def parent(self) -> str | None:
        return self._parent

    def name(self) -> str:
        return self._name

    def type(self) -> MetadataObject.Type:
        return self._type

    def privileges(self) -> list[Privilege]:
        return list(self._privileges)
