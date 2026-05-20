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

from gravitino.api.authorization.privileges import Privilege, Privileges
from gravitino.api.metadata_object import MetadataObject
from gravitino.utils.precondition import Precondition


@dataclass_json
@dataclass
class PrivilegeDTO(Privilege):
    _name: Privilege.Name = field(
        metadata=config(
            field_name="name",
            encoder=lambda x: x.name,
            decoder=lambda x: Privilege.Name[x],
        )
    )
    _condition: Privilege.Condition = field(metadata=config(field_name="condition"))

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        Precondition.check_argument(self._name is not None, "name cannot be null")
        Precondition.check_argument(
            self._condition is not None, "condition cannot be null"
        )

    def name(self) -> Privilege.Name:
        return self._name

    def simple_string(self) -> str:
        return (
            Privileges.allow(self._name.name).simple_string()
            if self._condition == Privilege.Condition.ALLOW
            else Privileges.deny(self._name.name).simple_string()
        )

    def condition(self) -> Privilege.Condition:
        return self._condition

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return (
            Privileges.allow(self._name.name).can_bind_to(obj_type)
            if self._condition == Privilege.Condition.ALLOW
            else Privileges.deny(self._name.name).can_bind_to(obj_type)
        )
