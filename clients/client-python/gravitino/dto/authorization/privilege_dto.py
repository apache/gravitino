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

from gravitino.api.authorization.privileges import Privilege
from gravitino.api.metadata_object import MetadataObject


def _encode_privilege_name(name: Privilege.Name) -> str:
    """Encode Privilege.Name enum to lowercase string for JSON serialization."""
    return name.name.lower()


def _decode_privilege_name(val: str) -> Privilege.Name:
    """Decode lowercase string back to Privilege.Name enum."""
    return Privilege.Name[val.upper()]


def _encode_condition(condition: Privilege.Condition) -> str:
    """Encode Privilege.Condition enum to lowercase string for JSON serialization."""
    return condition.value.lower()


def _decode_condition(val: str) -> Privilege.Condition:
    """Decode lowercase string back to Privilege.Condition enum."""
    return Privilege.Condition(val.upper())


@dataclass_json
@dataclass
class PrivilegeDTO(Privilege):
    """Data transfer object representing a privilege."""

    _name: Privilege.Name = field(
        metadata=config(
            field_name="name",
            encoder=_encode_privilege_name,
            decoder=_decode_privilege_name,
        )
    )
    _condition: Privilege.Condition = field(
        metadata=config(
            field_name="condition",
            encoder=_encode_condition,
            decoder=_decode_condition,
        )
    )

    def name(self) -> Privilege.Name:
        return self._name

    def simple_string(self) -> str:
        return f"{self._condition.value} {self._name.name.lower().replace('_', ' ')}"

    def condition(self) -> Privilege.Condition:
        return self._condition

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return True

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Privilege):
            return False
        return self._name == other.name() and self._condition == other.condition()

    def __hash__(self) -> int:
        return hash((self._name, self._condition))

    @staticmethod
    def builder() -> PrivilegeDTO.Builder:
        return PrivilegeDTO.Builder()

    class Builder:
        """Helper class to build a PrivilegeDTO object."""

        def __init__(self) -> None:
            self._name: Privilege.Name = None
            self._condition: Privilege.Condition = None

        def with_name(self, name: Privilege.Name) -> PrivilegeDTO.Builder:
            self._name = name
            return self

        def with_condition(
            self, condition: Privilege.Condition
        ) -> PrivilegeDTO.Builder:
            self._condition = condition
            return self

        def build(self) -> PrivilegeDTO:
            if self._name is None:
                raise ValueError("name cannot be None")
            if self._condition is None:
                raise ValueError("condition cannot be None")
            return PrivilegeDTO(self._name, self._condition)
