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

from gravitino.dto.authorization.group_dto import GroupDTO
from gravitino.dto.responses.base_response import BaseResponse
from gravitino.utils.precondition import Precondition


@dataclass_json
@dataclass
class GroupResponse(BaseResponse):
    """Represents a response for a group."""

    _group: GroupDTO = field(default=None, metadata=config(field_name="group"))

    def group(self) -> GroupDTO:
        return self._group

    def validate(self) -> None:
        Precondition.check_argument(
            self._group is not None, "Group response should have a group"
        )


@dataclass_json
@dataclass
class GroupListResponse(BaseResponse):
    """Represents a response for a list of groups with details."""

    _groups: list[GroupDTO] = field(
        default_factory=list, metadata=config(field_name="groups")
    )

    def groups(self) -> list[GroupDTO]:
        return self._groups

    def validate(self) -> None:
        Precondition.check_argument(
            self._groups is not None, "Group list response should have groups"
        )


@dataclass_json
@dataclass
class GroupNamesListResponse(BaseResponse):
    """Represents a response for a list of group names."""

    _names: list[str] = field(default_factory=list, metadata=config(field_name="names"))

    def names(self) -> list[str]:
        return self._names

    def validate(self) -> None:
        Precondition.check_argument(
            self._names is not None, "Group names list response should have names"
        )
