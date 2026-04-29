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

from gravitino.dto.authorization.user_dto import UserDTO
from gravitino.dto.responses.base_response import BaseResponse
from gravitino.utils.precondition import Precondition


@dataclass_json
@dataclass
class UserResponse(BaseResponse):
    """Represents a response for a user."""

    _user: UserDTO = field(default=None, metadata=config(field_name="user"))

    def user(self) -> UserDTO:
        return self._user

    def validate(self) -> None:
        Precondition.check_argument(
            self._user is not None, "User response should have a user"
        )


@dataclass_json
@dataclass
class UserListResponse(BaseResponse):
    """Represents a response for a list of users with details."""

    _users: list[UserDTO] = field(
        default_factory=list, metadata=config(field_name="users")
    )

    def users(self) -> list[UserDTO]:
        return self._users

    def validate(self) -> None:
        Precondition.check_argument(
            self._users is not None, "User list response should have users"
        )


@dataclass_json
@dataclass
class UserNamesListResponse(BaseResponse):
    """Represents a response for a list of user names."""

    _names: list[str] = field(default_factory=list, metadata=config(field_name="names"))

    def names(self) -> list[str]:
        return self._names

    def validate(self) -> None:
        Precondition.check_argument(
            self._names is not None, "User names list response should have names"
        )
