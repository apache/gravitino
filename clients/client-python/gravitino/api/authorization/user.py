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

from abc import abstractmethod

from gravitino.api.auditable import Auditable


class User(Auditable):
    """The interface of a user. The user is the entity which executes every operation."""

    @abstractmethod
    def name(self) -> str:
        """
        The name of the user. It's the identifier of User. It must be unique.
        Usually the name comes from an external user management system like LDAP,
        IAM and so on.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            str: The name of the user.
        """
        raise NotImplementedError("Not implemented")

    @abstractmethod
    def roles(self) -> list[str]:
        """
        The roles of the user. A user can have multiple roles. Every role binds several privileges.

        Raises:
            NotImplementedError: If not implemented

        Returns:
            list[str]: The role names of the user.
        """
        raise NotImplementedError("Not implemented")
