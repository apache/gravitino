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
from gravitino.api.authorization.securable_objects import SecurableObject


class Role(Auditable):
    """The interface of a role. The role is the entity which has kinds of privileges. One role can have
    multiple privileges of multiple securable objects.
    """

    @abstractmethod
    def name(self) -> str:
        """
        The name of the role.

        Raises:
            NotImplementedError: if the method is not implemented.

        Returns:
            str: The name of the role.
        """
        raise NotImplementedError()

    @abstractmethod
    def properties(self) -> dict[str, str]:
        """
        The properties of the role. Note, this method will return None if the properties are not set.

        Raises:
            NotImplementedError: if the method is not implemented.

        Returns:
            dict[str, str]: The properties of the role.
        """
        raise NotImplementedError()

    @abstractmethod
    def securable_objects(self) -> list[SecurableObject]:
        """
        The securable object represents a special kind of entity with a unique identifier. All
        securable objects are organized by tree structure. For example: If the securable object is a
        table, the identifier may be `catalog1.schema1.table1`._summary_

        Raises:
            NotImplementedError: if the method is not implemented.

        Returns:
            list[SecurableObject]: The securable objects of the role.
        """
        raise NotImplementedError()
