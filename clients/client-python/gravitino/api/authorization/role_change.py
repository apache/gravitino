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

from abc import ABC
from dataclasses import dataclass, field

from dataclasses_json import config

from gravitino.api.authorization.securable_objects import SecurableObject
from gravitino.utils.precondition import Precondition


class RoleChange(ABC):
    """The RoleChange interface defines the public API for managing roles in an authorization."""

    @staticmethod
    def add_securable_object(
        role_name: str,
        securable_object: SecurableObject,
    ) -> RoleChange.AddSecurableObject:
        """
        Create a RoleChange to add a securable object into a role.

        Args:
            role_name (str): The role name.
            securable_object (SecurableObject): The securable object.

        Returns:
            RoleChange.AddSecurableObject: return a RoleChange for the added securable object.
        """
        return RoleChange.AddSecurableObject(role_name, securable_object)

    @staticmethod
    def remove_securable_object(
        role_name: str,
        securable_object: SecurableObject,
    ) -> RoleChange.RemoveSecurableObject:
        """
        Create a RoleChange to remove a securable object from a role.

        Args:
            role_name (str): The role name.
            securable_object (SecurableObject): The securable object.

        Returns:
            RoleChange.RemoveSecurableObject: return a RoleChange for the added securable object.
        """
        return RoleChange.RemoveSecurableObject(role_name, securable_object)

    @staticmethod
    def update_securable_object(
        role_name: str,
        securable_object: SecurableObject,
        new_securable_object: SecurableObject,
    ) -> RoleChange.UpdateSecurableObject:
        """
        Update a securable object RoleChange.

        Args:
            role_name (str): The role name.
            securable_object (SecurableObject): The securable object.
            new_securable_object (SecurableObject): The new securable object.

        Returns:
            RoleChange.UpdateSecurableObject: return a RoleChange for the update-securable object.
        """
        return RoleChange.UpdateSecurableObject(
            role_name, securable_object, new_securable_object
        )

    @dataclass(frozen=True, eq=True)
    class AddSecurableObject:
        """A AddSecurableObject to add a securable object to a role."""

        _role_name: str = field(metadata=config(field_name="role_name"))
        _securable_object: SecurableObject = field(
            metadata=config(field_name="securable_object")
        )

        @property
        def role_name(self) -> str:
            """
            Returns the role name to be added.

            Returns:
                str: The role name to be added.
            """
            return self._role_name

        @property
        def securable_object(self) -> SecurableObject:
            """
            Returns the securable object to be added.

            Returns:
                SecurableObject: The securable object to be added.
            """
            return self._securable_object

        def __str__(self) -> str:
            """
            Returns a string representation of the AddSecurableObject instance. This string format
            includes the class name followed by the add securable object operation.

            Returns:
                str: A string representation of the AddSecurableObject instance.
            """
            return f"ADDSECURABLEOBJECT {self._role_name} + {self._securable_object}"

    @dataclass(frozen=True, eq=True)
    class RemoveSecurableObject:
        """A RemoveSecurableObject to remove a securable object from a role."""

        _role_name: str = field(metadata=config(field_name="role_name"))
        _securable_object: SecurableObject = field(
            metadata=config(field_name="securable_object")
        )

        @property
        def role_name(self) -> str:
            """
            Returns the role name to be removed.

            Returns:
                str: The role name to be removed.
            """
            return self._role_name

        @property
        def securable_object(self) -> SecurableObject:
            """
            Returns the securable object to be removed.

            Returns:
                SecurableObject: The securable object to be removed.
            """
            return self._securable_object

        def __str__(self) -> str:
            """
            Returns a string representation of the RemoveSecurableObject instance. This string format
            includes the class name followed by the add securable object operation.

            Returns:
                str: A string representation of the RemoveSecurableObject instance.
            """
            return f"REMOVESECURABLEOBJECT {self._role_name} + {self._securable_object}"

    @dataclass(frozen=True, eq=True)
    class UpdateSecurableObject:
        """A UpdateSecurableObject is to update securable object's privilege from a role.
        The securable object's metadata entity must be the same as new securable object's metadata
        entity.
        The securable object's privilege must be different from new securable object's privilege.
        """

        _role_name: str = field(metadata=config(field_name="role_name"))
        _securable_object: SecurableObject = field(
            metadata=config(field_name="securable_object")
        )
        _new_securable_object: SecurableObject = field(
            metadata=config(field_name="new_securable_object")
        )

        def __post_init__(self) -> None:
            Precondition.check_argument(
                self._securable_object.full_name()
                == self._new_securable_object.full_name(),
                "The securable object's metadata entity must be same as new securable object's metadata entity.",
            )

            Precondition.check_argument(
                self._securable_object.privileges()
                != self._new_securable_object.privileges(),
                "The securable object's privilege must be different as new securable object's privilege.",
            )

        @property
        def role_name(self) -> str:
            """
            Returns the role name to be updated.

            Returns:
                str: The role name to be updated.
            """
            return self._role_name

        @property
        def securable_object(self) -> SecurableObject:
            """
            Returns the securable object to be updated.

            Returns:
                SecurableObject: The securable object to be updated.
            """
            return self._securable_object

        @property
        def new_securable_object(self) -> SecurableObject:
            """
            Returns the new securable object.

            Returns:
                SecurableObject: return a securable object.
            """
            return self._new_securable_object

        def __str__(self) -> str:
            """
            Returns a string representation of the UpdateSecurableObject instance. This string format
            includes the class name followed by the add securable object operation._summary_

            Returns:
                str: A string representation of the RemoveSecurableObject instance.
            """
            return (
                f"UPDATESECURABLEOBJECT "
                f"{self._role_name} {self._securable_object} {self._new_securable_object}"
            )
