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

from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING

from gravitino.api.metadata_object import MetadataObject

if TYPE_CHECKING:
    pass


class Privilege(ABC):
    """The interface of a privilege. The privilege represents the ability to execute
    kinds of operations  for kinds of entities
    """

    @abstractmethod
    def name(self) -> Privilege.Name:
        """
        Return the generic name of the privilege.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            Privilege.Name: The generic name of the privilege.
        """
        raise NotImplementedError()

    @abstractmethod
    def simple_string(self) -> str:
        """
        Return a simple string representation of the privilege.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            str: A readable string representation for the privilege.
        """
        raise NotImplementedError()

    @abstractmethod
    def condition(self) -> "Privilege.Condition":
        """
        Return the condition of the privilege.

        raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            Privilege.Condition: The condition of the privilege.
            `ALLOW` means that you are allowed to use the  privilege,
            `DENY` means that you are denied to use the privilege
        """
        raise NotImplementedError()

    @abstractmethod
    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        """
        Check whether this privilege can bind to a securable object type.

        Args:
            obj_type: The securable object's metadata type.

        Returns:
            True if this privilege can bind to the given type, otherwise False.
        """
        raise NotImplementedError()

    class Name(Enum):
        """The name of this privilege."""

        # The privilege to create a catalog.
        CREATE_CATALOG = (0, 1 << 0)
        # The privilege to use a catalog.
        USE_CATALOG = (0, 1 << 2)
        # The privilege to create a schema.
        CREATE_SCHEMA = (0, 1 << 3)
        # The privilege to use a schema.
        USE_SCHEMA = (0, 1 << 4)
        # The privilege to create a table.
        CREATE_TABLE = (0, 1 << 5)
        # The privilege to modify a table.
        MODIFY_TABLE = (0, 1 << 6)
        # The privilege to select data from a table.
        SELECT_TABLE = (0, 1 << 7)
        # The privilege to create a fileset.
        CREATE_FILESET = (0, 1 << 8)
        # The privilege to write a fileset.
        WRITE_FILESET = (0, 1 << 9)
        # The privilege to read a fileset.
        READ_FILESET = (0, 1 << 10)
        # The privilege to create a topic.
        CREATE_TOPIC = (0, 1 << 11)
        # The privilege to produce to a topic.
        PRODUCE_TOPIC = (0, 1 << 12)
        # The privilege to consume from a topic.
        CONSUME_TOPIC = (0, 1 << 13)
        # The privilege to manage users
        MANAGE_USERS = (0, 1 << 14)
        # The privilege to manage groups
        MANAGE_GROUPS = (0, 1 << 15)
        # The privilege to create a role
        CREATE_ROLE = (0, 1 << 16)
        # The privilege to grant or revoke a role for the user or the group.
        MANAGE_GRANTS = (0, 1 << 17)
        # The privilege to create a model
        CREATE_MODEL = (0, 1 << 18)
        # The privilege to create a model version
        CREATE_MODEL_VERSION = (0, 1 << 19)
        # The privilege to view the metadata of the model and download all the model versions
        USE_MODEL = (0, 1 << 20)
        # The privilege to create a tag
        CREATE_TAG = (0, 1 << 21)
        # The privilege to apply a tag
        APPLY_TAG = (0, 1 << 22)
        # The privilege to create a policy
        CREATE_POLICY = (0, 1 << 23)
        # The privilege to apply a policy
        APPLY_POLICY = (0, 1 << 24)
        # The privilege to register a job template
        REGISTER_JOB_TEMPLATE = (0, 1 << 25)
        # The privilege to use a job template
        USE_JOB_TEMPLATE = (0, 1 << 26)
        # The privilege to run a job
        RUN_JOB = (0, 1 << 27)

        def __init__(self, high_bits: int, low_bits: int) -> None:
            """
            Initialize the Name with the high and low bits.

            Args:
                high_bits (int): The high bits of the privilege.
                low_bits (int): The low bits of the privilege.
            """
            self._high_bits = high_bits
            self._low_bits = low_bits

        @property
        def low_bits(self) -> int:
            """
            Return the low bits of Name.

            Returns:
                int: The low bits of Name
            """

            return self._low_bits

        @property
        def high_bits(self) -> int:
            """
            Return the high bits of Name.

            Returns:
                int: The high bits of Name
            """
            return self._high_bits

    class Condition(Enum):
        """
        The condition of this privilege.

        ALLOW means that you are allowed to use the privilege.
        DENY means that you are denied to use the privilege.

        If you have ALLOW and DENY for the same privilege name of the same
        securable object, the DENY will take effect.
        """

        # Allow to use the privilege
        ALLOW = "ALLOW"
        # Deny to use the privilege
        DENY = "DENY"


class Privileges:
    # TODO Implement the Privileges class.
    pass
