# pylint: disable=too-many-lines
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
from typing import TYPE_CHECKING, Optional

from gravitino.api.metadata_object import MetadataObject
from gravitino.exceptions.base import IllegalArgumentException

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

        CREATE_CATALOG = (0, 1 << 0)
        """The privilege to create a catalog."""

        USE_CATALOG = (0, 1 << 2)
        """The privilege to use a catalog."""

        CREATE_SCHEMA = (0, 1 << 3)
        """The privilege to create a schema."""

        USE_SCHEMA = (0, 1 << 4)
        """The privilege to use a schema."""

        CREATE_TABLE = (0, 1 << 5)
        """The privilege to create a table."""

        MODIFY_TABLE = (0, 1 << 6)
        """The privilege to modify a table."""

        SELECT_TABLE = (0, 1 << 7)
        """The privilege to select data from a table."""

        CREATE_FILESET = (0, 1 << 8)
        """The privilege to create a fileset."""

        WRITE_FILESET = (0, 1 << 9)
        """The privilege to write a fileset."""

        READ_FILESET = (0, 1 << 10)
        """The privilege to read a fileset."""

        CREATE_TOPIC = (0, 1 << 11)
        """The privilege to create a topic."""

        PRODUCE_TOPIC = (0, 1 << 12)
        """The privilege to produce to a topic."""

        CONSUME_TOPIC = (0, 1 << 13)
        """The privilege to consume from a topic."""

        MANAGE_USERS = (0, 1 << 14)
        """The privilege to manage users."""

        MANAGE_GROUPS = (0, 1 << 15)
        """The privilege to manage groups."""

        CREATE_ROLE = (0, 1 << 16)
        """The privilege to create a role."""

        MANAGE_GRANTS = (0, 1 << 17)
        """The privilege to grant or revoke a role for the user or the group."""

        REGISTER_MODEL = (0, 1 << 18)
        """The privilege to create a model."""

        CREATE_MODEL = REGISTER_MODEL
        """Deprecated. Please use REGISTER_MODEL."""

        LINK_MODEL_VERSION = (0, 1 << 19)
        """The privilege to create a model version."""

        CREATE_MODEL_VERSION = LINK_MODEL_VERSION
        """Deprecated. Please use LINK_MODEL_VERSION."""

        USE_MODEL = (0, 1 << 20)
        """The privilege to view model metadata and download all model versions."""

        CREATE_TAG = (0, 1 << 21)
        """The privilege to create a tag."""

        APPLY_TAG = (0, 1 << 22)
        """The privilege to apply a tag."""

        CREATE_POLICY = (0, 1 << 23)
        """The privilege to create a policy."""

        APPLY_POLICY = (0, 1 << 24)
        """The privilege to apply a policy."""

        REGISTER_JOB_TEMPLATE = (0, 1 << 25)
        """The privilege to register a job template."""

        USE_JOB_TEMPLATE = (0, 1 << 26)
        """The privilege to use a job template."""

        RUN_JOB = (0, 1 << 27)
        """The privilege to run a job."""

        CREATE_VIEW = (0, 1 << 28)
        """The privilege to create a view."""

        SELECT_VIEW = (0, 1 << 29)
        """The privilege to select data from a view."""

        REGISTER_FUNCTION = (0, 1 << 30)
        """The privilege to register a function."""

        EXECUTE_FUNCTION = (0, 1 << 31)
        """The privilege to execute (invoke) a function."""

        MODIFY_FUNCTION = (0, 1 << 32)
        """The privilege to alter a function's metadata."""

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


class GenericPrivilege(Privilege):
    """Abstract class representing a generic privilege."""

    def __init__(
        self,
        condition: Privilege.Condition,
        name: Privilege.Name,
    ) -> None:
        self._condition = condition
        self._name = name

    def name(self) -> Privilege.Name:
        return self._name

    def condition(self) -> Privilege.Condition:
        return self._condition

    def simple_string(self) -> str:
        return f"{self._condition.name} {self._name.name.lower().replace('_', ' ')}"

    def __hash__(self) -> int:
        return hash((self._condition, self._name))

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, GenericPrivilege):
            return False
        return self._condition == value._condition and self._name == value._name


class CreateCatalog(GenericPrivilege):
    """The privilege to create a catalog."""

    _ALLOW_INSTANCE: Optional[CreateCatalog] = None
    _DENY_INSTANCE: Optional[CreateCatalog] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The privilege with allow condition.
        """
        if CreateCatalog._ALLOW_INSTANCE is None:
            CreateCatalog._ALLOW_INSTANCE = CreateCatalog(
                Privilege.Condition.ALLOW, Privilege.Name.CREATE_CATALOG
            )
        return CreateCatalog._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.
        """
        if CreateCatalog._DENY_INSTANCE is None:
            CreateCatalog._DENY_INSTANCE = CreateCatalog(
                Privilege.Condition.DENY, Privilege.Name.CREATE_CATALOG
            )
        return CreateCatalog._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type == MetadataObject.Type.METALAKE


class UseCatalog(GenericPrivilege):
    """The privilege to use a catalog."""

    _ALLOW_INSTANCE: Optional[UseCatalog] = None
    _DENY_INSTANCE: Optional[UseCatalog] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.
        """
        if UseCatalog._ALLOW_INSTANCE is None:
            UseCatalog._ALLOW_INSTANCE = UseCatalog(
                Privilege.Condition.ALLOW, Privilege.Name.USE_CATALOG
            )

        return UseCatalog._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.
        """
        if UseCatalog._DENY_INSTANCE is None:
            UseCatalog._DENY_INSTANCE = UseCatalog(
                Privilege.Condition.DENY, Privilege.Name.USE_CATALOG
            )

        return UseCatalog._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type in [MetadataObject.Type.METALAKE, MetadataObject.Type.CATALOG]


class UseSchema(GenericPrivilege):
    """The privilege to use a schema."""

    _ALLOW_INSTANCE: Optional[UseSchema] = None
    _DENY_INSTANCE: Optional[UseSchema] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.
        """
        if UseSchema._ALLOW_INSTANCE is None:
            UseSchema._ALLOW_INSTANCE = UseSchema(
                Privilege.Condition.ALLOW, Privilege.Name.USE_SCHEMA
            )

        return UseSchema._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.
        """
        if UseSchema._DENY_INSTANCE is None:
            UseSchema._DENY_INSTANCE = UseSchema(
                Privilege.Condition.DENY, Privilege.Name.USE_SCHEMA
            )

        return UseSchema._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type in Privileges.SCHEMA_SUPPORTED_TYPES


class CreateSchema(GenericPrivilege):
    """Privilege to create a schema."""

    _ALLOW_INSTANCE: Optional[CreateSchema] = None
    _DENY_INSTANCE: Optional[CreateSchema] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.
        """
        if CreateSchema._ALLOW_INSTANCE is None:
            CreateSchema._ALLOW_INSTANCE = CreateSchema(
                Privilege.Condition.ALLOW, Privilege.Name.CREATE_SCHEMA
            )

        return CreateSchema._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.
        """
        if CreateSchema._DENY_INSTANCE is None:
            CreateSchema._DENY_INSTANCE = CreateSchema(
                Privilege.Condition.DENY, Privilege.Name.CREATE_SCHEMA
            )

        return CreateSchema._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type in [MetadataObject.Type.METALAKE, MetadataObject.Type.CATALOG]


class CreateTable(GenericPrivilege):
    """The privilege to create a table."""

    _ALLOW_INSTANCE: Optional[CreateTable] = None
    _DENY_INSTANCE: Optional[CreateTable] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.
        """
        if CreateTable._ALLOW_INSTANCE is None:
            CreateTable._ALLOW_INSTANCE = CreateTable(
                Privilege.Condition.ALLOW, Privilege.Name.CREATE_TABLE
            )

        return CreateTable._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.
        """
        if CreateTable._DENY_INSTANCE is None:
            CreateTable._DENY_INSTANCE = CreateTable(
                Privilege.Condition.DENY, Privilege.Name.CREATE_TABLE
            )

        return CreateTable._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type in Privileges.SCHEMA_SUPPORTED_TYPES


class SelectTable(GenericPrivilege):
    """
    Privilege to select table.
    """

    _ALLOW_INSTANCE: Optional[SelectTable] = None
    _DENY_INSTANCE: Optional[SelectTable] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.
        """
        if SelectTable._ALLOW_INSTANCE is None:
            SelectTable._ALLOW_INSTANCE = SelectTable(
                Privilege.Condition.ALLOW, Privilege.Name.SELECT_TABLE
            )

        return SelectTable._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.
        """
        if SelectTable._DENY_INSTANCE is None:
            SelectTable._DENY_INSTANCE = SelectTable(
                Privilege.Condition.DENY, Privilege.Name.SELECT_TABLE
            )

        return SelectTable._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type in Privileges.TABLE_SUPPORTED_TYPES


class ModifyTable(GenericPrivilege):
    """The privilege to write data to a table or modify the table schema."""

    _ALLOW_INSTANCE: Optional[ModifyTable] = None
    _DENY_INSTANCE: Optional[ModifyTable] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.
        """
        if ModifyTable._ALLOW_INSTANCE is None:
            ModifyTable._ALLOW_INSTANCE = ModifyTable(
                Privilege.Condition.ALLOW, Privilege.Name.MODIFY_TABLE
            )

        return ModifyTable._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.
        """
        if ModifyTable._DENY_INSTANCE is None:
            ModifyTable._DENY_INSTANCE = ModifyTable(
                Privilege.Condition.DENY, Privilege.Name.MODIFY_TABLE
            )

        return ModifyTable._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type in Privileges.TABLE_SUPPORTED_TYPES


class CreateFileset(GenericPrivilege):
    """The privilege to create a fileset."""

    _ALLOW_INSTANCE: Optional[CreateFileset] = None
    _DENY_INSTANCE: Optional[CreateFileset] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.
        """
        if CreateFileset._ALLOW_INSTANCE is None:
            CreateFileset._ALLOW_INSTANCE = CreateFileset(
                Privilege.Condition.ALLOW, Privilege.Name.CREATE_FILESET
            )

        return CreateFileset._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.
        """
        if CreateFileset._DENY_INSTANCE is None:
            CreateFileset._DENY_INSTANCE = CreateFileset(
                Privilege.Condition.DENY, Privilege.Name.CREATE_FILESET
            )

        return CreateFileset._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type in Privileges.SCHEMA_SUPPORTED_TYPES


class ReadFileset(GenericPrivilege):
    """
    Privilege to read fileset.
    """

    _ALLOW_INSTANCE: Optional[ReadFileset] = None
    _DENY_INSTANCE: Optional[ReadFileset] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.
        """
        if ReadFileset._ALLOW_INSTANCE is None:
            ReadFileset._ALLOW_INSTANCE = ReadFileset(
                Privilege.Condition.ALLOW, Privilege.Name.READ_FILESET
            )

        return ReadFileset._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.
        """
        if ReadFileset._DENY_INSTANCE is None:
            ReadFileset._DENY_INSTANCE = ReadFileset(
                Privilege.Condition.DENY, Privilege.Name.READ_FILESET
            )

        return ReadFileset._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type in Privileges.FILESET_SUPPORTED_TYPES


class WriteFileset(GenericPrivilege):
    """
    Privilege to write fileset.
    """

    _ALLOW_INSTANCE: Optional[WriteFileset] = None
    _DENY_INSTANCE: Optional[WriteFileset] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.
        """
        if WriteFileset._ALLOW_INSTANCE is None:
            WriteFileset._ALLOW_INSTANCE = WriteFileset(
                Privilege.Condition.ALLOW, Privilege.Name.WRITE_FILESET
            )

        return WriteFileset._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.

        """
        if WriteFileset._DENY_INSTANCE is None:
            WriteFileset._DENY_INSTANCE = WriteFileset(
                Privilege.Condition.DENY, Privilege.Name.WRITE_FILESET
            )

        return WriteFileset._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type in Privileges.FILESET_SUPPORTED_TYPES


class CreateTopic(GenericPrivilege):
    """The privilege to create a topic."""

    _ALLOW_INSTANCE: Optional[CreateTopic] = None
    _DENY_INSTANCE: Optional[CreateTopic] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.

        """
        if CreateTopic._ALLOW_INSTANCE is None:
            CreateTopic._ALLOW_INSTANCE = CreateTopic(
                Privilege.Condition.ALLOW, Privilege.Name.CREATE_TOPIC
            )

        return CreateTopic._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.

        """
        if CreateTopic._DENY_INSTANCE is None:
            CreateTopic._DENY_INSTANCE = CreateTopic(
                Privilege.Condition.DENY, Privilege.Name.CREATE_TOPIC
            )

        return CreateTopic._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type in Privileges.SCHEMA_SUPPORTED_TYPES


class ConsumeTopic(GenericPrivilege):
    """
    Privilege for consuming a topic.
    """

    _ALLOW_INSTANCE: Optional[ConsumeTopic] = None
    _DENY_INSTANCE: Optional[ConsumeTopic] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.

        """
        if ConsumeTopic._ALLOW_INSTANCE is None:
            ConsumeTopic._ALLOW_INSTANCE = ConsumeTopic(
                Privilege.Condition.ALLOW, Privilege.Name.CONSUME_TOPIC
            )

        return ConsumeTopic._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.

        """
        if ConsumeTopic._DENY_INSTANCE is None:
            ConsumeTopic._DENY_INSTANCE = ConsumeTopic(
                Privilege.Condition.DENY, Privilege.Name.CONSUME_TOPIC
            )

        return ConsumeTopic._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type in Privileges.TOPIC_SUPPORTED_TYPES


class ProduceTopic(GenericPrivilege):
    """The privilege to produce to a topic."""

    _ALLOW_INSTANCE: Optional[ProduceTopic] = None
    _DENY_INSTANCE: Optional[ProduceTopic] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.

        """
        if ProduceTopic._ALLOW_INSTANCE is None:
            ProduceTopic._ALLOW_INSTANCE = ProduceTopic(
                Privilege.Condition.ALLOW, Privilege.Name.PRODUCE_TOPIC
            )

        return ProduceTopic._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.

        """
        if ProduceTopic._DENY_INSTANCE is None:
            ProduceTopic._DENY_INSTANCE = ProduceTopic(
                Privilege.Condition.DENY, Privilege.Name.PRODUCE_TOPIC
            )

        return ProduceTopic._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type in Privileges.TOPIC_SUPPORTED_TYPES


class ManageUsers(GenericPrivilege):
    """The privilege to manage users."""

    _ALLOW_INSTANCE: Optional[ManageUsers] = None
    _DENY_INSTANCE: Optional[ManageUsers] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.

        """
        if ManageUsers._ALLOW_INSTANCE is None:
            ManageUsers._ALLOW_INSTANCE = ManageUsers(
                Privilege.Condition.ALLOW, Privilege.Name.MANAGE_USERS
            )
        return ManageUsers._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.

        """
        if ManageUsers._DENY_INSTANCE is None:
            ManageUsers._DENY_INSTANCE = ManageUsers(
                Privilege.Condition.DENY, Privilege.Name.MANAGE_USERS
            )
        return ManageUsers._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type == MetadataObject.Type.METALAKE


class ManageGroups(GenericPrivilege):
    """The privilege to manage groups."""

    _ALLOW_INSTANCE: Optional[ManageGroups] = None
    _DENY_INSTANCE: Optional[ManageGroups] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.

        """
        if ManageGroups._ALLOW_INSTANCE is None:
            ManageGroups._ALLOW_INSTANCE = ManageGroups(
                Privilege.Condition.ALLOW, Privilege.Name.MANAGE_GROUPS
            )

        return ManageGroups._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.

        """
        if ManageGroups._DENY_INSTANCE is None:
            ManageGroups._DENY_INSTANCE = ManageGroups(
                Privilege.Condition.DENY, Privilege.Name.MANAGE_GROUPS
            )

        return ManageGroups._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type == MetadataObject.Type.METALAKE


class CreateRole(GenericPrivilege):
    """The privilege to create a role."""

    _ALLOW_INSTANCE: Optional[CreateRole] = None
    _DENY_INSTANCE: Optional[CreateRole] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.

        """
        if CreateRole._ALLOW_INSTANCE is None:
            CreateRole._ALLOW_INSTANCE = CreateRole(
                Privilege.Condition.ALLOW, Privilege.Name.CREATE_ROLE
            )

        return CreateRole._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.

        """
        if CreateRole._DENY_INSTANCE is None:
            CreateRole._DENY_INSTANCE = CreateRole(
                Privilege.Condition.DENY, Privilege.Name.CREATE_ROLE
            )

        return CreateRole._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type == MetadataObject.Type.METALAKE


class ManageGrants(GenericPrivilege):
    """
    The privilege to grant or revoke privileges on securable objects. If bound on the metalake,
    we can grant or revoke the role for users or groups. Unlike most privileges,
    this can be bound at any level of the object hierarchy — METALAKE,
    CATALOG, SCHEMA, TABLE, VIEW, TOPIC, FILESET, FUNCTION, or MODEL.
    A grant at a parent level implicitly covers all descendants within it.
    """

    _ALLOW_INSTANCE: Optional[ManageGrants] = None
    _DENY_INSTANCE: Optional[ManageGrants] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.

        """
        if ManageGrants._ALLOW_INSTANCE is None:
            ManageGrants._ALLOW_INSTANCE = ManageGrants(
                Privilege.Condition.ALLOW, Privilege.Name.MANAGE_GRANTS
            )

        return ManageGrants._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.

        """
        if ManageGrants._DENY_INSTANCE is None:
            ManageGrants._DENY_INSTANCE = ManageGrants(
                Privilege.Condition.DENY, Privilege.Name.MANAGE_GRANTS
            )

        return ManageGrants._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type in Privileges.MANAGE_GRANTS_SUPPORTED_TYPES


class RegisterModel(GenericPrivilege):
    """The privilege to register a model"""

    _ALLOW_INSTANCE: Optional[RegisterModel] = None
    _DENY_INSTANCE: Optional[RegisterModel] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.

        """
        if RegisterModel._ALLOW_INSTANCE is None:
            RegisterModel._ALLOW_INSTANCE = RegisterModel(
                Privilege.Condition.ALLOW, Privilege.Name.REGISTER_MODEL
            )

        return RegisterModel._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.

        """
        if RegisterModel._DENY_INSTANCE is None:
            RegisterModel._DENY_INSTANCE = RegisterModel(
                Privilege.Condition.DENY, Privilege.Name.REGISTER_MODEL
            )

        return RegisterModel._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type in Privileges.SCHEMA_SUPPORTED_TYPES


class UseModel(GenericPrivilege):
    """The privilege to view the metadata of the model and download all the model versions"""

    _ALLOW_INSTANCE: Optional[UseModel] = None
    _DENY_INSTANCE: Optional[UseModel] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.

        """
        if UseModel._ALLOW_INSTANCE is None:
            UseModel._ALLOW_INSTANCE = UseModel(
                Privilege.Condition.ALLOW, Privilege.Name.USE_MODEL
            )

        return UseModel._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.
        Returns:
            Privilege: The instance with deny condition of the privilege.

        """
        if UseModel._DENY_INSTANCE is None:
            UseModel._DENY_INSTANCE = UseModel(
                Privilege.Condition.DENY, Privilege.Name.USE_MODEL
            )

        return UseModel._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type in Privileges.MODEL_SUPPORTED_TYPES


class LinkModelVersion(GenericPrivilege):
    """The privilege to link a model version"""

    _ALLOW_INSTANCE: Optional[LinkModelVersion] = None
    _DENY_INSTANCE: Optional[LinkModelVersion] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.

        """
        if LinkModelVersion._ALLOW_INSTANCE is None:
            LinkModelVersion._ALLOW_INSTANCE = LinkModelVersion(
                Privilege.Condition.ALLOW, Privilege.Name.LINK_MODEL_VERSION
            )

        return LinkModelVersion._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.

        """
        if LinkModelVersion._DENY_INSTANCE is None:
            LinkModelVersion._DENY_INSTANCE = LinkModelVersion(
                Privilege.Condition.DENY, Privilege.Name.LINK_MODEL_VERSION
            )

        return LinkModelVersion._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type in Privileges.MODEL_SUPPORTED_TYPES


class CreateTag(GenericPrivilege):
    """The privilege to create a tag"""

    _ALLOW_INSTANCE: Optional[CreateTag] = None
    _DENY_INSTANCE: Optional[CreateTag] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.

        """
        if CreateTag._ALLOW_INSTANCE is None:
            CreateTag._ALLOW_INSTANCE = CreateTag(
                Privilege.Condition.ALLOW, Privilege.Name.CREATE_TAG
            )

        return CreateTag._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.

        """
        if CreateTag._DENY_INSTANCE is None:
            CreateTag._DENY_INSTANCE = CreateTag(
                Privilege.Condition.DENY, Privilege.Name.CREATE_TAG
            )

        return CreateTag._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type == MetadataObject.Type.METALAKE


class ApplyTag(GenericPrivilege):
    """The privilege to apply tag to object."""

    _ALLOW_INSTANCE: Optional[ApplyTag] = None
    _DENY_INSTANCE: Optional[ApplyTag] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.

        """
        if ApplyTag._ALLOW_INSTANCE is None:
            ApplyTag._ALLOW_INSTANCE = ApplyTag(
                Privilege.Condition.ALLOW, Privilege.Name.APPLY_TAG
            )
        return ApplyTag._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.

        """
        if ApplyTag._DENY_INSTANCE is None:
            ApplyTag._DENY_INSTANCE = ApplyTag(
                Privilege.Condition.DENY, Privilege.Name.APPLY_TAG
            )
        return ApplyTag._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type in [MetadataObject.Type.METALAKE, MetadataObject.Type.TAG]


class CreatePolicy(GenericPrivilege):
    """The privilege to create a policy"""

    _ALLOW_INSTANCE: Optional[CreatePolicy] = None
    _DENY_INSTANCE: Optional[CreatePolicy] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.

        """
        if CreatePolicy._ALLOW_INSTANCE is None:
            CreatePolicy._ALLOW_INSTANCE = CreatePolicy(
                Privilege.Condition.ALLOW, Privilege.Name.CREATE_POLICY
            )

        return CreatePolicy._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.

        """
        if CreatePolicy._DENY_INSTANCE is None:
            CreatePolicy._DENY_INSTANCE = CreatePolicy(
                Privilege.Condition.DENY, Privilege.Name.CREATE_POLICY
            )

        return CreatePolicy._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type == MetadataObject.Type.METALAKE


class RunJob(GenericPrivilege):
    """The privilege to run a job."""

    _ALLOW_INSTANCE: Optional[RunJob] = None
    _DENY_INSTANCE: Optional[RunJob] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.

        """
        if RunJob._ALLOW_INSTANCE is None:
            RunJob._ALLOW_INSTANCE = RunJob(
                Privilege.Condition.ALLOW, Privilege.Name.RUN_JOB
            )

        return RunJob._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.

        """
        if RunJob._DENY_INSTANCE is None:
            RunJob._DENY_INSTANCE = RunJob(
                Privilege.Condition.DENY, Privilege.Name.RUN_JOB
            )
        return RunJob._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type == MetadataObject.Type.METALAKE


class ApplyPolicy(GenericPrivilege):
    """The privilege to apply policy to object."""

    _ALLOW_INSTANCE: Optional[ApplyPolicy] = None
    _DENY_INSTANCE: Optional[ApplyPolicy] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.

        """
        if ApplyPolicy._ALLOW_INSTANCE is None:
            ApplyPolicy._ALLOW_INSTANCE = ApplyPolicy(
                Privilege.Condition.ALLOW, Privilege.Name.APPLY_POLICY
            )

        return ApplyPolicy._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.

        """
        if ApplyPolicy._DENY_INSTANCE is None:
            ApplyPolicy._DENY_INSTANCE = ApplyPolicy(
                Privilege.Condition.DENY, Privilege.Name.APPLY_POLICY
            )
        return ApplyPolicy._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type in [MetadataObject.Type.METALAKE, MetadataObject.Type.POLICY]


class RegisterJobTemplate(GenericPrivilege):
    """The privilege to register a job template"""

    _ALLOW_INSTANCE: Optional[RegisterJobTemplate] = None
    _DENY_INSTANCE: Optional[RegisterJobTemplate] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.

        """
        if RegisterJobTemplate._ALLOW_INSTANCE is None:
            RegisterJobTemplate._ALLOW_INSTANCE = RegisterJobTemplate(
                Privilege.Condition.ALLOW, Privilege.Name.REGISTER_JOB_TEMPLATE
            )

        return RegisterJobTemplate._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.

        """
        if RegisterJobTemplate._DENY_INSTANCE is None:
            RegisterJobTemplate._DENY_INSTANCE = RegisterJobTemplate(
                Privilege.Condition.DENY, Privilege.Name.REGISTER_JOB_TEMPLATE
            )

        return RegisterJobTemplate._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type == MetadataObject.Type.METALAKE


class UseJobTemplate(GenericPrivilege):
    """The privilege to use a job template."""

    _ALLOW_INSTANCE: Optional[UseJobTemplate] = None
    _DENY_INSTANCE: Optional[UseJobTemplate] = None

    @staticmethod
    def allow() -> Privilege:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.

        """
        if UseJobTemplate._ALLOW_INSTANCE is None:
            UseJobTemplate._ALLOW_INSTANCE = UseJobTemplate(
                Privilege.Condition.ALLOW, Privilege.Name.USE_JOB_TEMPLATE
            )

        return UseJobTemplate._ALLOW_INSTANCE

    @staticmethod
    def deny() -> Privilege:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.

        """
        if UseJobTemplate._DENY_INSTANCE is None:
            UseJobTemplate._DENY_INSTANCE = UseJobTemplate(
                Privilege.Condition.DENY, Privilege.Name.USE_JOB_TEMPLATE
            )

        return UseJobTemplate._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type in [
            MetadataObject.Type.METALAKE,
            MetadataObject.Type.JOB_TEMPLATE,
        ]


class CreateView(GenericPrivilege):
    """The privilege to create a view."""

    _ALLOW_INSTANCE: Optional[CreateView] = None
    _DENY_INSTANCE: Optional[CreateView] = None

    @staticmethod
    def allow() -> CreateView:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.
        """
        if CreateView._ALLOW_INSTANCE is None:
            CreateView._ALLOW_INSTANCE = CreateView(
                Privilege.Condition.ALLOW, Privilege.Name.CREATE_VIEW
            )
        return CreateView._ALLOW_INSTANCE

    @staticmethod
    def deny() -> CreateView:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.
        """
        if CreateView._DENY_INSTANCE is None:
            CreateView._DENY_INSTANCE = CreateView(
                Privilege.Condition.DENY, Privilege.Name.CREATE_VIEW
            )
        return CreateView._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type in Privileges.SCHEMA_SUPPORTED_TYPES


class SelectView(GenericPrivilege):
    """The privilege to select data from a view."""

    _ALLOW_INSTANCE: Optional[SelectView] = None
    _DENY_INSTANCE: Optional[SelectView] = None

    @staticmethod
    def allow() -> SelectView:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.
        """
        if SelectView._ALLOW_INSTANCE is None:
            SelectView._ALLOW_INSTANCE = SelectView(
                Privilege.Condition.ALLOW, Privilege.Name.SELECT_VIEW
            )
        return SelectView._ALLOW_INSTANCE

    @staticmethod
    def deny() -> SelectView:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.
        """
        if SelectView._DENY_INSTANCE is None:
            SelectView._DENY_INSTANCE = SelectView(
                Privilege.Condition.DENY, Privilege.Name.SELECT_VIEW
            )
        return SelectView._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type in Privileges.VIEW_SUPPORTED_TYPES


class RegisterFunction(GenericPrivilege):
    """The privilege to register a function."""

    _ALLOW_INSTANCE: Optional[RegisterFunction] = None
    _DENY_INSTANCE: Optional[RegisterFunction] = None

    @staticmethod
    def allow() -> RegisterFunction:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.
        """
        if RegisterFunction._ALLOW_INSTANCE is None:
            RegisterFunction._ALLOW_INSTANCE = RegisterFunction(
                Privilege.Condition.ALLOW, Privilege.Name.REGISTER_FUNCTION
            )

        return RegisterFunction._ALLOW_INSTANCE

    @staticmethod
    def deny() -> RegisterFunction:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.
        """
        if RegisterFunction._DENY_INSTANCE is None:
            RegisterFunction._DENY_INSTANCE = RegisterFunction(
                Privilege.Condition.DENY, Privilege.Name.REGISTER_FUNCTION
            )
        return RegisterFunction._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type in Privileges.SCHEMA_SUPPORTED_TYPES


class ExecuteFunction(GenericPrivilege):
    """The privilege to execute (invoke) a function and view its metadata."""

    _ALLOW_INSTANCE: Optional[ExecuteFunction] = None
    _DENY_INSTANCE: Optional[ExecuteFunction] = None

    @staticmethod
    def allow() -> ExecuteFunction:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.
        """
        if ExecuteFunction._ALLOW_INSTANCE is None:
            ExecuteFunction._ALLOW_INSTANCE = ExecuteFunction(
                Privilege.Condition.ALLOW, Privilege.Name.EXECUTE_FUNCTION
            )
        return ExecuteFunction._ALLOW_INSTANCE

    @staticmethod
    def deny() -> ExecuteFunction:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.
        """
        if ExecuteFunction._DENY_INSTANCE is None:
            ExecuteFunction._DENY_INSTANCE = ExecuteFunction(
                Privilege.Condition.DENY, Privilege.Name.EXECUTE_FUNCTION
            )
        return ExecuteFunction._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type in Privileges.FUNCTION_SUPPORTED_TYPES


class ModifyFunction(GenericPrivilege):
    """
    The privilege to alter a function's metadata.
    """

    _ALLOW_INSTANCE: Optional[ModifyFunction] = None
    _DENY_INSTANCE: Optional[ModifyFunction] = None

    @staticmethod
    def allow() -> ModifyFunction:
        """
        Retrieve the instance with allow condition of the privilege.

        Returns:
            Privilege: The instance with allow condition of the privilege.
        """
        if ModifyFunction._ALLOW_INSTANCE is None:
            ModifyFunction._ALLOW_INSTANCE = ModifyFunction(
                Privilege.Condition.ALLOW, Privilege.Name.MODIFY_FUNCTION
            )
        return ModifyFunction._ALLOW_INSTANCE

    @staticmethod
    def deny() -> ModifyFunction:
        """
        Retrieve the instance with deny condition of the privilege.

        Returns:
            Privilege: The instance with deny condition of the privilege.
        """
        if ModifyFunction._DENY_INSTANCE is None:
            ModifyFunction._DENY_INSTANCE = ModifyFunction(
                Privilege.Condition.DENY, Privilege.Name.MODIFY_FUNCTION
            )
        return ModifyFunction._DENY_INSTANCE

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        return obj_type in Privileges.FUNCTION_SUPPORTED_TYPES


class Privileges:
    _PRIVILEGE_TYPES = {
        Privilege.Name.CREATE_CATALOG: CreateCatalog,
        Privilege.Name.USE_CATALOG: UseCatalog,
        Privilege.Name.CREATE_SCHEMA: CreateSchema,
        Privilege.Name.USE_SCHEMA: UseSchema,
        Privilege.Name.CREATE_TABLE: CreateTable,
        Privilege.Name.MODIFY_TABLE: ModifyTable,
        Privilege.Name.SELECT_TABLE: SelectTable,
        Privilege.Name.CREATE_FILESET: CreateFileset,
        Privilege.Name.WRITE_FILESET: WriteFileset,
        Privilege.Name.READ_FILESET: ReadFileset,
        Privilege.Name.CREATE_TOPIC: CreateTopic,
        Privilege.Name.PRODUCE_TOPIC: ProduceTopic,
        Privilege.Name.CONSUME_TOPIC: ConsumeTopic,
        Privilege.Name.MANAGE_USERS: ManageUsers,
        Privilege.Name.MANAGE_GROUPS: ManageGroups,
        Privilege.Name.CREATE_ROLE: CreateRole,
        Privilege.Name.MANAGE_GRANTS: ManageGrants,
        Privilege.Name.REGISTER_MODEL: RegisterModel,
        Privilege.Name.LINK_MODEL_VERSION: LinkModelVersion,
        Privilege.Name.USE_MODEL: UseModel,
        Privilege.Name.CREATE_TAG: CreateTag,
        Privilege.Name.APPLY_TAG: ApplyTag,
        Privilege.Name.CREATE_POLICY: CreatePolicy,
        Privilege.Name.APPLY_POLICY: ApplyPolicy,
        Privilege.Name.REGISTER_JOB_TEMPLATE: RegisterJobTemplate,
        Privilege.Name.USE_JOB_TEMPLATE: UseJobTemplate,
        Privilege.Name.RUN_JOB: RunJob,
        Privilege.Name.CREATE_VIEW: CreateView,
        Privilege.Name.SELECT_VIEW: SelectView,
        Privilege.Name.REGISTER_FUNCTION: RegisterFunction,
        Privilege.Name.EXECUTE_FUNCTION: ExecuteFunction,
        Privilege.Name.MODIFY_FUNCTION: ModifyFunction,
    }

    TABLE_SUPPORTED_TYPES = {
        MetadataObject.Type.METALAKE,
        MetadataObject.Type.CATALOG,
        MetadataObject.Type.SCHEMA,
        MetadataObject.Type.TABLE,
    }
    MODEL_SUPPORTED_TYPES = {
        MetadataObject.Type.METALAKE,
        MetadataObject.Type.CATALOG,
        MetadataObject.Type.SCHEMA,
        MetadataObject.Type.MODEL,
    }
    TOPIC_SUPPORTED_TYPES = {
        MetadataObject.Type.METALAKE,
        MetadataObject.Type.CATALOG,
        MetadataObject.Type.SCHEMA,
        MetadataObject.Type.TOPIC,
    }
    FILESET_SUPPORTED_TYPES = {
        MetadataObject.Type.METALAKE,
        MetadataObject.Type.CATALOG,
        MetadataObject.Type.SCHEMA,
        MetadataObject.Type.FILESET,
    }
    VIEW_SUPPORTED_TYPES = {
        MetadataObject.Type.METALAKE,
        MetadataObject.Type.CATALOG,
        MetadataObject.Type.SCHEMA,
        MetadataObject.Type.VIEW,
    }
    FUNCTION_SUPPORTED_TYPES = {
        MetadataObject.Type.METALAKE,
        MetadataObject.Type.CATALOG,
        MetadataObject.Type.SCHEMA,
        MetadataObject.Type.FUNCTION,
    }
    MANAGE_GRANTS_SUPPORTED_TYPES = {
        MetadataObject.Type.METALAKE,
        MetadataObject.Type.CATALOG,
        MetadataObject.Type.SCHEMA,
        MetadataObject.Type.TABLE,
        MetadataObject.Type.TOPIC,
        MetadataObject.Type.FILESET,
        MetadataObject.Type.MODEL,
        MetadataObject.Type.VIEW,
        MetadataObject.Type.FUNCTION,
    }
    SCHEMA_SUPPORTED_TYPES = {
        MetadataObject.Type.METALAKE,
        MetadataObject.Type.CATALOG,
        MetadataObject.Type.SCHEMA,
    }

    @staticmethod
    def allow(privilege: str) -> Privilege:
        return Privileges._privilege_type(privilege).allow()

    @staticmethod
    def deny(privilege: str) -> Privilege:
        return Privileges._privilege_type(privilege).deny()

    @staticmethod
    def _privilege_type(privilege: str):
        try:
            name = Privilege.Name[privilege]
            return Privileges._PRIVILEGE_TYPES[name]
        except KeyError as exc:
            raise IllegalArgumentException(
                f"Doesn't support the privilege: {privilege}"
            ) from exc
