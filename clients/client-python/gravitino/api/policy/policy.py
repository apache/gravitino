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
from enum import Enum

from gravitino.api.auditable import Auditable
from gravitino.api.policy.policy_content import (
    AccessControlContent,
    ColumnMaskContent,
    IcebergDataCompactionContent,
    IcebergOrphanFileRemovalContent,
    IcebergRewriteManifestsContent,
    IcebergSnapshotExpirationContent,
    PolicyContent,
    RowFilterContent,
)
from gravitino.exceptions.base import IllegalArgumentException


class Policy(Auditable):
    """A policy is a set of rules that can be associated with a set of
    metadata objects, used for data governance such as data access control
    and data retention.

    A policy is uniquely identified by its name within a metalake.
    """

    BUILT_IN_TYPE_PREFIX = "system_"

    @abstractmethod
    def name(self) -> str:
        """The name of the policy.

        Returns:
            str: The name of the policy.
        """
        raise NotImplementedError()

    @abstractmethod
    def policy_type(self) -> str:
        """The type of the policy, for example ``system_access_control`` for
        built-in policies, or ``custom`` for user-defined policies. Built-in
        policy types are defined in :class:`Policy.BuiltInType`.

        Returns:
            str: The type of the policy.
        """
        raise NotImplementedError()

    @abstractmethod
    def comment(self) -> str | None:
        """The comment of the policy.

        Returns:
            Optional[str]: The comment of the policy, None if not set.
        """
        raise NotImplementedError()

    @abstractmethod
    def enabled(self) -> bool:
        """Whether the policy is enabled.

        Returns:
            bool: True if the policy is enabled, False otherwise.
        """
        raise NotImplementedError()

    @abstractmethod
    def content(self) -> PolicyContent:
        """The content of the policy: the rules plus the object types and
        properties that describe them. The concrete class of the content is
        determined by the policy type.

        Returns:
            PolicyContent: The content of the policy.
        """
        raise NotImplementedError()

    @abstractmethod
    def inherited(self) -> bool | None:
        """Whether this policy is inherited from a parent metadata object,
        when the policy is retrieved in the context of a metadata object.

        Returns:
            Optional[bool]: True if the policy is inherited, False if it is
                directly associated, None if the inheritance information is
                not applicable or not available.
        """
        raise NotImplementedError()

    class BuiltInType(Enum):
        """The built-in policy types, each paired with the content class that
        carries its rules."""

        ICEBERG_COMPACTION = (
            "system_iceberg_compaction",
            IcebergDataCompactionContent,
        )
        ICEBERG_ORPHAN_FILE_REMOVAL = (
            "system_iceberg_orphan_file_removal",
            IcebergOrphanFileRemovalContent,
        )
        ICEBERG_REWRITE_MANIFESTS = (
            "system_iceberg_rewrite_manifests",
            IcebergRewriteManifestsContent,
        )
        ICEBERG_SNAPSHOT_EXPIRATION = (
            "system_iceberg_snapshot_expiration",
            IcebergSnapshotExpirationContent,
        )
        ROW_FILTER = ("system_row_filter", RowFilterContent)
        COLUMN_MASK = ("system_column_mask", ColumnMaskContent)
        ACCESS_CONTROL = ("system_access_control", AccessControlContent)
        # The content class of CUSTOM is PolicyContents.CustomContent; it is
        # resolved lazily to avoid a circular import.
        CUSTOM = ("custom", None)

        def __init__(self, policy_type: str, content_class: type | None):
            self._policy_type = policy_type
            self._content_class = content_class

        def policy_type(self) -> str:
            """Returns the wire policy type string."""
            return self._policy_type

        def content_class(self) -> type[PolicyContent]:
            """Returns the content class associated with this type."""
            if self._content_class is not None:
                return self._content_class
            # Late import: policy_contents imports this module's Policy only
            # for typing, but CustomContent lives there.
            from gravitino.api.policy.policy_contents import (  # pylint: disable=import-outside-toplevel
                PolicyContents,
            )

            return PolicyContents.CustomContent

        @staticmethod
        def from_policy_type(policy_type: str) -> Policy.BuiltInType:
            """Resolves a policy type string to a built-in type.

            Args:
                policy_type: The policy type string.

            Returns:
                Policy.BuiltInType: The matching built-in type.

            Raises:
                IllegalArgumentException: If the policy type is null, is a
                    ``system_``-prefixed type that is not a known built-in
                    type, or cannot be mapped to a built-in type.
            """
            if policy_type is None:
                raise IllegalArgumentException("Policy type cannot be null")
            for built_in_type in Policy.BuiltInType:
                if built_in_type.policy_type().lower() == policy_type.lower():
                    return built_in_type
            if policy_type.lower().startswith(Policy.BUILT_IN_TYPE_PREFIX):
                raise IllegalArgumentException(
                    f"Unknown built-in policy type: {policy_type}"
                )
            raise IllegalArgumentException(
                f"Can't map policy type: {policy_type} to any built-in policy type"
            )
