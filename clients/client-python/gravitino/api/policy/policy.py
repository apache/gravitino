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

from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional

from gravitino.api.auditable import Auditable
from gravitino.api.metadata_object import MetadataObject
from gravitino.api.policy.policy_content import PolicyContent
from gravitino.api.policy.policy_contents import PolicyContents
from gravitino.exceptions.base import (
    IllegalArgumentException,
    UnsupportedOperationException,
)
from gravitino.utils.precondition import Precondition


class Policy(Auditable):
    """The interface of the policy.

    The policy is a set of rules that can be associated with a metadata object.
    The policy can be used for data governance and so on.
    """

    BUILT_IN_TYPE_PREFIX: str = "system_"
    """The prefix for built-in policy types. All built-in policy types should start with this prefix."""

    @abstractmethod
    def name(self) -> str:
        """Get the name of the policy.

        Returns:
            The name of the policy.
        """

    @abstractmethod
    def policy_type(self) -> str:
        """Get the type of the policy.

        Returns:
            str: The type of the policy.
        """

    @abstractmethod
    def comment(self) -> str:
        """Get the comment of the policy.

        Returns:
            str: The comment of the policy.
        """

    @abstractmethod
    def enabled(self) -> bool:
        """Whether the policy is enabled or not.

        Returns:
            bool: `True` if the policy is enabled, False otherwise.
        """

    @abstractmethod
    def content(self) -> PolicyContent:
        """Get the content of the policy.

        Returns:
            PolicyContent: The content of the policy.
        """

    @abstractmethod
    def inherited(self) -> Optional[bool]:
        """Check if the policy is inherited from a parent object or not.

        **NOTE**: The return value is optional, Only when the policy is associated with a metadata
        object, and called from the metadata object, the return value will be present. Otherwise, it
        will be empty.

        Returns:
            Optional[bool]:
                `True` if the policy is inherited, false if it is owned by the object itself.
                the policy is not associated with any object.
        """

    def associated_objects(self) -> "Policy.AssociatedObjects":
        """Get the associated objects of the policy.

        Returns:
            Policy.AssociatedObjects: The associated objects of the policy.
        """
        raise UnsupportedOperationException(
            "The associatedObjects method is not supported."
        )

    class AssociatedObjects(ABC):
        """The interface of the associated objects of the policy.

        The associated objects are the objects that the policy is associated with.
        """

        def count(self) -> int:
            """Gets the number of objects that are associated with this policy

            Returns:
                int: The number of objects that are associated with this policy
            """
            objects = self.objects()
            return 0 if objects is None else len(objects)

        @abstractmethod
        def objects(self) -> list[MetadataObject]:
            """Gets the list of objects that are associated with this policy.

            Returns:
                list[MetadataObject]: The list of objects that are associated with this policy.
            """

    class BuiltInType(Enum):
        """The built-in policy types.

        Predefined policy types that are provided by the system.
        """

        # TODO: add built-in policies, such as:
        # DATA_COMPACTION(BUILT_IN_TYPE_PREFIX + "data_compaction",
        # PolicyContent.DataCompactionContent.class)

        CUSTOM = ("custom", PolicyContents.CustomContent)
        """Custom policy type. "custom" is a fixed string that indicates the policy is
        a non-built-in
        """

        def __init__(self, policy_type: str, content_class: type[PolicyContent]):
            self._policy_type = policy_type
            self._content_class = content_class

        def policy_type(self) -> str:
            """Get the policy type string.

            Returns:
                str: the policy type string.
            """
            return self._policy_type

        def content_class(self) -> type[PolicyContent]:
            """Get the content class of the policy.

            Returns:
                type[PolicyContent]: the content class of the policy.
            """
            return self._content_class

        @staticmethod
        def from_policy_type(policy_type: str) -> "Policy.BuiltInType":
            """Get the built-in policy type from the policy type string.

            Args:
                policy_type (str): the policy type string

            Raises:
                IllegalArgumentException:
                    if `policy_type` is not either a valid built-in policy type or a
                    custom policy type.

            Returns:
                Policy.BuiltInType:
                    the built-in policy type if it matches, otherwise returns `CUSTOM` type
            """
            Precondition.check_string_not_empty(
                policy_type, "policy_type cannot be blank"
            )
            for type_ in Policy.BuiltInType:
                if type_.policy_type() == policy_type:
                    return type_
            if policy_type.startswith(Policy.BUILT_IN_TYPE_PREFIX):
                raise IllegalArgumentException(
                    f"Unknown built-in policy type: {policy_type}"
                )
            raise IllegalArgumentException(
                f"Unknown policy type: {policy_type}, it should start with "
                f"'{Policy.BUILT_IN_TYPE_PREFIX}' or be 'custom'"
            )
