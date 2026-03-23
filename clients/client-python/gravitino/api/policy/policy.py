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

import typing as tp
from abc import ABC, abstractmethod

from gravitino.api.auditable import Auditable
from gravitino.api.metadata_object import MetadataObject
from gravitino.api.policy.policy_content import PolicyContent
from gravitino.exceptions.base import UnsupportedOperationException


class Policy(Auditable, ABC):
    """
    The interface of the policy. The policy is a set of rules that
    can be associated with a metadata object. The policy can be used
    for data governance and so on.
    """

    # The prefix for built-in policy types. All built-in policy types should start with this prefix.
    BUILT_IN_TYPE_PREFIX = "system_"

    @abstractmethod
    def name(self) -> str:
        """
        Get the name of the policy.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            str: The name of the policy.
        """
        raise NotImplementedError()

    @abstractmethod
    def policy_type(self) -> str:
        """
        Get the type of the policy.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            str: The type of the policy.
        """
        raise NotImplementedError()

    @abstractmethod
    def comment(self) -> str:
        """
        Get the comment of the policy.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            str: The comment of the policy.
        """
        raise NotImplementedError()

    @abstractmethod
    def enabled(self) -> bool:
        """
        Get whether the policy is enabled or not.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            bool: True if the policy is enabled, false otherwise.
        """
        raise NotImplementedError()

    @abstractmethod
    def content(self) -> PolicyContent:
        """
        Get the content of the policy.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            PolicyContent: The content of the policy.
        """
        raise NotImplementedError()

    @abstractmethod
    def inherited(self) -> tp.Optional[bool]:
        """
        Check if the policy is inherited from a parent object or not.
        Note: The return value is optional, Only when the policy is associated
        with a metadata object, and called from the metadata object,
        the return value will be present. Otherwise, it will be empty.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            Optional[bool]: True if the policy is inherited,
            false if it is owned by the object itself.
            Empty if the policy is not associated with any object.
        """
        raise NotImplementedError()

    @abstractmethod
    def associated_objects(self) -> AssociatedObjects:
        raise UnsupportedOperationException(
            "The associatedObjects method is not supported."
        )


class AssociatedObjects(ABC):
    """
    The interface of the associated objects of the policy.
    """

    @abstractmethod
    def count(self) -> int:
        """
        _summary_

        Returns:
            int: _description_
        """
        objects = self.objects()
        return len(objects) if objects is not None else 0

    @abstractmethod
    def objects(self) -> list[MetadataObject]:
        """
        Get the list of metadata objects that are associated with this policy.

        Returns:
            list[MetadataObject]: The list of objects that are associated with this policy.
        """
        pass
