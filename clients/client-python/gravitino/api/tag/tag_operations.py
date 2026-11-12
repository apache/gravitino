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
from typing import Optional

from gravitino.api.policy.policy_association_selector import PolicyAssociationSelector
from gravitino.api.policy.policy_tag_association import PolicyTagAssociation
from gravitino.api.tag.tag import Tag
from gravitino.api.tag.tag_change import TagChange
from gravitino.exceptions.base import UnsupportedOperationException


class TagOperations(ABC):
    """
    Interface for supporting global tag operations. This interface will provide tag listing, getting,
    creating, and other tag operations under a metalake. This interface will be mixed with
    GravitinoMetalake or GravitinoClient to provide tag operations.
    """

    @abstractmethod
    def list_tags(self) -> list[str]:
        """List all the tag names under a metalake.

        Returns:
            list[str]: The list of tag names.

        Raises:
            NoSuchMetalakeException: If the metalake does not exist.
        """
        pass

    @abstractmethod
    def list_tags_info(self) -> list[Tag]:
        """
        List tags information under a metalake.

        Returns:
            list[Tag]: The list of tag information.

        Raises:
            NoSuchMetalakeException: If the metalake does not exist.
        """
        pass

    @abstractmethod
    def get_tag(self, tag_name: str) -> Tag:
        """
        Get a tag by its name under a metalake.

        Args:
            tag_name (str): The name of the tag.

        Returns:
            Tag: The tag information.

        Raises:
            NoSuchTagException: If the tag does not exist.
        """
        pass

    @abstractmethod
    def create_tag(
        self,
        tag_name: str,
        comment: str,
        properties: dict[str, str],
        allowed_values: list[str] | None = None,
    ) -> Tag:
        """
        Create a new tag under a metalake.

        Raises:
            NoSuchMetalakeException: If the metalake does not exist.
            TagAlreadyExistsException: If the tag already exists.

        Args:
            tag_name (str): The name of the tag.
            comment (str): The comment of the tag.
            properties (dict[str, str]): The properties of the tag.
            allowed_values (list[str] | None): The allowed assignment values.

        Returns:
            Tag: The tag information.
        """
        pass

    @abstractmethod
    def alter_tag(
        self,
        tag_name: str,
        *changes: TagChange,
    ) -> Tag:
        """
        Alter a tag under a metalake.

        Args:
            tag_name (str): The name of the tag.
            changes (TagChange): The changes to apply to the tag.

        Returns:
            Tag: The altered tag.

        Raises:
            NoSuchTagException: If the tag does not exist.
            NoSuchMetalakeException: If the metalake does not exist.
            IllegalArgumentException: If the changes cannot be applied to the tag.
            TagAlreadyExistsException: If a tag with the new name already exists.
        """
        pass

    @abstractmethod
    def delete_tag(self, tag_name: str) -> bool:
        """
        Delete a tag under a metalake.

        Args:
            tag_name (str): The name of the tag.

        Returns:
            bool: True if the tag was deleted, False otherwise.

        Raises:
            NoSuchTagException: If the tag does not exist.
            NoSuchMetalakeException: If the metalake does not exist.
        """
        pass

    def list_policy_associations_for_tag(
        self, tag_name: str
    ) -> list[PolicyTagAssociation]:
        """
        List the policy associations of a tag under a metalake.

        Args:
            tag_name (str): The name of the tag.

        Returns:
            list[PolicyTagAssociation]: The policy associations of the tag.

        Raises:
            UnsupportedOperationException: If the implementation does not
                support policy associations for tags.
        """
        raise UnsupportedOperationException(
            "listPolicyAssociationsForTag is not supported"
        )

    def add_policy_for_tag(
        self,
        tag_name: str,
        policy_name: str,
        selector: Optional[PolicyAssociationSelector] = None,
    ) -> PolicyTagAssociation:
        """
        Associate a policy with a tag under a metalake. When no selector is
        given, the association applies to all values of the tag.

        Args:
            tag_name (str): The name of the tag.
            policy_name (str): The name of the policy.
            selector (PolicyAssociationSelector): The selector scoping the
                association, or None for all tag values.

        Returns:
            PolicyTagAssociation: The created policy association.

        Raises:
            UnsupportedOperationException: If the implementation does not
                support policy associations for tags.
        """
        raise UnsupportedOperationException("addPolicyForTag is not supported")

    def remove_policy_from_tag(self, tag_name: str, policy_name: str) -> None:
        """
        Remove the association between a policy and a tag under a metalake.

        Args:
            tag_name (str): The name of the tag.
            policy_name (str): The name of the policy.

        Raises:
            UnsupportedOperationException: If the implementation does not
                support policy associations for tags.
        """
        raise UnsupportedOperationException("removePolicyFromTag is not supported")
