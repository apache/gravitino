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

from typing import Optional

from abc import ABC, abstractmethod

from gravitino.api.policy.policy import Policy
from gravitino.api.policy.policy_change import PolicyChange
from gravitino.api.policy.policy_content import PolicyContent
from gravitino.api.policy.policy_tag_association import PolicyTagAssociation
from gravitino.exceptions.base import UnsupportedOperationException
from gravitino.api.pagination import Page


class PolicyOperations(ABC):
    """The operations for interacting with policies in a metalake: listing,
    creating, altering, enabling, disabling and deleting policies."""

    @abstractmethod
    def list_policies(self) -> list[str]:
        """Lists the names of all policies in the metalake.

        Returns:
            list[str]: The names of the policies.

        Raises:
            NoSuchMetalakeException: If the metalake does not exist.
        """
        raise NotImplementedError()

    def list_policies_page(
        self, *, page_size: int, page_token: Optional[str] = None
    ) -> Page[str]:
        """One page of ``list_policies``, in ascending order.

        The default implementation raises UnsupportedOperationException.

        Args:
            page_size: The maximum number of entries in the page; must be at least 1.
            page_token: The ``next_page_token`` of the previous page, or None for the first page.

        Returns:
            Page[str]: The page.
        """
        raise UnsupportedOperationException("Paged listing is not supported")

    @abstractmethod
    def list_policy_infos(self) -> list[Policy]:
        """Lists all policies in the metalake with their details.

        Returns:
            list[Policy]: The policies.

        Raises:
            NoSuchMetalakeException: If the metalake does not exist.
        """
        raise NotImplementedError()

    def list_policy_infos_page(
        self, *, page_size: int, page_token: Optional[str] = None
    ) -> Page[Policy]:
        """One page of ``list_policy_infos``, in ascending order.

        The default implementation raises UnsupportedOperationException.

        Args:
            page_size: The maximum number of entries in the page; must be at least 1.
            page_token: The ``next_page_token`` of the previous page, or None for the first page.

        Returns:
            Page[Policy]: The page.
        """
        raise UnsupportedOperationException("Paged listing is not supported")

    @abstractmethod
    def get_policy(self, policy_name: str) -> Policy:
        """Gets a policy by name.

        Args:
            policy_name: The name of the policy.

        Returns:
            Policy: The policy.

        Raises:
            NoSuchPolicyException: If the policy does not exist.
        """
        raise NotImplementedError()

    @abstractmethod
    def create_policy(
        self,
        policy_name: str,
        policy_type: str,
        comment: str | None,
        enabled: bool,
        content: PolicyContent,
    ) -> Policy:
        """Creates a policy in the metalake.

        Args:
            policy_name: The name of the policy.
            policy_type: The type of the policy.
            comment: The comment of the policy.
            enabled: Whether the policy is enabled.
            content: The content of the policy.

        Returns:
            Policy: The created policy.

        Raises:
            PolicyAlreadyExistsException: If a policy with the same name
                already exists.
        """
        raise NotImplementedError()

    @abstractmethod
    def enable_policy(self, policy_name: str) -> None:
        """Enables a policy.

        Args:
            policy_name: The name of the policy.

        Raises:
            NoSuchPolicyException: If the policy does not exist.
        """
        raise NotImplementedError()

    @abstractmethod
    def disable_policy(self, policy_name: str) -> None:
        """Disables a policy.

        Args:
            policy_name: The name of the policy.

        Raises:
            NoSuchPolicyException: If the policy does not exist.
        """
        raise NotImplementedError()

    @abstractmethod
    def alter_policy(self, policy_name: str, *changes: PolicyChange) -> Policy:
        """Alters a policy in the metalake.

        Args:
            policy_name: The name of the policy.
            changes: The changes to apply to the policy.

        Returns:
            Policy: The altered policy.

        Raises:
            NoSuchPolicyException: If the policy does not exist.
            PolicyAlreadyExistsException: If a rename targets an existing
                policy name.
        """
        raise NotImplementedError()

    @abstractmethod
    def delete_policy(self, policy_name: str) -> bool:
        """Deletes a policy from the metalake.

        Args:
            policy_name: The name of the policy.

        Returns:
            bool: True if the policy was deleted, False if it did not exist.

        Raises:
            NoSuchPolicyException: If the policy does not exist.
        """
        raise NotImplementedError()

    def list_tag_associations_for_policy(
        self, policy_name: str
    ) -> list[PolicyTagAssociation]:
        """Lists the tag associations of a policy.

        Args:
            policy_name: The name of the policy.

        Returns:
            list[PolicyTagAssociation]: The tag associations of the policy.

        Raises:
            NoSuchPolicyException: If the policy does not exist.
            UnsupportedOperationException: If the implementation does not
                support listing tag associations.
        """
        raise UnsupportedOperationException(
            "listTagAssociationsForPolicy is not supported"
        )

    def list_tag_names_for_policy_page(
        self, policy_name: str, *, page_size: int, page_token: Optional[str] = None
    ) -> Page[str]:
        """One page of the names of the tags associated with a policy, in ascending order.

        The default implementation raises UnsupportedOperationException.

        Args:
            policy_name: The policy name.
            page_size: The maximum number of entries in the page; must be at least 1.
            page_token: The ``next_page_token`` of the previous page, or None for the first page.

        Returns:
            Page[str]: The page.
        """
        raise UnsupportedOperationException("Paged listing is not supported")
