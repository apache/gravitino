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

from gravitino.api.policy import Policy, PolicyContent


class PolicyOperations(ABC):
    """
    The interface of the policy operations. The policy operations
    are used to manage policies under a metalake. This interface will be
    mixed with GravitinoMetalake or GravitinoClient to provide policy operations.
    """

    @abstractmethod
    async def list_policies(self) -> list[str]:
        """
        List all the policy names under a metalake.

        Raises:
            NotImplementedError: If the method is not implemented.
            NoSuchMetalakeException: If the metalake does not exist.

        Returns:
            list[str]: The list of policy names under the metalake.
        """
        raise NotImplementedError()

    @abstractmethod
    async def list_policy_infos(self) -> list[Policy]:
        """
        List all the policies with detailed information under a metalake.

        Raises:
            NotImplementedError: If the method is not implemented.
            NoSuchMetalakeException: If the metalake does not exist.

        Returns:
            list[Policy]: The list of policies with details under the metalake.
        """
        raise NotImplementedError()

    @abstractmethod
    async def get_policy(self, policy_name: str) -> Policy:
        """
        Get a policy by its name under a metalake.

        Args:
            policy_name (str): The name of the policy.

        Raises:
            NotImplementedError: If the method is not implemented.
            NoSuchMetalakeException: If the metalake does not exist.

        Returns:
            Policy: The policy instance.
        """
        raise NotImplementedError()

    @abstractmethod
    async def create_policy(
        self,
        name: str,
        policy_type: str,
        comment: str,
        enabled: bool,
        content: PolicyContent,
    ) -> Policy:
        raise NotImplementedError()

    @abstractmethod
    async def enable_policy(self, policy_name: str) -> None:
        """
        Enable a policy under a metalake. If the policy is already enabled, this method does nothing.

        Args:
            policy_name (str):  The name of the policy to enable.

        Raises:
            NotImplementedError: If the method is not implemented.
            NoSuchPolicyException: If the policy does not exist.
        """
        raise NotImplementedError()

    @abstractmethod
    async def disable_policy(self, policy_name: str) -> None:
        """
        Disable a policy under a metalake. If the policy is already disabled, this method does nothing.

        Args:
            policy_name (str): The name of the policy to disable.

        Raises:
            NotImplementedError: If the method is not implemented.
            NoSuchPolicyException: If the policy does not exist.
        """
        raise NotImplementedError()

    @abstractmethod
    async def alter_policy(
        self,
        policy_name: str,
        *changes,
    ) -> Policy:
        # todo
        raise NotImplementedError()

    @abstractmethod
    async def delete_policy(self, policy_name: str) -> bool:
        """
        Delete a policy under a metalake.

        Args:
            policy_name (str): The name of the policy to delete.

        Raises:
            NotImplementedError: If the method is not implemented.
            NoSuchPolicyException: If the policy does not exist.

        Returns:
            bool: True if the policy is deleted successfully, False otherwise.
        """
        raise NotImplementedError()
