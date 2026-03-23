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

from gravitino.api.policy import Policy


class SupportsPolicies(ABC):
    """The interface for supporting getting or associating
    policies with a metadata object. This interface will be mixed
    with metadata objects to provide policy operations."""

    @abstractmethod
    def list_policies(self) -> list[str]:
        """
        List all the policy names for the specific object.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            list[str]: The policy name list for the specific object.
        """
        raise NotImplementedError()

    @abstractmethod
    def list_policy_infos(self) -> list[Policy]:
        """
        List all the policies with details for the specific object.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            list[Policy]: the policy list with details for the specific object.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_policy(self, policy_name: str) -> Policy:
        """
        Get a policy by its name for the specific object.

        Args:
            policy_name (str): The name of the policy.

        Raises:
            NotImplementedError: If the method is not implemented.
            NoSuchPolicyException: If the policy with the specific name does not exist.

        Returns:
            Policy: The policy.
        """
        raise NotImplementedError()

    @abstractmethod
    def associate_policies(
        self,
        policies_to_add: list[str],
        policies_to_remove: list[str],
    ) -> list[str]:
        """
        Associate policies to the specific object.
        The policiesToAdd will be applied to the object and the policiesToRemove
        will be removed from the object. Note that:
        1. Adding or removing policies that are not existed will be ignored.
        2. If the same name policy is in both policiesToAdd and policiesToRemove,
        it will be ignored.
        3. If the policy is already applied to the object, it will
        throw PolicyAlreadyAssociatedException

        Args:
            policies_to_add (list[str]): The policy name list to be added to the specific object.
            policies_to_remove (list[str]): The policy name list to be removed from the specific object.

        Raises:
            NotImplementedError: If the method is not implemented.
            PolicyAlreadyAssociatedException: If the policy is already applied to the object.

        Returns:
            list[str]: The list of applied policies.
        """
        raise NotImplementedError()
