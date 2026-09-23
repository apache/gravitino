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


class PolicyOperation(ABC):
    """
    Abstract base class for Gravitino policy operations.
    """

    @abstractmethod
    async def get_list_of_policies(self) -> str:
        """
        Retrieve the list of policies.

        Returns:
            str: JSON-formatted string containing policy list information
        """
        pass

    @abstractmethod
    async def load_policy(self, policy_name: str) -> str:
        """
        Load detailed information of a specific policy.

        Args:
            policy_name: Name of the policy

        Returns:
            str: JSON-formatted string containing full policy metadata
        """
        pass

    @abstractmethod
    async def list_policies_for_tag(self, tag_name: str) -> str:
        """List all policies directly associated with a tag.

        Args:
            tag_name: Name of the tag

        Returns:
            str: JSON-formatted list of policy-tag associations, including selectors
        """
        pass

    @abstractmethod
    async def associate_policy_with_tag(
        self, tag_name: str, policy_name: str, selector: dict
    ) -> str:
        """Associate one policy with a tag.

        Args:
            tag_name: Name of the tag
            policy_name: Name of the policy
            selector: Selector controlling which tag assignments match the policy

        Returns:
            str: JSON-formatted policy-tag association
        """
        pass

    @abstractmethod
    async def disassociate_policy_from_tag(
        self, tag_name: str, policy_name: str
    ) -> str:
        """Remove one policy association from a tag.

        Args:
            tag_name: Name of the tag
            policy_name: Name of the policy

        Returns:
            str: JSON-formatted removal confirmation
        """
        pass

    @abstractmethod
    async def list_tags_for_policy(self, policy_name: str) -> str:
        """List all tags directly associated with a policy.

        Args:
            policy_name: Name of the policy

        Returns:
            str: JSON-formatted list of policy-tag associations, including selectors
        """
        pass

    @abstractmethod
    async def list_policies_for_metadata(
        self, metadata_full_name: str, metadata_type: str
    ) -> str:
        """
        List the effective policies derived from a metadata item's effective tags.

        Args:
            metadata_full_name: Full name of the metadata object whose effective policies to list.
            It's typically in the format "catalog.schema.table" or "catalog.schema" or "catalog"
            or "catalog.schema.fileset". The "model", "topic" are also supported and the format
            is the same as for "catalog.schema.table".
            metadata_type: Type of the metadata (e.g., "table", "column")

        Returns:
            str: JSON-formatted list of effective policy metadata derived from the metadata's tags
        """
        pass
