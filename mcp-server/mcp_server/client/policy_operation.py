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
    async def associate_policy_with_metadata(
        self,
        metadata_full_name: str,
        metadata_type: str,
        policies_to_add: list,
        policies_to_remove: list,
    ) -> str:
        """
        Associate policies with metadata.

        Args:
            metadata_full_name: Full name of the metadata object to associate policies with.
            It's typically in the format "catalog.schema.table" or "catalog.schema" or "catalog"
            or "catalog.schema.fileset". The "model", "topic" are also supported and the format
            is the same as for "catalog.schema.table".
            metadata_type: Type of the metadata (e.g., "table", "column")
            policies_to_add: List of policy names to associate with the metadata
            policies_to_remove: List of policy names to disassociate from the metadata

        Returns:
            str: JSON formatted string containing list of policy names that were
            successfully associated with the metadata
        """
        pass

    @abstractmethod
    async def get_policy_for_metadata(
        self, metadata_full_name: str, metadata_type: str, policy_name: str
    ) -> str:
        """
        Get the policy associated with a specific metadata item.

        Args:
            metadata_full_name: Full name of the metadata object to associate policies with.
            It's typically in the format "catalog.schema.table" or "catalog.schema" or "catalog"
            or "catalog.schema.fileset". The "model", "topic" are also supported and the format
            is the same as for "catalog.schema.table".
            metadata_type: Type of the metadata (e.g., "table", "column")
            policy_name: Name of the policy

        Returns:
            str: JSON formatted string containing policy metadata
        """
        pass

    @abstractmethod
    async def list_policies_for_metadata(
        self, metadata_full_name: str, metadata_type: str
    ) -> str:
        """
        List all policies associated with a specific metadata item.

        Args:
            metadata_full_name: Full name of the metadata object to associate policies with.
            It's typically in the format "catalog.schema.table" or "catalog.schema" or "catalog"
            or "catalog.schema.fileset". The "model", "topic" are also supported and the format
            is the same as for "catalog.schema.table".
            metadata_type: Type of the metadata (e.g., "table", "column")

        Returns:
            str: JSON formatted string containing list of policy metadata associated with the metadata
        """
        pass

    @abstractmethod
    async def list_metadata_by_policy(self, policy_name: str) -> str:
        """
        List all metadata items associated with a specific policy.

        Args:
            policy_name: Name of the policy to filter metadata by

        Returns:
            str: JSON formatted string containing list of metadata items associated with the policy
        """
        pass
