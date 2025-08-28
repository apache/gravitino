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


class TagOperation(ABC):
    """
    Abstract base class for Gravitino tag operations.
    """

    @abstractmethod
    async def create_tag(
        self, tag_name: str, tag_comment: str, tag_properties: dict
    ) -> str:
        """
        Create a new tag within the specified metalake.

        Args:
            tag_name: Name of the tag to be created
            tag_comment: Description or comment for the tag
            tag_properties: Dictionary of key-value pairs representing tag properties

        Returns:
            str: JSON-formatted string containing the created tag information
        """
        pass

    @abstractmethod
    async def get_tag_by_name(self, tag_name: str) -> str:
        """
        Load a tag by its name.

        Args:
            tag_name: Name of the tag to get

        Returns:
            str: JSON-formatted string containing the tag information
        """
        pass

    @abstractmethod
    async def list_of_tags(self) -> str:
        """
        Retrieve the list of tags within the metalake

        Returns:
            str: JSON-formatted string containing tag list information
        """
        pass

    @abstractmethod
    async def alter_tag(self, tag_name: str, updates: list) -> str:
        """
        Alter an existing tag within the specified metalake.

        Args:
            tag_name: Name of the tag to be altered
            updates: List of update operations to be applied to the tag

        Returns:
            str: JSON-formatted string containing the altered tag information
        """
        pass

    @abstractmethod
    async def delete_tag(self, name: str) -> None:
        """
        Delete a tag by its name.

        Args:
            name: Name of the tag to delete

        Returns:
            None
        """
        pass

    @abstractmethod
    async def associate_tag_with_metadata(
        self,
        metadata_full_name: str,
        metadata_type: str,
        tags_to_associate: list,
        tags_to_disassociate: list,
    ) -> str:
        """
        Associate tags with metadata.

        Args:
            metadata_full_name: Full name of the metadata item (e.g., table, column)
            metadata_type: Type of the metadata (e.g., "table", "column")
            tags_to_associate: List of tag names to associate with the metadata
            tags_to_disassociate: List of tag names to disassociate from the metadata

        Returns:
            str: JSON formatted string containing list of tag names that were
            successfully associated with the metadata
        """
        pass

    @abstractmethod
    async def list_tags_for_metadata(
        self, metadata_full_name: str, metadata_type: str
    ) -> str:
        """
        List all tags associated with a specific metadata item.

        Args:
            metadata_full_name: Full name of the metadata item (e.g., table, column)
            metadata_type: Type of the metadata (e.g., "table", "column")

        Returns:
            str: JSON formatted string containing list of tag names associated with the metadata
        """
        pass

    @abstractmethod
    async def list_metadata_by_tag(self, tag_name: str) -> str:
        """
        List all metadata items associated with a specific tag.

        Args:
            tag_name: Name of the tag to filter metadata by

        Returns:
            str: JSON formatted string containing list of metadata items associated with the tag
        """
        pass
