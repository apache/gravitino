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

from gravitino.api.tag.tag import Tag
from gravitino.api.tag.tag_change import TagChange


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
