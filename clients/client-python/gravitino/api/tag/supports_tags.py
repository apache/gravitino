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
from typing import List

from gravitino.api.tag.tag import Tag


class SupportsTags(ABC):
    """Interface for supporting getting or associate tags to objects.

    This interface will be mixed with metadata objects to provide tag operations.
    """

    @abstractmethod
    def list_tags(self) -> List[str]:
        """List all the tag names for the specific object.

        Returns:
            List[str]: The list of tag names.
        """
        pass

    @abstractmethod
    def list_tags_info(self) -> List[Tag]:
        """List all the tags with details for the specific object.

        Returns:
            List[Tag]: The list of tags.
        """
        pass

    @abstractmethod
    def get_tag(self, name: str) -> Tag:
        """Get a tag by its name for the specific object.

        Args:
            name (str): The name of the tag.

        Raises:
            NoSuchTagException: If the tag does not associate with the object.

        Returns:
            Tag: The tag.
        """
        pass

    @abstractmethod
    def associate_tags(
        self, tags_to_add: List[str], tags_to_remove: List[str]
    ) -> List[str]:
        """Associate tags to the specific object.

        The `tags_to_add` will be added to the object, and the `tags_to_remove` will be removed from the object.

        Note that:
        1. Adding or removing tags that are not existed will be ignored.
        2. If the same name tag is in both `tags_to_add` and `tags_to_remove`, it will be ignored.
        3. If the tag is already associated with the object, it will raise `TagAlreadyAssociatedException`.

        Args:
            tags_to_add (List[str]): The arrays of tag name to be added to the object.
            tags_to_remove (List[str]): The array of tag name to be removed from the object.

        Raises:
            TagAlreadyAssociatedException: If the tag is already associated with the object.

        Returns:
            List[str]: The array of tag names that are associated with the object.
        """
        pass
