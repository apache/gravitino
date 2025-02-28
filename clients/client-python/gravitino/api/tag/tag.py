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
from typing import ClassVar, Dict, List, Optional

from gravitino.api.auditable import Auditable
from gravitino.api.metadata_object import MetadataObject
from gravitino.exceptions.base import UnsupportedOperationException


class AssociatedObjects(ABC):
    """The interface of the associated objects of the tag."""

    @abstractmethod
    def count(self) -> int:
        """Get the number of associated objects.

        Returns:
            int: The number of associated objects.
        """
        objects = self.objects()
        return 0 if objects is None else len(objects)

    @abstractmethod
    def objects(self) -> Optional[List[MetadataObject]]:
        """Get the associated objects.

        Returns:
            Optional[List[MetadataObject]]: The list of objects that are associated with this tag..
        """
        pass


class Tag(Auditable):
    """The interface of a tag.

    A tag is a label that can be attached to a catalog, schema, table, fileset, topic,
    or column. It can be used to categorize, classify, or annotate these objects.
    """

    PROPERTY_COLOR: ClassVar[str] = "color"
    """
    A reserved property to specify the color of the tag. The color is a string of hex code that
    represents the color of the tag. The color is used to visually distinguish the tag from other
    tags.
    """

    @abstractmethod
    def name(self) -> str:
        """Get the name of the tag.

        Returns:
            str: The name of the tag.
        """
        pass

    @abstractmethod
    def comment(self) -> str:
        """Get the comment of the tag.

        Returns:
            str: The comment of the tag.
        """
        pass

    @abstractmethod
    def properties(self) -> Dict[str, str]:
        """Get the properties of the tag.

        Returns:
            Dict[str, str]: The properties of the tag.
        """
        pass

    @abstractmethod
    def inherited(self) -> Optional[bool]:
        """Check if the tag is inherited from a parent object or not.

        If the tag is inherited, it will return `True`, if it is owned by the object itself, it will return `False`.

        **Note**. The return value is optional, only when the tag is associated with an object, and called from the
        object, the return value will be present. Otherwise, it will be empty.

        Returns:
            Optional[bool]:
                True if the tag is inherited, false if it is owned by the object itself. Empty if the
                tag is not associated with any object.
        """
        pass

    def associated_objects(self) -> AssociatedObjects:
        """The associated objects of the tag.

        Raises:
            UnsupportedOperationException: The associated_objects method is not supported.

        Returns:
            AssociatedObjects: The associated objects of the tag.
        """
        raise UnsupportedOperationException(
            "The associated_objects method is not supported."
        )
