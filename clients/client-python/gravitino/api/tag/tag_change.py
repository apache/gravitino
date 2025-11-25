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

from abc import ABC
from dataclasses import dataclass, field

from dataclasses_json import config


class TagChange(ABC):
    """Interface for supporting tag changes.
    This interface will be used to provide tag modification operations for each tag."""

    @staticmethod
    def rename(new_name: str) -> RenameTag:
        """
        Create a tag change instance to rename the tag.

        Args:
            new_name (str): The new name of the tag.

        Returns:
            TagChange: A tag change instance to rename the tag.
        """
        return TagChange.RenameTag(new_name)

    @staticmethod
    def update_comment(new_comment: str) -> UpdateComment:
        """
        Create a tag change instance to update the tag comment.

        Args:
            new_comment (str): The new comment of the tag.

        Returns:
            TagChange: A tag change instance to update the tag comment.
        """
        return TagChange.UpdateComment(new_comment)

    @staticmethod
    def set_property(tag_property: str, tag_value: str) -> SetProperty:
        """
        Creates a new tag change instance to set the property and value for the tag.

        Args:
            tag_property (str): The property to set.
            tag_value (str): The value to set.

        Returns:
            TagChange: The tag change instance to set the property and value for the tag.
        """
        return TagChange.SetProperty(tag_property, tag_value)

    @staticmethod
    def remove_property(tag_property: str) -> RemoveProperty:
        """
        Creates a new tag change instance to remove a property from the tag.

        Args:
            tag_property (str): The property to remove.

        Returns:
            TagChange: The tag change instance to remove a property from the tag.
        """
        return TagChange.RemoveProperty(tag_property)

    @dataclass(frozen=True, eq=True)
    class RenameTag:
        """A tag change to rename the tag."""

        _new_name: str = field(metadata=config(field_name="newName"))

        @property
        def new_name(self) -> str:
            """The new name of the tag."""
            return self._new_name

        def __str__(self) -> str:
            return f"RenameTag {self._new_name}"

    @dataclass(frozen=True, eq=True)
    class UpdateComment:
        """A tag change to update the tag comment."""

        _new_comment: str = field(metadata=config(field_name="newComment"))

        @property
        def new_comment(self) -> str:
            """The new comment of the tag."""
            return self._new_comment

        def __str__(self) -> str:
            return f"UpdateComment {self._new_comment}"

    @dataclass(frozen=True, eq=True)
    class SetProperty:
        """A tag change to set a property-value pair for the tag."""

        _property: str = field(metadata=config(field_name="property"))
        _value: str = field(metadata=config(field_name="value"))

        @property
        def name(self) -> str:
            """The property to set."""
            return self._property

        @property
        def value(self) -> str:
            """The value to set."""
            return self._value

        def __str__(self) -> str:
            return f"SetProperty {self._property}={self._value}"

    @dataclass(frozen=True, eq=True)
    class RemoveProperty:
        """A tag change to remove a property-value pair for the tag."""

        _property: str = field(metadata=config(field_name="property"))

        @property
        def removed_property(self) -> str:
            """The property to remove."""
            return self._property

        def __str__(self) -> str:
            return f"REMOVETAGPROPERTY {self._property}"
