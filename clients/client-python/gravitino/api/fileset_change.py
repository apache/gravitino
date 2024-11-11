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

from abc import ABC
from dataclasses import dataclass, field

from dataclasses_json import config


class FilesetChange(ABC):
    """A fileset change is a change to a fileset. It can be used to rename a fileset, update the comment
    of a fileset, set a property and value pair for a fileset, or remove a property from a fileset.
    """

    @staticmethod
    def rename(new_name):
        """Creates a new fileset change to rename the fileset.

        Args:
            new_name: The new name of the fileset.

        Returns:
             The fileset change.
        """
        return FilesetChange.RenameFileset(new_name)

    @staticmethod
    def update_comment(new_comment):
        """Creates a new fileset change to update the fileset comment.

        Args:
            new_comment: The new comment for the fileset.

        Returns:
             The fileset change.
        """
        return FilesetChange.UpdateFilesetComment(new_comment)

    @staticmethod
    def set_property(fileset_property, value):
        """Creates a new fileset change to set the property and value for the fileset.

        Args:
            fileset_property: The property name to set.
            value: The value to set the property to.

        Returns:
             The fileset change.
        """
        return FilesetChange.SetProperty(fileset_property, value)

    @staticmethod
    def remove_property(fileset_property):
        """Creates a new fileset change to remove a property from the fileset.

        Args:
            fileset_property: The property name to remove.

        Returns:
            The fileset change.
        """
        return FilesetChange.RemoveProperty(fileset_property)

    @staticmethod
    def remove_comment():
        """Creates a new fileset change to remove comment from the fileset.

        Returns:
            The fileset change.

        Deprecated:
            Please use `update_comment(str)` with null value as the argument instead.
        """
        return FilesetChange.UpdateFilesetComment(None)

    @dataclass
    class RenameFileset:
        """A fileset change to rename the fileset."""

        _new_name: str = field(metadata=config(field_name="new_name"))

        def new_name(self):
            """Retrieves the new name set for the fileset.

            Returns:
                 The new name of the fileset.
            """
            return self._new_name

        def __eq__(self, other):
            """Compares this RenameFileset instance with another object for equality.
            Two instances are considered equal if they designate the same new name for the fileset.

            Args:
                 other: The object to compare with this instance.

            Returns:
                 true if the given object represents an identical fileset renaming operation; false otherwise.
            """
            if not isinstance(other, FilesetChange.RenameFileset):
                return False
            return self._new_name == other.new_name()

        def __hash__(self):
            """Generates a hash code for this RenameFileset instance.
            The hash code is primarily based on the new name for the fileset.

            Returns:
                 A hash code value for this renaming operation.
            """
            return hash(self._new_name)

        def __str__(self):
            """Provides a string representation of the RenameFile instance.
            This string includes the class name followed by the new name of the fileset.

            Returns:
                 A string summary of this renaming operation.
            """
            return f"RENAMEFILESET {self._new_name}"

    @dataclass
    class UpdateFilesetComment:
        """A fileset change to update the fileset comment."""

        _new_comment: str = field(metadata=config(field_name="new_comment"))

        def new_comment(self):
            """Retrieves the new comment intended for the fileset.

            Returns:
                 The new comment that has been set for the fileset.
            """
            return self._new_comment

        def __eq__(self, other) -> bool:
            """Compares this UpdateFilesetComment instance with another object for equality.
            Two instances are considered equal if they designate the same new comment for the fileset.

            Args:
                 other: The object to compare with this instance.

            Returns:
                 true if the given object represents the same comment update; false otherwise.
            """
            if not isinstance(other, FilesetChange.UpdateFilesetComment):
                return False
            return self._new_comment == other.new_comment()

        def __hash__(self):
            """Generates a hash code for this UpdateFileComment instance.
            The hash code is based on the new comment for the fileset.

            Returns:
                 A hash code representing this comment update operation.
            """
            return hash(self._new_comment)

        def __str__(self):
            """Provides a string representation of the UpdateFilesetComment instance.
            This string format includes the class name followed by the new comment for the fileset.

            Returns:
                 A string summary of this comment update operation.
            """
            return f"UPDATEFILESETCOMMENT {self._new_comment}"

    @dataclass
    class SetProperty:
        """A fileset change to set the property and value for the fileset."""

        _property: str = field(metadata=config(field_name="property"))
        _value: str = field(metadata=config(field_name="value"))

        def property(self):
            """Retrieves the name of the property being set in the fileset.

            Returns:
                 The name of the property.
            """
            return self._property

        def value(self):
            """Retrieves the value assigned to the property in the fileset.

            Returns:
                 The value of the property.
            """
            return self._value

        def __eq__(self, other) -> bool:
            """Compares this SetProperty instance with another object for equality.
            Two instances are considered equal if they have the same property and value for the fileset.

            Args:
                 other: The object to compare with this instance.

            Returns:
                 true if the given object represents the same property setting; false otherwise.
            """
            if not isinstance(other, FilesetChange.SetProperty):
                return False
            return self._property == other.property() and self._value == other.value()

        def __hash__(self):
            """Generates a hash code for this SetProperty instance.
            The hash code is based on both the property name and its assigned value.

            Returns:
                 A hash code value for this property setting.
            """
            return hash((self._property, self._value))

        def __str__(self):
            """Provides a string representation of the SetProperty instance.
            This string format includes the class name followed by the property and its value.

            Returns:
                 A string summary of the property setting.
            """
            return f"SETPROPERTY {self._property} {self._value}"

    @dataclass
    class RemoveProperty:
        """A fileset change to remove a property from the fileset."""

        _property: str = field(metadata=config(field_name="property"))

        def property(self):
            """Retrieves the name of the property to be removed from the fileset.

            Returns:
                 The name of the property for removal.
            """
            return self._property

        def __eq__(self, other) -> bool:
            """Compares this RemoveProperty instance with another object for equality.
            Two instances are considered equal if they target the same property for removal from the fileset.

            Args:
                 other: The object to compare with this instance.

            Returns:
                 true if the given object represents the same property removal; false otherwise.
            """
            if not isinstance(other, FilesetChange.RemoveProperty):
                return False
            return self._property == other.property()

        def __hash__(self):
            """Generates a hash code for this RemoveProperty instance.
            The hash code is based on the property name that is to be removed from the fileset.

            Returns:
                 A hash code value for this property removal operation.
            """
            return hash(self._property)

        def __str__(self):
            """Provides a string representation of the RemoveProperty instance.
            This string format includes the class name followed by the property name to be removed.

            Returns:
                 A string summary of the property removal operation.
            """
            return f"REMOVEPROPERTY {self._property}"

    @dataclass
    class RemoveComment:
        """A fileset change to remove comment from the fileset.

        Deprecated:
            Please use `UpdateFilesetComment(str)` with null value as the argument instead.
        """

        def __eq__(self, other) -> bool:
            """Compares this RemoveComment instance with another object for equality.
            Two instances are considered equal if they are RemoveComment instance.

            Args:
                 other: The object to compare with this instance.

            Returns:
                 true if the given object represents the comment removal; false otherwise.
            """
            return isinstance(other, FilesetChange.RemoveComment)

        def __hash__(self):
            """Generates a hash code for this RemoveComment instance.
            The hash code is based on the RemoveComment instance name.

            Returns:
                 A hash code value for comment removal operation.
            """
            return hash("REMOVECOMMENT")

        def __str__(self):
            """Provides a string representation of the RemoveComment instance.
            This string format includes the class name.

            Returns:
                 A string summary of the comment removal operation.
            """
            return "REMOVECOMMENT"
