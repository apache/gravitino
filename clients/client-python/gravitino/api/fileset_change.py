"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from abc import ABC


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
    def set_property(property, value):
        """Creates a new fileset change to set the property and value for the fileset.

        Args:
            property: The property name to set.
            value: The value to set the property to.

        Returns:
             The fileset change.
        """
        return FilesetChange.SetProperty(property, value)

    @staticmethod
    def remove_property(property):
        """Creates a new fileset change to remove a property from the fileset.

        Args:
            property: The property name to remove.

        Returns:
            The fileset change.
        """
        return FilesetChange.RemoveProperty(property)

    class RenameFileset:
        """A fileset change to rename the fileset."""

        def __init__(self, new_name):
            self.new_name = new_name

        def get_new_name(self):
            """Retrieves the new name set for the fileset.

            Returns:
                 The new name of the fileset.
            """
            return self.new_name

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
            return self.new_name == other.new_name

        def __hash__(self):
            """Generates a hash code for this RenameFileset instance.
            The hash code is primarily based on the new name for the fileset.

            Returns:
                 A hash code value for this renaming operation.
            """
            return hash(self.new_name)

        def __str__(self):
            """Provides a string representation of the RenameFile instance.
            This string includes the class name followed by the new name of the fileset.

            Returns:
                 A string summary of this renaming operation.
            """
            return f"RENAMEFILESET {self.new_name}"

    class UpdateFilesetComment:
        """A fileset change to update the fileset comment."""

        def __init__(self, new_comment):
            self.new_comment = new_comment

        def get_new_comment(self):
            """Retrieves the new comment intended for the fileset.

            Returns:
                 The new comment that has been set for the fileset.
            """
            return self.new_comment

        def __eq__(self, other):
            """Compares this UpdateFilesetComment instance with another object for equality.
            Two instances are considered equal if they designate the same new comment for the fileset.

            Args:
                 other: The object to compare with this instance.

            Returns:
                 true if the given object represents the same comment update; false otherwise.
            """
            if not isinstance(other, FilesetChange.UpdateFilesetComment):
                return False
            return self.new_comment == other.new_comment

        def __hash__(self):
            """Generates a hash code for this UpdateFileComment instance.
            The hash code is based on the new comment for the fileset.

            Returns:
                 A hash code representing this comment update operation.
            """
            return hash(self.new_comment)

        def __str__(self):
            """Provides a string representation of the UpdateFilesetComment instance.
            This string format includes the class name followed by the new comment for the fileset.

            Returns:
                 A string summary of this comment update operation.
            """
            return f"UPDATEFILESETCOMMENT {self.new_comment}"

    class SetProperty:
        """A fileset change to set the property and value for the fileset."""

        def __init__(self, property, value):
            self.property = property
            self.value = value

        def get_property(self):
            """Retrieves the name of the property being set in the fileset.

            Returns:
                 The name of the property.
            """
            return self.property

        def get_value(self):
            """Retrieves the value assigned to the property in the fileset.

            Returns:
                 The value of the property.
            """
            return self.value

        def __eq__(self, other):
            """Compares this SetProperty instance with another object for equality.
            Two instances are considered equal if they have the same property and value for the fileset.

            Args:
                 other: The object to compare with this instance.

            Returns:
                 true if the given object represents the same property setting; false otherwise.
            """
            if not isinstance(other, FilesetChange.SetProperty):
                return False
            return self.property == other.property and self.value == other.value

        def __hash__(self):
            """Generates a hash code for this SetProperty instance.
            The hash code is based on both the property name and its assigned value.

            Returns:
                 A hash code value for this property setting.
            """
            return hash((self.property, self.value))

        def __str__(self):
            """Provides a string representation of the SetProperty instance.
            This string format includes the class name followed by the property and its value.

            Returns:
                 A string summary of the property setting.
            """
            return f"SETPROPERTY {self.property} {self.value}"

    class RemoveProperty:
        """A fileset change to remove a property from the fileset."""

        def __init__(self, property):
            self.property = property

        def get_property(self):
            """Retrieves the name of the property to be removed from the fileset.

            Returns:
                 The name of the property for removal.
            """
            return self.property

        def __eq__(self, other):
            """Compares this RemoveProperty instance with another object for equality.
            Two instances are considered equal if they target the same property for removal from the fileset.

            Args:
                 other: The object to compare with this instance.

            Returns:
                 true if the given object represents the same property removal; false otherwise.
            """
            if not isinstance(other, FilesetChange.RemoveProperty):
                return False
            return self.property == other.property

        def __hash__(self):
            """Generates a hash code for this RemoveProperty instance.
            The hash code is based on the property name that is to be removed from the fileset.

            Returns:
                 A hash code value for this property removal operation.
            """
            return hash(self.property)

        def __str__(self):
            """Provides a string representation of the RemoveProperty instance.
            This string format includes the class name followed by the property name to be removed.

            Returns:
                 A string summary of the property removal operation.
            """
            return f"REMOVEPROPERTY {self.property}"
