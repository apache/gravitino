"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from dataclasses import dataclass


class MetalakeChange:
    """A metalake change is a change to a metalake. It can be used to rename a metalake, update the
    comment of a metalake, set a property and value pair for a metalake, or remove a property from a
    metalake.
    """

    @staticmethod
    def rename(new_name: str) -> 'MetalakeChange.RenameMetalake':
        """Creates a new metalake change to rename the metalake.

        Args:
            new_name: The New name of the metalake.

        Returns:
            The metalake change.
        """
        return MetalakeChange.RenameMetalake(new_name)

    @staticmethod
    def update_comment(new_comment: str) -> 'MetalakeChange.UpdateMetalakeComment':
        """Creates a new metalake change to update the metalake comment.

        Args:
            new_comment: The new comment of the metalake.

        Return:
            The metalake change.
        """
        return MetalakeChange.UpdateMetalakeComment(new_comment)

    @staticmethod
    def set_property(property: str, value: str) -> 'SetProperty':
        """Creates a new metalake change to set a property and value pair for the metalake.

        Args:
            property: The property name to set.
            value: The value to set the property to.

        Return:
             The metalake change.
        """
        return MetalakeChange.SetProperty(property, value)

    @staticmethod
    def remove_property(property: str) -> 'RemoveProperty':
        """Creates a new metalake change to remove a property from the metalake.

        Args:
            property: The property name to remove.

        Return:
            The metalake change.
        """
        return MetalakeChange.RemoveProperty(property)

    @dataclass(frozen=True)
    class RenameMetalake:
        """A metalake change to rename the metalake."""
        newName: str

        def __str__(self):
            return f"RENAMEMETALAKE {self.newName}"

    @dataclass(frozen=True)
    class UpdateMetalakeComment:
        """A metalake change to update the metalake comment"""
        newComment: str

        def __str__(self):
            return f"UPDATEMETALAKECOMMENT {self.newComment}"

    @dataclass(frozen=True)
    class SetProperty:
        """A metalake change to set a property and value pair for the metalake"""
        property: str
        value: str

        def __str__(self):
            return f"SETPROPERTY {self.property} {self.value}"

    @dataclass(frozen=True)
    class RemoveProperty:
        """A metalake change to remove a property from the metalake"""
        property: str

        def __str__(self):
            return f"REMOVEPROPERTY {self.property}"
