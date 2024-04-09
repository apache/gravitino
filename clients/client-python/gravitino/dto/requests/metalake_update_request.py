"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from abc import abstractmethod, ABC
from dataclasses import dataclass, field

from dataclasses_json import config, DataClassJsonMixin

from gravitino.meta_change import MetalakeChange


@dataclass
class MetalakeUpdateRequestType(DataClassJsonMixin):
    type: str = field(metadata=config(field_name='@type'))

    def __init__(self, type: str):
        self.type = type


class MetalakeUpdateRequest:
    """Represents an interface for Metalake update requests."""

    @abstractmethod
    def validate(self):
        pass

    @abstractmethod
    def metalake_change(self):
        pass

    @dataclass
    class RenameMetalakeRequest(MetalakeUpdateRequestType):
        """Represents a request to rename a Metalake."""

        newName: str = None
        """The new name for the Metalake."""

        def __init__(self, newName: str):
            super().__init__("rename")
            self.newName = newName

        def validate(self):
            """Validates the fields of the request.

            Raises:
                IllegalArgumentException if the new name is not set.
            """
            if not self.newName:
                raise ValueError('"newName" field is required and cannot be empty')

        def metalake_change(self):
            return MetalakeChange.rename(self.newName)

    @dataclass
    class UpdateMetalakeCommentRequest(MetalakeUpdateRequestType):
        """Represents a request to update the comment on a Metalake."""

        newComment: str = None
        """The new comment for the Metalake."""

        def __init__(self, newComment: str):
            super().__init__("updateComment")
            self.newComment = newComment

        def validate(self):
            """Validates the fields of the request.

            Raises:
                IllegalArgumentException if the new comment is not set.
            """
            if not self.newComment:
                raise ValueError('"newComment" field is required and cannot be empty')

        def metalake_change(self):
            return MetalakeChange.update_comment(self.newComment)

    @dataclass
    class SetMetalakePropertyRequest(MetalakeUpdateRequestType):
        """Represents a request to set a property on a Metalake."""

        property: str = None
        """The property to set."""

        value: str = None
        """The value of the property."""

        def __init__(self, property: str, value: str):
            super().__init__("setProperty")
            self.property = property
            self.value = value

        def validate(self):
            """Validates the fields of the request.

            Raises:
                 IllegalArgumentException if property or value are not set.
            """
            if not self.property:
                raise ValueError('"property" field is required and cannot be empty')
            if not self.value:
                raise ValueError('"value" field is required and cannot be empty')

        def metalake_change(self):
            return MetalakeChange.set_property(self.property, self.value)

    @dataclass
    class RemoveMetalakePropertyRequest(MetalakeUpdateRequestType):
        """Represents a request to remove a property from a Metalake."""

        property: str = None
        """The property to remove."""

        def __init__(self, property: str):
            super().__init__("removeProperty")
            self.property = property

        def validate(self):
            """Validates the fields of the request.

            Raises:
                 IllegalArgumentException if property is not set.
            """
            if not self.property:
                raise ValueError('"property" field is required and cannot be empty')

        def metalake_change(self):
            return MetalakeChange.remove_property(self.property)
