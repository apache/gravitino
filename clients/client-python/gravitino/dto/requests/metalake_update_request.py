"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from abc import abstractmethod
from dataclasses import dataclass, field

from dataclasses_json import config

from gravitino.api.metalake_change import MetalakeChange
from gravitino.rest.rest_message import RESTRequest


@dataclass
class MetalakeUpdateRequestBase(RESTRequest):
    type: str = field(metadata=config(field_name='@type'))

    def __init__(self, type: str):
        self.type = type

    @abstractmethod
    def metalake_change(self):
        pass


class MetalakeUpdateRequest:
    """Represents an interface for Metalake update requests."""

    @dataclass
    class RenameMetalakeRequest(MetalakeUpdateRequestBase):
        """Represents a request to rename a Metalake."""

        new_name: str = field(metadata=config(field_name='newName'))
        """The new name for the Metalake."""

        def __init__(self, new_name: str):
            super().__init__("rename")
            self.new_name = new_name

        def validate(self):
            """Validates the fields of the request.

            Raises:
                IllegalArgumentException if the new name is not set.
            """
            if not self.new_name:
                raise ValueError('"newName" field is required and cannot be empty')

        def metalake_change(self):
            return MetalakeChange.rename(self.new_name)

    @dataclass
    class UpdateMetalakeCommentRequest(MetalakeUpdateRequestBase):
        """Represents a request to update the comment on a Metalake."""

        new_comment: str = field(metadata=config(field_name='newComment'))
        """The new comment for the Metalake."""

        def __init__(self, new_comment: str):
            super().__init__("updateComment")
            self.new_comment = new_comment

        def validate(self):
            """Validates the fields of the request.

            Raises:
                IllegalArgumentException if the new comment is not set.
            """
            if not self.new_comment:
                raise ValueError('"newComment" field is required and cannot be empty')

        def metalake_change(self):
            return MetalakeChange.update_comment(self.new_comment)

    @dataclass
    class SetMetalakePropertyRequest(MetalakeUpdateRequestBase):
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
    class RemoveMetalakePropertyRequest(MetalakeUpdateRequestBase):
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
