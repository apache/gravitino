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
    _type: str = field(metadata=config(field_name="@type"))

    def __init__(self, type: str):
        self._type = type

    @abstractmethod
    def metalake_change(self):
        pass


class MetalakeUpdateRequest:
    """Represents an interface for Metalake update requests."""

    @dataclass
    class RenameMetalakeRequest(MetalakeUpdateRequestBase):
        """Represents a request to rename a Metalake."""

        _new_name: str = field(metadata=config(field_name="newName"))
        """The new name for the Metalake."""

        def __init__(self, new_name: str):
            super().__init__("rename")
            self._new_name = new_name

        def validate(self):
            """Validates the fields of the request.

            Raises:
                IllegalArgumentException if the new name is not set.
            """
            if not self._new_name:
                raise ValueError('"newName" field is required and cannot be empty')

        def metalake_change(self):
            return MetalakeChange.rename(self._new_name)

    @dataclass
    class UpdateMetalakeCommentRequest(MetalakeUpdateRequestBase):
        """Represents a request to update the comment on a Metalake."""

        _new_comment: str = field(metadata=config(field_name="newComment"))
        """The new comment for the Metalake."""

        def __init__(self, new_comment: str):
            super().__init__("updateComment")
            self._new_comment = new_comment

        def validate(self):
            """Validates the fields of the request.

            Raises:
                IllegalArgumentException if the new comment is not set.
            """
            if not self._new_comment:
                raise ValueError('"newComment" field is required and cannot be empty')

        def metalake_change(self):
            return MetalakeChange.update_comment(self._new_comment)

    @dataclass
    class SetMetalakePropertyRequest(MetalakeUpdateRequestBase):
        """Represents a request to set a property on a Metalake."""

        _property: str = field(metadata=config(field_name="property"))
        """The property to set."""

        _value: str = field(metadata=config(field_name="value"))
        """The value of the property."""

        def __init__(self, property: str, value: str):
            super().__init__("setProperty")
            self._property = property
            self._value = value

        def validate(self):
            """Validates the fields of the request.

            Raises:
                 IllegalArgumentException if property or value are not set.
            """
            if not self._property:
                raise ValueError('"property" field is required and cannot be empty')
            if not self._value:
                raise ValueError('"value" field is required and cannot be empty')

        def metalake_change(self):
            return MetalakeChange.set_property(self._property, self._value)

    @dataclass
    class RemoveMetalakePropertyRequest(MetalakeUpdateRequestBase):
        """Represents a request to remove a property from a Metalake."""

        _property: str = field(metadata=config(field_name="property"))
        """The property to remove."""

        def __init__(self, property: str):
            super().__init__("removeProperty")
            self._property = property

        def validate(self):
            """Validates the fields of the request.

            Raises:
                 IllegalArgumentException if property is not set.
            """
            if not self._property:
                raise ValueError('"property" field is required and cannot be empty')

        def metalake_change(self):
            return MetalakeChange.remove_property(self._property)
