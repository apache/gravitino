"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from abc import abstractmethod
from dataclasses import dataclass, field

from dataclasses_json import config

from gravitino.api.fileset_change import FilesetChange
from gravitino.rest.rest_message import RESTRequest


@dataclass
class FilesetUpdateRequestBase(RESTRequest):
    _type: str = field(metadata=config(field_name="@type"))

    def __init__(self, type: str):
        self._type = type

    @abstractmethod
    def fileset_change(self):
        pass


class FilesetUpdateRequest:
    """Request to update a fileset."""

    @dataclass
    class RenameFilesetRequest(FilesetUpdateRequestBase):
        """The fileset update request for renaming a fileset."""

        _new_name: str = field(metadata=config(field_name="newName"))
        """The new name for the Fileset."""

        def __init__(self, new_name: str):
            super().__init__("rename")
            self._new_name = new_name

        def validate(self):
            """Validates the fields of the request.

            Raises:
                IllegalArgumentException if the new name is not set.
            """
            if not self._new_name:
                raise ValueError('"new_name" field is required and cannot be empty')

        def fileset_change(self):
            """Returns the fileset change.

            Returns:
                the fileset change.
            """
            return FilesetChange.rename(self._new_name)

    @dataclass
    class UpdateFilesetCommentRequest(FilesetUpdateRequestBase):
        """Represents a request to update the comment on a Fileset."""

        _new_comment: str = field(metadata=config(field_name="newComment"))
        """The new comment for the Fileset."""

        def __init__(self, new_comment: str):
            super().__init__("updateComment")
            self._new_comment = new_comment

        def validate(self):
            """Validates the fields of the request.

            Raises:
                IllegalArgumentException if the new comment is not set.
            """
            if not self._new_comment:
                raise ValueError('"new_comment" field is required and cannot be empty')

        def fileset_change(self):
            """Returns the fileset change"""
            return FilesetChange.update_comment(self._new_comment)

    @dataclass
    class SetFilesetPropertyRequest(FilesetUpdateRequestBase):
        """Represents a request to set a property on a Fileset."""

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

        def fileset_change(self):
            return FilesetChange.set_property(self._property, self._value)

    @dataclass
    class RemoveFilesetPropertyRequest(FilesetUpdateRequestBase):
        """Represents a request to remove a property from a Fileset."""

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

        def fileset_change(self):
            return FilesetChange.remove_property(self._property)
