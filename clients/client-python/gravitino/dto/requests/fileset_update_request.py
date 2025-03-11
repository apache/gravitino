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

from abc import abstractmethod
from dataclasses import dataclass, field

from dataclasses_json import config

from gravitino.api.file.fileset_change import FilesetChange
from gravitino.rest.rest_message import RESTRequest


@dataclass
class FilesetUpdateRequestBase(RESTRequest):
    _type: str = field(metadata=config(field_name="@type"))

    def __init__(self, action_type: str):
        self._type = action_type

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
            """Validates the fields of the request. Always pass."""
            pass

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

        def __init__(self, fileset_property: str, value: str):
            super().__init__("setProperty")
            self._property = fileset_property
            self._value = value

        def validate(self):
            """Validates the fields of the request.

            Raises:
                 IllegalArgumentException if fileset_property or value are not set.
            """
            if not self._property:
                raise ValueError(
                    '"fileset_property" field is required and cannot be empty'
                )
            if not self._value:
                raise ValueError('"value" field is required and cannot be empty')

        def fileset_change(self):
            return FilesetChange.set_property(self._property, self._value)

    @dataclass
    class RemoveFilesetPropertyRequest(FilesetUpdateRequestBase):
        """Represents a request to remove a property from a Fileset."""

        _property: str = field(metadata=config(field_name="property"))
        """The property to remove."""

        def __init__(self, fileset_property: str):
            super().__init__("removeProperty")
            self._property = fileset_property

        def validate(self):
            """Validates the fields of the request.

            Raises:
                 IllegalArgumentException if fileset_property is not set.
            """
            if not self._property:
                raise ValueError(
                    '"fileset_property" field is required and cannot be empty'
                )

        def fileset_change(self):
            return FilesetChange.remove_property(self._property)

    @dataclass
    class RemoveFilesetCommentRequest(FilesetUpdateRequestBase):
        """Represents a request to remove comment from a Fileset.

        Deprecated:
            Please use `UpdateFilesetCommentRequest` with null value as the argument instead.
        """

        def __init__(self):
            super().__init__("removeComment")

        def validate(self):
            """Validates the fields of the request.

            always pass
            """
            pass

        def fileset_change(self):
            return FilesetChange.remove_comment()
