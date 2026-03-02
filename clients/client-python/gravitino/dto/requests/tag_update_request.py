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

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from dataclasses_json import config, dataclass_json

from gravitino.api.tag.tag_change import TagChange
from gravitino.rest.rest_message import RESTRequest


@dataclass
@dataclass_json
class TagUpdateRequestBase(RESTRequest, ABC):
    @abstractmethod
    def tag_change(self) -> TagChange:
        """
        Returns the tag change.

        Raises:
            NotImplementedError: if the method is not implemented.

        Returns:
            TagUpdateRequestBase: the tag change.
        """
        raise NotImplementedError()


class TagUpdateRequest:
    """Request to update a tag."""

    @dataclass_json
    @dataclass
    class RenameTagRequest(TagUpdateRequestBase):
        """The tag update request for renaming a tag."""

        _type: str = field(
            init=False, default="rename", metadata=config(field_name="@type")
        )

        _new_name: str = field(init=True, metadata=config(field_name="newName"))

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            if not self._new_name:
                raise ValueError('"newName" must not be blank')

        def new_name(self) -> str:
            return self._new_name

        def tag_change(self) -> TagChange.RenameTag:
            return TagChange.rename(self._new_name)

    @dataclass_json
    @dataclass
    class UpdateTagCommentRequest(TagUpdateRequestBase):
        """The tag update request for updating a tag comment."""

        _type: str = field(
            init=False, default="updateComment", metadata=config(field_name="@type")
        )

        _new_comment: str = field(metadata=config(field_name="newComment"))

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            # always pass
            pass

        def new_comment(self) -> str:
            return self._new_comment

        def tag_change(self) -> TagChange.UpdateTagComment:
            return TagChange.UpdateTagComment(self._new_comment)

    @dataclass_json
    @dataclass
    class SetTagPropertyRequest(TagUpdateRequestBase):
        """The tag update request for setting a tag property."""

        _type: str = field(
            init=False, default="setProperty", metadata=config(field_name="@type")
        )

        _property: str = field(metadata=config(field_name="property"))
        _value: str = field(metadata=config(field_name="value"))

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            if not self._property:
                raise ValueError('"property" must not be blank')

            if not self._value:
                raise ValueError('"value" must not be blank')

        def get_prop(self) -> str:
            return self._property

        def get_value(self) -> str:
            return self._value

        def tag_change(self) -> TagChange.SetProperty:
            return TagChange.set_property(self._property, self._value)

    @dataclass_json
    @dataclass
    class RemoveTagPropertyRequest(TagUpdateRequestBase):
        """The tag update request for removing a tag property."""

        _type: str = field(
            init=False, default="removeProperty", metadata=config(field_name="@type")
        )

        _property: str = field(metadata=config(field_name="property"))

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            if not self._property:
                raise ValueError('"property" must not be blank')

        def get_prop(self) -> str:
            return self._property

        def tag_change(self) -> TagChange.RemoveProperty:
            return TagChange.remove_property(self._property)
