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
from typing import Optional

from dataclasses_json import config, dataclass_json

from gravitino.api.domain.domain_change import DomainChange
from gravitino.rest.rest_message import RESTRequest
from gravitino.utils.precondition import Precondition


@dataclass
@dataclass_json
class DomainUpdateRequestBase(RESTRequest, ABC):
    _type: str = field(init=False, metadata=config(field_name="@type"))

    @abstractmethod
    def domain_change(self) -> DomainChange:
        """
        Returns the domain change.

        Raises:
            NotImplementedError: if the method is not implemented.

        Returns:
            DomainUpdateRequestBase: the domain change.
        """
        raise NotImplementedError()


class DomainUpdateRequest:
    """Request to update a domain."""

    @dataclass_json
    @dataclass
    class RenameDomainRequest(DomainUpdateRequestBase):
        """The domain update request for renaming a domain."""

        _new_name: str = field(init=True, metadata=config(field_name="newName"))

        def __post_init__(self) -> None:
            self._type = "rename"

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            Precondition.check_argument(
                isinstance(self._new_name, str) and bool(self._new_name.strip()),
                '"newName" must not be blank',
            )

        @property
        def new_name(self) -> str:
            return self._new_name

        def domain_change(self) -> DomainChange.RenameDomain:
            return DomainChange.rename(self._new_name)

    @dataclass_json
    @dataclass
    class UpdateDomainCommentRequest(DomainUpdateRequestBase):
        """The domain update request for updating a domain comment."""

        _new_comment: Optional[str] = field(metadata=config(field_name="newComment"))

        def __post_init__(self) -> None:
            self._type = "updateComment"

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            # always pass
            pass

        @property
        def new_comment(self) -> Optional[str]:
            return self._new_comment

        def domain_change(self) -> DomainChange.UpdateDomainComment:
            return DomainChange.UpdateDomainComment(self._new_comment)

    @dataclass_json
    @dataclass
    class SetDomainPropertyRequest(DomainUpdateRequestBase):
        """The domain update request for setting a domain property."""

        _property: str = field(metadata=config(field_name="property"))
        _value: str = field(metadata=config(field_name="value"))

        def __post_init__(self) -> None:
            self._type = "setProperty"

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            Precondition.check_argument(
                isinstance(self._property, str) and bool(self._property.strip()),
                '"property" must not be blank',
            )
            Precondition.check_argument(
                self._value is not None, '"value" must not be None'
            )

        @property
        def prop(self) -> str:
            return self._property

        @property
        def value(self) -> str:
            return self._value

        def domain_change(self) -> DomainChange.SetProperty:
            return DomainChange.set_property(self._property, self._value)

    @dataclass_json
    @dataclass
    class RemoveDomainPropertyRequest(DomainUpdateRequestBase):
        """The domain update request for removing a domain property."""

        _property: str = field(metadata=config(field_name="property"))

        def __post_init__(self) -> None:
            self._type = "removeProperty"

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            Precondition.check_argument(
                isinstance(self._property, str) and bool(self._property.strip()),
                '"property" must not be blank',
            )

        @property
        def prop(self) -> str:
            return self._property

        def domain_change(self) -> DomainChange.RemoveProperty:
            return DomainChange.remove_property(self._property)
