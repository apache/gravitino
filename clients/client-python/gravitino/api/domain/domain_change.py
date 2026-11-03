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

from abc import ABC
from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import config


class DomainChange(ABC):
    """Interface for supporting domain changes.
    This interface will be used to provide domain modification operations for each domain.
    """

    @staticmethod
    def rename(new_name: str) -> DomainChange.RenameDomain:
        """
        Create a domain change instance to rename the domain.

        Args:
            new_name (str): The new name of the domain.

        Returns:
            RenameDomain: A domain change instance to rename the domain.
        """
        return DomainChange.RenameDomain(new_name)

    @staticmethod
    def update_comment(new_comment: Optional[str]) -> DomainChange.UpdateDomainComment:
        """
        Create a domain change instance to update the domain comment.

        Args:
            new_comment (str): The new comment of the domain.

        Returns:
            UpdateDomainComment: A domain change instance to update the domain comment.
        """
        return DomainChange.UpdateDomainComment(new_comment)

    @staticmethod
    def set_property(
        domain_property: str, domain_value: str
    ) -> DomainChange.SetProperty:
        """
        Creates a new domain change instance to set the property and value for the domain.

        Args:
            domain_property (str): The property to set.
            domain_value (str): The value to set.

        Returns:
            SetProperty: The domain change instance to set the property and value for the domain.
        """
        return DomainChange.SetProperty(domain_property, domain_value)

    @staticmethod
    def remove_property(domain_property: str) -> DomainChange.RemoveProperty:
        """
        Creates a new domain change instance to remove a property from the domain.

        Args:
            domain_property (str): The property to remove.

        Returns:
            RemoveProperty: The domain change instance to remove a property from the domain.
        """
        return DomainChange.RemoveProperty(domain_property)

    @dataclass(frozen=True, eq=True)
    class RenameDomain:
        """A domain change to rename the domain."""

        _new_name: str = field(metadata=config(field_name="newName"))

        @property
        def new_name(self) -> str:
            """The new name of the domain."""
            return self._new_name

        def __str__(self) -> str:
            return f"RENAMEDOMAIN {self._new_name}"

    @dataclass(frozen=True, eq=True)
    class UpdateDomainComment:
        """A domain change to update the domain comment."""

        _new_comment: Optional[str] = field(metadata=config(field_name="newComment"))

        @property
        def new_comment(self) -> Optional[str]:
            """The new comment of the domain."""
            return self._new_comment

        def __str__(self) -> str:
            return f"UPDATEDOMAINCOMMENT {self._new_comment}"

    @dataclass(frozen=True, eq=True)
    class SetProperty:
        """A domain change to set a property-value pair for the domain."""

        _property: str = field(metadata=config(field_name="property"))
        _value: str = field(metadata=config(field_name="value"))

        @property
        def name(self) -> str:
            """The property to set."""
            return self._property

        @property
        def value(self) -> str:
            """The value to set."""
            return self._value

        def __str__(self) -> str:
            return f"SETDOMAINPROPERTY {self._property} = {self._value}"

    @dataclass(frozen=True, eq=True)
    class RemoveProperty:
        """A domain change to remove a property-value pair for the domain."""

        _property: str = field(metadata=config(field_name="property"))

        @property
        def removed_property(self) -> str:
            """The property to remove."""
            return self._property

        def __str__(self) -> str:
            return f"REMOVEDOMAINPROPERTY {self._property}"
