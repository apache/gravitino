"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

from dataclasses import dataclass, field

from dataclasses_json import config


class MetalakeChange:
    """A metalake change is a change to a metalake. It can be used to rename a metalake, update the
    comment of a metalake, set a property and value pair for a metalake, or remove a property from a
    metalake.
    """

    @staticmethod
    def rename(new_name: str) -> "MetalakeChange.RenameMetalake":
        """Creates a new metalake change to rename the metalake.

        Args:
            new_name: The New name of the metalake.

        Returns:
            The metalake change.
        """
        return MetalakeChange.RenameMetalake(new_name)

    @staticmethod
    def update_comment(new_comment: str) -> "MetalakeChange.UpdateMetalakeComment":
        """Creates a new metalake change to update the metalake comment.

        Args:
            new_comment: The new comment of the metalake.

        Returns:
            The metalake change.
        """
        return MetalakeChange.UpdateMetalakeComment(new_comment)

    @staticmethod
    def set_property(metalake_property: str, value: str) -> "SetProperty":
        """Creates a new metalake change to set a property and value pair for the metalake.

        Args:
            metalake_property: The property name to set.
            value: The value to set the property to.

        Returns:
             The metalake change.
        """
        return MetalakeChange.SetProperty(metalake_property, value)

    @staticmethod
    def remove_property(metalake_property: str) -> "RemoveProperty":
        """Creates a new metalake change to remove a property from the metalake.

        Args:
            metalake_property: The property name to remove.

        Returns:
            The metalake change.
        """
        return MetalakeChange.RemoveProperty(metalake_property)

    @dataclass(frozen=True)
    class RenameMetalake:
        """A metalake change to rename the metalake."""

        _new_name: str = field(metadata=config(field_name="new_name"))

        def new_name(self) -> str:
            return self._new_name

        def __str__(self):
            return f"RENAMEMETALAKE {self._new_name}"

    @dataclass(frozen=True)
    class UpdateMetalakeComment:
        """A metalake change to update the metalake comment"""

        _new_comment: str = field(metadata=config(field_name="new_comment"))

        def new_comment(self) -> str:
            return self._new_comment

        def __str__(self):
            return f"UPDATEMETALAKECOMMENT {self._new_comment}"

    @dataclass(frozen=True)
    class SetProperty:
        """A metalake change to set a property and value pair for the metalake"""

        _property: str = field(metadata=config(field_name="property"))
        _value: str = field(metadata=config(field_name="value"))

        def property(self) -> str:
            return self._property

        def value(self) -> str:
            return self._value

        def __str__(self):
            return f"SETPROPERTY {self._property} {self._value}"

    @dataclass(frozen=True)
    class RemoveProperty:
        """A metalake change to remove a property from the metalake"""

        _property: str

        def property(self) -> str:
            return self._property

        def __str__(self):
            return f"REMOVEPROPERTY {self.property}"
