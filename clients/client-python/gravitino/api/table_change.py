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

from abc import ABC
from dataclasses import dataclass, field
from typing import final

from dataclasses_json import config


class TableChange(ABC):
    """Defines the public APIs for managing tables in a schema.

    The `TableChange` interface defines the public API for managing tables in a schema.
    If the catalog implementation supports tables, it must implement this interface.
    """

    @staticmethod
    def rename(new_name: str) -> "RenameTable":
        """Create a `TableChange` for renaming a table.

        Args:
            new_name: The new table name.

        Returns:
            RenameTable: A `TableChange` for the rename.
        """
        return TableChange.RenameTable(new_name)

    @staticmethod
    def update_comment(new_comment: str) -> "UpdateComment":
        """Create a `TableChange` for updating the comment.

        Args:
            new_comment: The new comment.

        Returns:
            UpdateComment: A `TableChange` for the update.
        """
        return TableChange.UpdateComment(new_comment)

    @staticmethod
    def set_property(property_name: str, value: str) -> "SetProperty":
        """Create a `TableChange` for setting a table property.

        If the property already exists, it will be replaced with the new value.

        Args:
            property_name (str): The property name.
            value (str): The new property value.

        Returns:
            SetProperty: A `TableChange` for the addition.
        """
        return TableChange.SetProperty(property_name, value)

    @final
    @dataclass(frozen=True)
    class RenameTable:
        """A `TableChange` to rename a table."""

        _new_name: str = field(metadata=config(field_name="new_name"))

        def get_new_name(self) -> str:
            """Retrieves the new name for the table.

            Returns:
                str: The new name of the table.
            """
            return self._new_name

        def __str__(self):
            return f"RENAMETABLE {self._new_name}"

    @final
    @dataclass(frozen=True)
    class UpdateComment:
        """A `TableChange` to update a table's comment."""

        _new_comment: str = field(metadata=config(field_name="new_comment"))

        def get_new_comment(self) -> str:
            """Retrieves the new comment for the table.

            Returns:
                str: The new comment of the table.
            """
            return self._new_comment

        def __str__(self):
            return f"UPDATECOMMENT {self._new_comment}"

    @final
    @dataclass(frozen=True)
    class SetProperty:
        """A `TableChange` to set a table property."""

        _property: str = field(metadata=config(field_name="property"))
        _value: str = field(metadata=config(field_name="value"))

        def get_property(self) -> str:
            """Retrieves the name of the property.

            Returns:
                str: The name of the property.
            """
            return self._property

        def get_value(self) -> str:
            """Retrieves the value of the property.

            Returns:
                str: The value of the property.
            """
            return self._value

        def __str__(self):
            return f"SETPROPERTY {self._property} {self._value}"
