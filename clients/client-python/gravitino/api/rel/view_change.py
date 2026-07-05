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
from dataclasses import dataclass
from typing import Optional

from gravitino.api.rel.column import Column
from gravitino.api.rel.representation import Representation
from gravitino.utils.precondition import Precondition


class ViewChange(ABC):
    """Defines changes that can be applied to a view."""

    @staticmethod
    def rename(new_name: str) -> "RenameView":
        """Create a change for renaming a view."""
        return RenameView(new_name)

    @staticmethod
    def set_property(property_name: str, value: str) -> "SetProperty":
        """Create a change for setting a view property."""
        return SetProperty(property_name, value)

    @staticmethod
    def remove_property(property_name: str) -> "RemoveProperty":
        """Create a change for removing a view property."""
        return RemoveProperty(property_name)

    @staticmethod
    def replace_view(
        columns: list[Column],
        representations: list[Representation],
        default_catalog: Optional[str] = None,
        default_schema: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> "ReplaceView":
        """Create a change for replacing the view body."""
        return ReplaceView(
            columns, representations, default_catalog, default_schema, comment
        )


@dataclass(frozen=True)
class RenameView(ViewChange):
    """A ViewChange to rename a view."""

    _new_name: str

    def __post_init__(self):
        Precondition.check_string_not_empty(
            self._new_name, "newName must not be null or empty"
        )

    def new_name(self) -> str:
        """Returns the new view name."""
        return self._new_name


@dataclass(frozen=True)
class SetProperty(ViewChange):
    """A ViewChange to set a view property."""

    _property: str
    _value: str

    def __post_init__(self):
        Precondition.check_string_not_empty(
            self._property, "property must not be null or empty"
        )

    def property(self) -> str:
        """Returns the property name."""
        return self._property

    def value(self) -> str:
        """Returns the property value."""
        return self._value


@dataclass(frozen=True)
class RemoveProperty(ViewChange):
    """A ViewChange to remove a view property."""

    _property: str

    def __post_init__(self):
        Precondition.check_string_not_empty(
            self._property, "property must not be null or empty"
        )

    def property(self) -> str:
        """Returns the property name."""
        return self._property


@dataclass(frozen=True)
class ReplaceView(ViewChange):
    """A ViewChange to replace the view body."""

    _columns: list[Column]
    _representations: list[Representation]
    _default_catalog: Optional[str] = None
    _default_schema: Optional[str] = None
    _comment: Optional[str] = None

    def __post_init__(self):
        Precondition.check_argument(
            self._columns is not None, "columns must not be null"
        )
        Precondition.check_argument(
            self._representations is not None and len(self._representations) > 0,
            "representations must not be null or empty",
        )

    def columns(self) -> list[Column]:
        """Returns the new output columns."""
        return self._columns

    def representations(self) -> list[Representation]:
        """Returns the new representations."""
        return self._representations

    def default_catalog(self) -> Optional[str]:
        """Returns the new default catalog."""
        return self._default_catalog

    def default_schema(self) -> Optional[str]:
        """Returns the new default schema."""
        return self._default_schema

    def comment(self) -> Optional[str]:
        """Returns the new comment."""
        return self._comment
