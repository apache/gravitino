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

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, final

from dataclasses_json import config

from gravitino.api.column import Column
from gravitino.api.expressions.expression import Expression
from gravitino.api.types.type import Type


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

    @staticmethod
    def remove_property(property_name: str) -> "RemoveProperty":
        """Create a `TableChange` for removing a table property.

        If the property does not exist, the change will succeed.

        Args:
            property_name (str): The property name.

        Returns:
            RemoveProperty: A `TableChange` for the addition.
        """
        return TableChange.RemoveProperty(property_name)

    @staticmethod
    def add_column(
        field_name: list[str],
        data_type: Type,
        comment: Optional[str] = None,
        position: Optional["TableChange.ColumnPosition"] = None,
        nullable: bool = True,
        auto_increment: bool = False,
        default_value: Optional[Expression] = None,
    ) -> "AddColumn":
        """Create a `TableChange` for adding a column.

        Args:
            field_name (list[str]):
                Field name of the new column.
            data_type (Type):
                The new column's data type.
            comment (Optional[str]):
                The new field's comment string, defaults to `None`.
            position (Optional[TableChange.ColumnPosition]):
                The new column's position, defaults to `None`.
            nullable (bool):
                The new column's nullable.
            auto_increment (bool):
                The new column's autoIncrement.
            default_value (Expression):
                The new column's default value.

        Returns:
            AddColumn: A `TableChange` for the addition.
        """
        return AddColumn(
            field_name,
            data_type,
            comment,
            position,
            nullable,
            auto_increment,
            default_value,
        )

    @staticmethod
    def rename_column(field_name: list[str], new_name: str) -> "RenameColumn":
        """Create a `TableChange` for renaming a field.

        The name is used to find the field to rename. The new name will replace the **leaf field
        name**. For example, `rename_column(["a", "b", "c"], "x")` should produce column **a.b.x**.

        If the field does not exist, the change will result in an `IllegalArgumentException`.

        Args:
            field_name (list[str]): The current field name.
            new_name (str): The new name.

        Returns:
            RenameColumn: A TableChange for the rename.
        """
        return RenameColumn(field_name, new_name)

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

    @final
    @dataclass(frozen=True)
    class RemoveProperty:
        """A `TableChange` to remove a table property.

        If the property does not exist, the change should succeed.
        """

        _property: str = field(metadata=config(field_name="property"))

        def get_property(self) -> str:
            """Retrieves the name of the property to be removed from the table.

            Returns:
                str: The name of the property scheduled for removal.
            """
            return self._property

        def __str__(self):
            return f"REMOVEPROPERTY {self._property}"

    class ColumnPosition(ABC):
        """The interface for all column positions.

        Column positions are used to specify the position of a column when adding
        a new column to a table.
        """

        @staticmethod
        def first() -> "First":
            """The first position of `ColumnPosition` instance.

            Returns:
                First:
                    The first position of `ColumnPosition` instance.
            """
            return First()

        @staticmethod
        def after(column: str) -> "After":
            """Returns the position after the given column.

            Args:
                column:
                    The name of the reference column to place the new column after.

            Returns:
                After:
                    The position after the given column.
            """
            return After(column)

        @staticmethod
        def default_pos() -> "Default":
            """Returns the default position of `ColumnPosition` instance.

            Returns:
                Default:
                    The default position of `ColumnPosition` instance.
            """
            return Default()

    class ColumnChange(ABC):
        """The interface for all column changes.

        Column changes are used to modify the schema of a table.
        """

        @abstractmethod
        def field_name(self) -> list[str]:
            """Retrieves the field name of the column to be modified.

            Returns:
                list[str]:
                    A list of strings representing the field name.
            """


@final
@dataclass(frozen=True)
class First(TableChange.ColumnPosition):
    """Column position FIRST.

    It means the specified column should be the first column. Note that, the specified column
    may be a nested field, and then FIRST means this field should be the first one within the
    struct.
    """

    def __str__(self):
        return "FIRST"


@final
@dataclass(frozen=True)
class After(TableChange.ColumnPosition):
    """Column position AFTER

    It means the specified column should be put after the given `column`.
    Note that, the specified column may be a nested field, and then the given `column`
    refers to a field in the same struct.
    """

    _column: str = field(metadata=config(field_name="column"))

    def get_column(self) -> str:
        """Retrieves the name of the reference column after which the specified column will be placed.

        Returns:
            str: The name of the reference column.
        """
        return self._column

    def __str__(self):
        return f"AFTER {self._column}"


@final
@dataclass(frozen=True)
class Default(TableChange.ColumnPosition):
    """Column position DEFAULT.

    It means the position of the column was ignored by the user, and should be determined
    by the catalog implementation.
    """

    def __str__(self):
        return "DEFAULT"


@final
@dataclass(frozen=True)
class AddColumn(TableChange.ColumnChange):
    """A TableChange to add a field.

    The implementation may need to back-fill all the existing data to add this new column,
    or remember the column default value specified here and let the reader fill the column value
    when reading existing data that do not have this new column.

    If the field already exists, the change must result in an `IllegalArgumentException`.
    If the new field is nested and its parent does not exist or is not a struct, the change must
    result in an `IllegalArgumentException`.
    """

    _field_name: list[str] = field(metadata=config(field_name="field_name"))
    _data_type: Type = field(metadata=config(field_name="data_type"))
    _comment: Optional[str] = field(default=None, metadata=config(field_name="comment"))
    _position: Optional[TableChange.ColumnPosition] = field(
        default=None, metadata=config(field_name="position")
    )
    _nullable: bool = field(default=True, metadata=config(field_name="nullable"))
    _auto_increment: bool = field(
        default=False, metadata=config(field_name="auto_increment")
    )
    _default_value: Optional[Expression] = field(
        default=None,
        metadata=config(field_name="default_value"),
    )

    def get_field_name(self) -> list[str]:
        """Retrieves the field name of the new column.

        Returns:
            list[str]: The field name of the new column.
        """
        return self._field_name

    def get_data_type(self) -> Type:
        """Retrieves the data type of the new column.

        Returns:
            Type: The data type of the new column.
        """
        return self._data_type

    def get_comment(self) -> Optional[str]:
        """Retrieves the comment for the new column.

        Returns:
            str: comment for the new column.
        """
        return self._comment

    def get_position(self) -> Optional[TableChange.ColumnPosition]:
        """Retrieves the position where the new column should be added.

        Returns:
            TableChange.ColumnPosition:
                The position of the column.
        """
        return self._position

    def is_nullable(self) -> bool:
        """Checks if the new column is nullable.

        Returns:
            bool: `True` if the column is nullable; `False` otherwise.
        """
        return self._nullable

    def is_auto_increment(self) -> bool:
        """Checks if the new column is auto-increment.

        Returns:
            bool: `True` if the column is auto-increment; `False` otherwise.
        """
        return self._auto_increment

    def get_default_value(self) -> Expression:
        """Retrieves the default value of the new column.

        Returns:
            Expression: The default value of the column.
        """
        return (
            self._default_value if self._default_value else Column.DEFAULT_VALUE_NOT_SET
        )

    def field_name(self) -> list[str]:
        return self._field_name

    def __hash__(self) -> int:
        base_tuple = (
            self._data_type,
            self._comment,
            self._position,
            self._nullable,
            self._auto_increment,
            (
                tuple(self._default_value)
                if self._default_value == Column.DEFAULT_VALUE_NOT_SET
                else self._default_value
            ),
        )
        result = hash(base_tuple)
        return 31 * result + hash(tuple(self._field_name))


@final
@dataclass(frozen=True)
class RenameColumn(TableChange.ColumnChange):
    """A `TableChange` to rename a field.

    The name is used to find the field to rename. The new name will replace the **leaf field name**.
    For example, `rename_column("a.b.c", "x")` should produce column **a.b.x**.

    If the field does not exist, the change must result in an `IllegalArgumentException`.
    """

    _field_name: list[str] = field(metadata=config(field_name="field_name"))
    _new_name: str = field(metadata=config(field_name="new_name"))

    def get_field_name(self) -> list[str]:
        """Retrieves the hierarchical field name of the column to be renamed.

        Returns:
            list[str]: The field name of the column to be renamed.
        """
        return self._field_name

    def get_new_name(self) -> str:
        """Retrieves the new name for the column.

        Returns:
            str: The new name of the column.
        """
        return self._new_name

    def field_name(self) -> list[str]:
        return self._field_name

    def __hash__(self) -> int:
        return hash((tuple(self._field_name), self._new_name))
