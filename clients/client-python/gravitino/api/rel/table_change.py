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
from typing import Optional, cast, final

from dataclasses_json import config

from gravitino.api.rel.column import Column
from gravitino.api.rel.expressions.expression import Expression
from gravitino.api.rel.indexes.index import Index
from gravitino.api.rel.types.type import Type


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

    @staticmethod
    def update_column_default_value(
        field_name: list[str], new_default_value: Expression
    ) -> "UpdateColumnDefaultValue":
        """Create a `TableChange` for updating the default value of a field.

        The name is used to find the field to update.

        If the field does not exist, the change will result in an `IllegalArgumentException`.

        Args:
            field_name (list[str]): The field name of the column to update.
            new_default_value (Expression): The new default value.

        Returns:
            TableChange: A `TableChange` for the update.
        """
        return UpdateColumnDefaultValue(field_name, new_default_value)

    @staticmethod
    def update_column_type(
        field_name: list[str], new_data_type: Type
    ) -> "UpdateColumnType":
        """Create a `TableChange` for updating the type of a field that is nullable.

        The field name are used to find the field to update.

        If the field does not exist, the change will result in an `IllegalArgumentException`.

        Args:
            field_name (list[str]): The field name of the column to update.
            new_data_type (Type): The new data type.

        Returns:
            TableChange: A `TableChange` for the update.
        """
        return UpdateColumnType(field_name, new_data_type)

    @staticmethod
    def update_column_comment(
        field_name: list[str], new_comment: str
    ) -> "UpdateColumnComment":
        """Create a `TableChange` for updating the comment of a field.

        The name is used to find the field to update.

        If the field does not exist, the change will result in an `IllegalArgumentException`.

        Args:
            field_name (list[str]): The field name of the column to update.
            new_comment (str): The new comment.

        Returns:
            TableChange: A `TableChange` for the update.
        """
        return UpdateColumnComment(field_name, new_comment)

    @staticmethod
    def update_column_position(
        field_name: list[str], new_position: "TableChange.ColumnPosition"
    ) -> "UpdateColumnPosition":
        """Create a `TableChange` for updating the position of a field.

        The name is used to find the field to update.

        If the field does not exist, the change will result in an `IllegalArgumentException`.

        Args:
            field_name (list[str]): The field name of the column to update.
            new_position (TableChange.ColumnPosition): The new position.

        Returns:
            TableChange: A `TableChange` for the update.
        """
        return UpdateColumnPosition(field_name, new_position)

    @staticmethod
    def delete_column(field_name: list[str], if_exists: bool) -> "DeleteColumn":
        """Create a `TableChange` for deleting a field.

        If the field does not exist, the change will result in an `IllegalArgumentException`
        unless `if_exists` is true.

        Args:
            field_name (list[str]): Field name of the column to delete.
            if_exists (bool): If true, silence the error if column does not exist during drop.
                Otherwise, an `IllegalArgumentException` will be thrown.

        Returns:
            TableChange: A `TableChange` for the delete.
        """
        return DeleteColumn(field_name, if_exists)

    @staticmethod
    def update_column_nullability(
        field_name: list[str], nullable: bool
    ) -> "UpdateColumnNullability":
        """Create a `TableChange` for updating the nullability of a field.

        The name is used to find the field to update.

        If the field does not exist, the change will result in an `IllegalArgumentException`.

        Args:
            field_name (list[str]): The field name of the column to update.
            nullable (bool): The new nullability.

        Returns:
            TableChange: A `TableChange` for the update.
        """
        return UpdateColumnNullability(field_name, nullable)

    @staticmethod
    def add_index(
        index_type: Index.IndexType,
        name: str,
        field_names: list[list[str]],
    ) -> "AddIndex":
        """Create a `TableChange` for adding an index.

        Args:
            index_type (Index.IndexType): The type of the index.
            name (str): The name of the index.
            field_names (list[list[str]]): The field names of the index.

        Returns:
            TableChange: A `TableChange` for the add index.
        """
        return TableChange.AddIndex(index_type, name, field_names)

    @staticmethod
    def delete_index(name: str, if_exists: bool) -> "DeleteIndex":
        """Create a `TableChange` for deleting an index.

        Args:
            name (str): The name of the index to be dropped.
            if_exists (bool): If true, silence the error if column does not exist during drop.
                Otherwise, an `IllegalArgumentException` will be thrown.

        Returns:
            TableChange: A `TableChange` for the delete index.
        """
        return TableChange.DeleteIndex(name, if_exists)

    @staticmethod
    def update_column_auto_increment(
        field_name: list[str], auto_increment: bool
    ) -> "UpdateColumnAutoIncrement":
        """Create a `TableChange` for updating the autoIncrement of a field.

        The name is used to find the field to update.

        If the field does not exist, the change will result in an `IllegalArgumentException`.

        Args:
            field_name (list[str]): The field name of the column to update.
            auto_increment (bool): The new autoIncrement.

        Returns:
            TableChange: A `TableChange` for the update.
        """
        return UpdateColumnAutoIncrement(field_name, auto_increment)

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

        def __eq__(self, value: object) -> bool:
            if not isinstance(value, TableChange.RenameTable):
                return False
            other = cast(TableChange.RenameTable, value)
            return self._new_name == other.get_new_name()

        def __hash__(self) -> int:
            return hash(self._new_name)

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

        def __eq__(self, value: object) -> bool:
            if not isinstance(value, TableChange.UpdateComment):
                return False
            other = cast(TableChange.UpdateComment, value)
            return self._new_comment == other.get_new_comment()

        def __hash__(self) -> int:
            return hash(self._new_comment)

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

        def __eq__(self, value: object) -> bool:
            if not isinstance(value, TableChange.SetProperty):
                return False
            other = cast(TableChange.SetProperty, value)
            return (
                self._property == other.get_property()
                and self._value == other.get_value()
            )

        def __hash__(self) -> int:
            return hash((self._property, self._value))

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

        def __eq__(self, value: object) -> bool:
            if not isinstance(value, TableChange.RemoveProperty):
                return False
            other = cast(TableChange.RemoveProperty, value)
            return self._property == other.get_property()

        def __hash__(self) -> int:
            return hash(self._property)

    @final
    @dataclass(frozen=True)
    class AddIndex:
        """A TableChange to add an index.

        Add an index key based on the type and field name passed in as well as the name.
        """

        _type: Index.IndexType = field(metadata=config(field_name="type"))
        _name: str = field(metadata=config(field_name="name"))
        _field_names: list[list[str]] = field(metadata=config(field_name="field_names"))

        def get_type(self) -> Index.IndexType:
            """Retrieves the type of the index.

            Returns:
                IndexType: The type of the index.
            """
            return self._type

        def get_name(self) -> str:
            """Retrieves the name of the index.

            Returns:
                str: The name of the index.
            """
            return self._name

        def get_field_names(self) -> list[list[str]]:
            """Retrieves the field names of the index.

            Returns:
                list[list[str]]: The field names of the index.
            """
            return self._field_names

        def __eq__(self, value: object) -> bool:
            if not isinstance(value, TableChange.AddIndex):
                return False
            other = cast(TableChange.AddIndex, value)
            return (
                self._type == other.get_type()
                and self._name == other.get_name()
                and self._field_names == other.get_field_names()
            )

        def __hash__(self) -> int:
            return 31 * hash((self._type, self._name)) + hash(
                tuple(tuple(field_name) for field_name in self._field_names)
            )

    @final
    @dataclass(frozen=True)
    class DeleteIndex:
        """A `TableChange` to delete an index.

        If the index does not exist, the change must result in an `IllegalArgumentException`.
        """

        _name: str = field(metadata=config(field_name="name"))
        _if_exists: bool = field(metadata=config(field_name="if_exists"))

        def get_name(self) -> str:
            """Retrieves the name of the index to be deleted.

            Returns:
                str: The name of the index to be deleted.
            """
            return self._name

        def is_if_exists(self) -> bool:
            """Retrieves the value of the `if_exists` flag.

            Returns:
                bool: True if the index should be deleted if it exists, False otherwise.
            """
            return self._if_exists

        def __eq__(self, value: object) -> bool:
            if not isinstance(value, TableChange.DeleteIndex):
                return False
            other = cast(TableChange.DeleteIndex, value)
            return (
                self._name == other.get_name()
                and self._if_exists == other.is_if_exists()
            )

        def __hash__(self) -> int:
            return hash((self._name, self._if_exists))

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

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, After):
            return False
        other = cast(After, value)
        return self._column == other.get_column()

    def __hash__(self) -> int:
        return hash(self._column)


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

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, AddColumn):
            return False
        other = cast(AddColumn, value)
        return (
            self._nullable == other.is_nullable()
            and self._auto_increment == other.is_auto_increment()
            and self._field_name == other.get_field_name()
            and self._comment == other.get_comment()
            and self._data_type == other.get_data_type()
            and self._position == other.get_position()
            and self.get_default_value() == other.get_default_value()
        )

    def __hash__(self) -> int:
        return 31 * hash(
            (
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
        ) + hash(tuple(self._field_name))


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

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, RenameColumn):
            return False
        other = cast(RenameColumn, value)
        return (
            self._field_name == other.get_field_name()
            and self._new_name == other.get_new_name()
        )

    def __hash__(self) -> int:
        return 31 * hash(self._new_name) + hash(tuple(self._field_name))


@final
@dataclass(frozen=True)
class UpdateColumnDefaultValue(TableChange.ColumnChange):
    """A `TableChange` to update the default value of a field.

    The field names are used to find the field to update.

    If the field does not exist, the change must result in an `IllegalArgumentException`.
    """

    _field_name: list[str] = field(metadata=config(field_name="field_name"))
    _new_default_value: Optional[Expression] = field(
        metadata=config(field_name="new_default_value")
    )

    def field_name(self) -> list[str]:
        return self._field_name

    def get_new_default_value(self) -> Expression:
        return (
            self._new_default_value
            if self._new_default_value
            else Column.DEFAULT_VALUE_NOT_SET
        )

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, UpdateColumnDefaultValue):
            return False
        other = cast(UpdateColumnDefaultValue, value)
        return (
            self._field_name == other.field_name()
            and self.get_new_default_value() == other.get_new_default_value()
        )

    def __hash__(self) -> int:
        return 31 * hash(
            tuple(self.get_new_default_value())
            if self._new_default_value == Column.DEFAULT_VALUE_NOT_SET
            else self._new_default_value
        ) + hash(tuple(self._field_name))


@final
@dataclass(frozen=True)
class UpdateColumnType(TableChange.ColumnChange):
    """A `TableChange` to update the type of a field.

    The field names are used to find the field to update.

    If the field does not exist, the change must result in an `IllegalArgumentException`.
    """

    _field_name: list[str] = field(metadata=config(field_name="field_name"))
    _new_data_type: Type = field(metadata=config(field_name="new_data_type"))

    def field_name(self) -> list[str]:
        return self._field_name

    def get_field_name(self) -> list[str]:
        return self._field_name

    def get_new_data_type(self) -> Type:
        return self._new_data_type

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, UpdateColumnType):
            return False
        other = cast(UpdateColumnType, value)
        return (
            self._field_name == other.get_field_name()
            and self._new_data_type == other.get_new_data_type()
        )

    def __hash__(self) -> int:
        return 31 * hash(self._new_data_type) + hash(tuple(self._field_name))


@final
@dataclass(frozen=True)
class UpdateColumnComment(TableChange.ColumnChange):
    """A `TableChange` to update the comment of a field.

    The field names are used to find the field to update.

    If the field does not exist, the change must result in an `IllegalArgumentException`.
    """

    _field_name: list[str] = field(metadata=config(field_name="field_name"))
    _new_comment: str = field(metadata=config(field_name="new_comment"))

    def field_name(self) -> list[str]:
        return self._field_name

    def get_field_name(self) -> list[str]:
        return self._field_name

    def get_new_comment(self) -> str:
        return self._new_comment

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, UpdateColumnComment):
            return False
        other = cast(UpdateColumnComment, value)
        return (
            self._field_name == other.get_field_name()
            and self._new_comment == other.get_new_comment()
        )

    def __hash__(self) -> int:
        return 31 * hash(self._new_comment) + hash(tuple(self._field_name))


@final
@dataclass(frozen=True)
class UpdateColumnPosition(TableChange.ColumnChange):
    """A TableChange to update the position of a field.

    The field names are used to find the field to update.

    If the field does not exist, the change must result in an `IllegalArgumentException`.
    """

    _field_name: list[str] = field(metadata=config(field_name="field_name"))
    _position: TableChange.ColumnPosition = field(
        metadata=config(field_name="position")
    )

    def field_name(self) -> list[str]:
        return self._field_name

    def get_field_name(self) -> list[str]:
        """Retrieves the field name of the column whose position is being updated.

        Returns:
            list[str]: A list of strings representing the field name.
        """
        return self._field_name

    def get_position(self) -> TableChange.ColumnPosition:
        """Retrieves the new position for the column.

        Returns:
            TableChange.ColumnPosition: The new position of the column.
        """
        return self._position

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, UpdateColumnPosition):
            return False
        other = cast(UpdateColumnPosition, value)
        return (
            self._field_name == other.get_field_name()
            and self._position == other.get_position()
        )

    def __hash__(self) -> int:
        return 31 * hash(self._position) + hash(tuple(self._field_name))


@final
@dataclass(frozen=True)
class DeleteColumn(TableChange.ColumnChange):
    """A TableChange to delete a field.

    If the field does not exist, the change must result in an `IllegalArgumentException`.
    """

    _field_name: list[str] = field(metadata=config(field_name="field_name"))
    _if_exists: bool = field(metadata=config(field_name="if_exists"))

    def field_name(self) -> list[str]:
        return self._field_name

    def get_field_name(self) -> list[str]:
        """Retrieves the field name of the column to be deleted.

        Returns:
            list[str]: A list of strings representing the field name.
        """
        return self._field_name

    def get_if_exists(self) -> bool:
        """Checks if the field should be deleted only if it exists.

        Returns:
            bool: `True` if the field should be deleted only if it exists; `False` otherwise.
        """
        return self._if_exists

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, DeleteColumn):
            return False
        other = cast(DeleteColumn, value)
        return (
            self._field_name == other.get_field_name()
            and self._if_exists == other.get_if_exists()
        )

    def __hash__(self) -> int:
        return 31 * hash(self._if_exists) + hash(tuple(self._field_name))


@final
@dataclass(frozen=True)
class UpdateColumnNullability(TableChange.ColumnChange):
    """A TableChange to update the nullability of a field.

    The field names are used to find the field to update.

    If the field does not exist, the change must result in an `IllegalArgumentException`.
    """

    _field_name: list[str] = field(metadata=config(field_name="field_name"))
    _nullable: bool = field(metadata=config(field_name="nullable"))

    def field_name(self) -> list[str]:
        return self._field_name

    def get_field_name(self) -> list[str]:
        """Retrieves the field name of the column whose nullability is being updated.

        Returns:
            list[str]: A list of strings representing the field name.
        """
        return self._field_name

    def get_nullable(self) -> bool:
        """Checks if the column is nullable.

        Returns:
            bool: `True` if the column is nullable; `False` otherwise.
        """
        return self._nullable

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, UpdateColumnNullability):
            return False
        other = cast(UpdateColumnNullability, value)
        return (
            self._field_name == other.get_field_name()
            and self._nullable == other.get_nullable()
        )

    def __hash__(self) -> int:
        return 31 * hash(self._nullable) + hash(tuple(self._field_name))


@final
@dataclass(frozen=True)
class UpdateColumnAutoIncrement(TableChange.ColumnChange):
    """A TableChange to update the `autoIncrement` of a field.

    True is to add autoIncrement, false is to delete autoIncrement.
    """

    _field_name: list[str] = field(metadata=config(field_name="field_name"))
    _auto_increment: bool = field(metadata=config(field_name="auto_increment"))

    def field_name(self) -> list[str]:
        return self._field_name

    def is_auto_increment(self) -> bool:
        """Checks if the column is `autoIncrement`.

        Returns:
            bool: `True` if the column is `autoIncrement`; `False` otherwise.
        """
        return self._auto_increment

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, UpdateColumnAutoIncrement):
            return False
        other = cast(UpdateColumnAutoIncrement, value)
        return (
            self._field_name == other.field_name()
            and self._auto_increment == other.is_auto_increment()
        )

    def __hash__(self) -> int:
        return 31 * hash(self._auto_increment) + hash(tuple(self._field_name))
