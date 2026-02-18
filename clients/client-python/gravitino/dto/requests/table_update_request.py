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

import typing
from abc import abstractmethod
from dataclasses import dataclass, field

from dataclasses_json import config, dataclass_json

from gravitino.api.rel.expressions.expression import Expression
from gravitino.api.rel.indexes.index import Index
from gravitino.api.rel.indexes.indexes import Indexes
from gravitino.api.rel.table_change import (
    DeleteColumn,
    RenameColumn,
    TableChange,
    UpdateColumnAutoIncrement,
    UpdateColumnComment,
    UpdateColumnDefaultValue,
    UpdateColumnNullability,
    UpdateColumnPosition,
    UpdateColumnType,
)
from gravitino.api.rel.types.json_serdes._helper.serdes_utils import SerdesUtils
from gravitino.api.rel.types.type import Type
from gravitino.rest.rest_message import RESTRequest


@dataclass_json
@dataclass
class TableUpdateRequestBase(RESTRequest):
    """Base class for all table update requests."""

    _type: str = field(metadata=config(field_name="@type"))

    def __init__(self, action_type: str) -> None:
        self._type = action_type

    @abstractmethod
    def table_change(self) -> TableChange:
        """Convert to table change operation"""
        pass


class TableUpdateRequest:
    """Namespace for all table update request types."""

    @dataclass_json
    @dataclass
    class RenameTableRequest(TableUpdateRequestBase):
        """
        Update request to rename a table
        """

        _new_name: str = field(metadata=config(field_name="newName"))

        def __init__(self, new_name: str) -> None:
            """
            Constructor for RenameTableRequest.

            Args:
                new_name (str): the new name of the table
            """
            super().__init__("rename")
            self._new_name = new_name

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            if not self._new_name:
                raise ValueError('"newName" field is required and cannot be empty')

        def get_new_name(self) -> str:
            return self._new_name

        def table_change(self) -> TableChange.RenameTable:
            return TableChange.rename(self._new_name)

    @dataclass_json
    @dataclass
    class UpdateTableCommentRequest(TableUpdateRequestBase):
        """
        Update request to change a table comment
        """

        _new_comment: str = field(metadata=config(field_name="newComment"))

        def __init__(self, new_comment: str) -> None:
            """
            Constructor for UpdateTableCommentRequest.

            Args:
                new_comment (str): the new comment of the table
            """
            super().__init__("updateComment")
            self._new_comment = new_comment

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            # Validates the fields of the request. Always pass.
            pass

        def get_new_comment(self) -> str:
            return self._new_comment

        def table_change(self) -> TableChange.UpdateComment:
            return TableChange.update_comment(self._new_comment)

    @dataclass_json
    @dataclass
    class SetTablePropertyRequest(TableUpdateRequestBase):
        """
        Update request to set a table property
        """

        _property: str = field(metadata=config(field_name="property"))
        _value: str = field(metadata=config(field_name="value"))

        def __init__(self, prop: str, value: str) -> None:
            """
            Constructor for SetTablePropertyRequest.

            Args:
                pro (str): the property to set
                value (str): the value to set
            """
            super().__init__("setProperty")
            self._property = prop
            self._value = value

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            if not self._property:
                raise ValueError('"property" field is required')
            if not self._value:
                raise ValueError('"value" field is required')

        def get_property(self) -> str:
            return self._property

        def get_value(self) -> str:
            return self._value

        def table_change(self) -> TableChange.SetProperty:
            return TableChange.set_property(self._property, self._value)

    @dataclass_json
    @dataclass
    class RemoveTablePropertyRequest(TableUpdateRequestBase):
        """
        Update request to remove a table property
        """

        _property: str = field(metadata=config(field_name="property"))

        def __init__(self, prop: str) -> None:
            """
            Constructor for RemoveTablePropertyRequest.

            Args:
                pro (str): the property to remove
            """
            super().__init__("removeProperty")
            self._property = prop

        def validate(self) -> None:
            """
            Validates the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            if not self._property:
                raise ValueError('"property" field is required')

        def get_property(self) -> str:
            return self._property

        def table_change(self) -> TableChange.RemoveProperty:
            return TableChange.remove_property(self._property)

    @dataclass_json
    @dataclass
    class AddTableColumnRequest(TableUpdateRequestBase):
        """Represents a request to add a column to a table."""

        _field_name: list[str] = field(metadata=config(field_name="fieldName"))
        _data_type: Type = field(
            metadata=config(
                field_name="type",
                encoder=SerdesUtils.write_data_type,
                decoder=SerdesUtils.read_data_type,
            )
        )
        _comment: typing.Optional[str] = field(metadata=config(field_name="comment"))
        _position: typing.Optional[TableChange.ColumnPosition] = field(
            metadata=config(
                field_name="position",
                encoder=SerdesUtils.column_position_encoder,
                decoder=SerdesUtils.column_position_decoder,
            )
        )
        _default_value: typing.Optional[Expression] = field(
            metadata=config(
                field_name="defaultValue",
                encoder=SerdesUtils.column_default_value_encoder,
                decoder=SerdesUtils.column_default_value_decoder,
                exclude=lambda v: v is None,
            )
        )
        _nullable: bool = field(default=True, metadata=config(field_name="nullable"))
        _auto_increment: bool = field(
            default=False, metadata=config(field_name="autoIncrement")
        )

        def __init__(
            self,
            field_name: list[str],
            data_type: Type,
            comment: typing.Optional[str],
            position: TableChange.ColumnPosition,
            default_value: typing.Optional[Expression],
            nullable: bool,
            auto_increment: bool,
        ) -> None:
            """
            Constructor for AddTableColumnRequest.

            Args:
                field_name (list[str]): the field name to add
                data_type (Type): the data type of the field to add
                comment (typing.Optional[str]): the comment of the field to add
                position (TableChange.ColumnPosition): the position of the field to add, null for default position
                default_value (Expression): whether the field has default value
                nullable (bool): whether the field to add is nullable
                auto_increment (bool): whether the field to add is auto increment
            """
            super().__init__("addColumn")
            self._field_name = field_name
            self._data_type = data_type
            self._comment = comment
            self._position = position
            self._default_value = default_value
            self._nullable = nullable
            self._auto_increment = auto_increment

        def validate(self) -> None:
            """
            Validates the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            if not self._field_name:
                raise ValueError("Field name must be specified")

            if any(not name for name in self._field_name):
                raise ValueError('elements in "field_name" cannot be empty')

            if not self._data_type:
                raise ValueError('"type" field is required and cannot be empty')

        def get_field_name(self) -> list[str]:
            return self._field_name

        def get_data_type(self) -> Type:
            return self._data_type

        def get_comment(self) -> typing.Optional[str]:
            return self._comment

        def get_position(self) -> typing.Optional[TableChange.ColumnPosition]:
            return self._position

        def get_default_value(self) -> typing.Optional[Expression]:
            return self._default_value

        def is_nullable(self) -> bool:
            return self._nullable

        def is_auto_increment(self) -> bool:
            return self._auto_increment

        def table_change(self):
            return TableChange.add_column(
                self._field_name,
                self._data_type,
                self._comment,
                self._position,
                self._nullable,
                self._auto_increment,
                self._default_value,
            )

    @dataclass_json
    @dataclass
    class RenameTableColumnRequest(TableUpdateRequestBase):
        """Represents a request to rename a column of a table."""

        _old_field_name: list[str] = field(metadata=config(field_name="oldFieldName"))
        _new_field_name: str = field(metadata=config(field_name="newFieldName"))

        def __init__(self, old_field_name: list[str], new_field_name: str) -> None:
            """
            Constructor for RenameTableColumnRequest.

            Args:
                old_field_name (list[str]): the old field name to rename
                new_field_name (str): the new field name
            """
            super().__init__("renameColumn")
            self._old_field_name = old_field_name
            self._new_field_name = new_field_name

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            if not self._old_field_name:
                raise ValueError(
                    '"old_field_name" field is required and must contain at least one element'
                )

            if any(not name for name in self._old_field_name):
                raise ValueError('elements in "old_field_name" cannot be empty')

            if not self._new_field_name:
                raise ValueError('"newFieldName" field is required and cannot be empty')

        def table_change(self) -> RenameColumn:
            return TableChange.rename_column(self._old_field_name, self._new_field_name)

    @dataclass_json
    @dataclass
    class UpdateTableColumnDefaultValueRequest(TableUpdateRequestBase):
        """Represents a request to update the default value of a column of a table."""

        _field_name: list[str] = field(metadata=config(field_name="fieldName"))
        _new_default_value: Expression = field(
            metadata=config(
                field_name="newDefaultValue",
                encoder=SerdesUtils.column_default_value_encoder,
                decoder=SerdesUtils.column_default_value_decoder,
            )
        )

        def __init__(
            self, field_name: list[str], new_default_value: Expression
        ) -> None:
            """
            Constructor for UpdateTableColumnDefaultValueRequest.

            Args:
                field_name (list[str]): the field name to update
                new_default_value (Expression): the new default value of the field
            """
            super().__init__("updateColumnDefaultValue")
            self._field_name = field_name
            self._new_default_value = new_default_value

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            if not self._field_name:
                raise ValueError(
                    '"field_name" field is required and must contain at least one element'
                )

            if any(not name for name in self._field_name):
                raise ValueError('elements in "field_name" cannot be empty')

            if (
                not self._new_default_value
                or self._new_default_value == Expression.EMPTY_EXPRESSION
            ):
                raise ValueError(
                    '"newDefaultValue" field is required and cannot be empty'
                )

        def get_field_name(self) -> list[str]:
            return self._field_name

        def get_new_default_value(self) -> Expression:
            return self._new_default_value

        def table_change(self) -> UpdateColumnDefaultValue:
            return TableChange.update_column_default_value(
                self._field_name, self._new_default_value
            )

    @dataclass_json
    @dataclass
    class UpdateTableColumnTypeRequest(TableUpdateRequestBase):
        """Represents a request to update the type of a column of a table."""

        _field_name: list[str] = field(metadata=config(field_name="fieldName"))
        _new_type: Type = field(
            metadata=config(
                field_name="newType",
                encoder=SerdesUtils.write_data_type,
                decoder=SerdesUtils.read_data_type,
            )
        )

        def __init__(self, field_name: list[str], new_type: Type) -> None:
            """
            Constructor for UpdateTableColumnTypeRequest.

            Args:
                field_name (list[str]): the field name to update
                new_type (Type): the new type of the field
            """
            super().__init__("updateColumnType")
            self._field_name = field_name
            self._new_type = new_type

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            if not self._field_name:
                raise ValueError(
                    '"field_name" field is required and must contain at least one element'
                )

            if any(not name for name in self._field_name):
                raise ValueError('elements in "field_name" cannot be empty')

            if not self._new_type:
                raise ValueError('"newType" field is required and cannot be null')

        def get_field_name(self) -> list[str]:
            return self._field_name

        def get_new_type(self) -> Type:
            return self._new_type

        def table_change(self) -> UpdateColumnType:
            return TableChange.update_column_type(self._field_name, self._new_type)

    @dataclass_json
    @dataclass
    class UpdateTableColumnCommentRequest(TableUpdateRequestBase):
        """
        Represents a request to update the comment of a column of a table.
        """

        _field_name: list[str] = field(metadata=config(field_name="fieldName"))
        _new_comment: str = field(metadata=config(field_name="newComment"))

        def __init__(self, field_name: list[str], new_comment: str) -> None:
            """
            Constructor for UpdateTableColumnCommentRequest.

            Args:
                field_name (list[str]): the field name to update
                new_comment (str): the new comment of the field
            """
            super().__init__("updateColumnComment")
            self._field_name = field_name
            self._new_comment = new_comment

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            if not self._field_name:
                raise ValueError(
                    '"field_name" field is required and must contain at least one element'
                )

            if any(not name for name in self._field_name):
                raise ValueError('elements in "field_name" cannot be empty')

            if not self._new_comment:
                raise ValueError('"newComment" field is required and cannot be empty')

        def get_field_name(self) -> list[str]:
            return self._field_name

        def get_new_comment(self) -> str:
            return self._new_comment

        def table_change(self) -> UpdateColumnComment:
            return TableChange.update_column_comment(
                self._field_name, self._new_comment
            )

    @dataclass_json
    @dataclass
    class UpdateTableColumnPositionRequest(TableUpdateRequestBase):
        """Represents a request to update the position of a column of a table."""

        _field_name: list[str] = field(metadata=config(field_name="fieldName"))
        _new_position: TableChange.ColumnPosition = field(
            metadata=config(
                field_name="newPosition",
                encoder=SerdesUtils.column_position_encoder,
                decoder=SerdesUtils.column_position_decoder,
            )
        )

        def __init__(
            self,
            field_name: list[str],
            new_position: TableChange.ColumnPosition,
        ) -> None:
            """
            Constructor.

            Args:
                field_name (list[str]): The field name to update
                new_position (TableChange.ColumnPosition): The new position of the field
            """
            super().__init__("updateColumnPosition")
            self._field_name = field_name
            self._new_position = new_position

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            if not self._field_name:
                raise ValueError(
                    '"field_name" field is required and must contain at least one element'
                )

            if any(not name for name in self._field_name):
                raise ValueError('elements in "field_name" cannot be empty')

            if not self._new_position:
                raise ValueError('"newPosition" field is required and cannot be null')

        def field_name(self) -> list[str]:
            return self._field_name

        def new_position(self) -> TableChange.ColumnPosition:
            return self._new_position

        def table_change(self) -> UpdateColumnPosition:
            return TableChange.update_column_position(
                self._field_name, self._new_position
            )

    @dataclass_json
    @dataclass
    class UpdateTableColumnNullabilityRequest(TableUpdateRequestBase):
        """
        Represents a request to update the nullability of a column of a table.
        """

        _field_name: list[str] = field(metadata=config(field_name="fieldName"))
        _nullable: bool = field(metadata=config(field_name="nullable"))

        def __init__(self, field_name: list[str], nullable: bool) -> None:
            """
            Constructor for UpdateTableColumnNullabilityRequest.

            Args:
                field_name (list[str]): the field name to update
                nullable (bool): the new nullability of the field
            """
            super().__init__("updateColumnNullability")
            self._field_name = field_name
            self._nullable = nullable

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            if not self._field_name:
                raise ValueError(
                    '"field_name" field is required and must contain at least one element'
                )

            if any(not name for name in self._field_name):
                raise ValueError('elements in "field_name" cannot be empty')

        def field_name(self) -> list[str]:
            return self._field_name

        def nullable(self) -> bool:
            return self._nullable

        def table_change(self) -> UpdateColumnNullability:
            return TableChange.update_column_nullability(
                self._field_name, self._nullable
            )

    @dataclass_json
    @dataclass
    class DeleteTableColumnRequest(TableUpdateRequestBase):
        """
        Represents a request to delete a column from a table.
        """

        _field_name: list[str] = field(metadata=config(field_name="fieldName"))
        _if_exists: bool = field(metadata=config(field_name="ifExists"))

        def __init__(self, field_name: list[str], if_exists: bool) -> None:
            """
            Constructor for DeleteTableColumnRequest.

            Args:
                field_name (list[str]): the field name to delete
                if_exists (bool): whether to delete the column if it exists
            """
            super().__init__("deleteColumn")
            self._field_name = field_name
            self._if_exists = if_exists

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            if not self._field_name:
                raise ValueError(
                    '"field_name" field is required and must contain at least one element'
                )

            if any(not name for name in self._field_name):
                raise ValueError('elements in "field_name" cannot be empty')

        def get_field_name(self) -> list[str]:
            return self._field_name

        def get_if_exists(self) -> bool:
            return self._if_exists

        def table_change(self) -> DeleteColumn:
            return TableChange.delete_column(self._field_name, self._if_exists)

    @dataclass_json
    @dataclass
    class AddTableIndexRequest(TableUpdateRequestBase):
        """
        Represents a request to add an index to a table.
        """

        _index: Index = field(
            metadata=config(
                field_name="index",
                encoder=SerdesUtils.table_index_encoder,
                decoder=SerdesUtils.table_index_decoder,
            )
        )

        def __init__(
            self,
            index_type: Index.IndexType,
            name: str,
            field_names: list[list[str]],
        ) -> None:
            """
            The constructor of the add table index request.

            Args:
                index_type (Index.IndexType): The type of the index
                name (str): The name of the index
                field_names (list[list[str]]): The field names under the table contained in the index.
            """
            super().__init__("addTableIndex")
            self._index = Indexes.of(index_type, name, field_names)

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            if not self._index:
                raise ValueError("Index cannot be null")

            if not self._index.type():
                raise ValueError("Index type cannot be null")

            if not self._index.name():
                raise ValueError('"name" field is required')

            if not self._index.field_names():
                raise ValueError(
                    "The index must be set with corresponding column names"
                )

        def get_index(self) -> Index:
            return self._index

        def table_change(self) -> TableChange.AddIndex:
            return TableChange.AddIndex(
                self._index.type(), self._index.name(), self._index.field_names()
            )

    @dataclass_json
    @dataclass
    class DeleteTableIndexRequest(TableUpdateRequestBase):
        """
        Represents a request to delete an index from a table.
        """

        _name: str = field(metadata=config(field_name="name"))
        _if_exists: bool = field(metadata=config(field_name="ifExists"))

        def __init__(self, name: str, if_exists: bool) -> None:
            """
            The constructor of the delete table index request.

            Args:
                name (str): The name of the index
                if_exists (bool): Whether to delete the index if it exists
            """
            super().__init__("deleteTableIndex")
            self._name = name
            self._if_exists = if_exists

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            if not self._name:
                raise ValueError('"name" field is required')

        def get_name(self) -> str:
            return self._name

        def get_if_exists(self) -> bool:
            return self._if_exists

        def table_change(self) -> TableChange.DeleteIndex:
            return TableChange.delete_index(self._name, self._if_exists)

    @dataclass_json
    @dataclass
    class UpdateColumnAutoIncrementRequest(TableUpdateRequestBase):
        """
        Represents a request to update a column autoIncrement from a table.
        """

        _field_name: list[str] = field(metadata=config(field_name="fieldName"))
        _auto_increment: bool = field(metadata=config(field_name="autoIncrement"))

        def __init__(self, field_name: list[str], auto_increment: bool) -> None:
            """
            Constructor for UpdateColumnAutoIncrementRequest.

            Args:
                field_name (list[str]): the field name to update.
                auto_increment (bool): Whether the column is auto-incremented.
            """
            super().__init__("updateColumnAutoIncrement")
            self._field_name = field_name
            self._auto_increment = auto_increment

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            if not self._field_name:
                raise ValueError(
                    '"field_name" field is required and must contain at least one element'
                )

            if any(not name for name in self._field_name):
                raise ValueError('elements in "field_name" cannot be empty')

        def field_name(self) -> list[str]:
            return self._field_name

        def auto_increment(self) -> bool:
            return self._auto_increment

        def table_change(self) -> UpdateColumnAutoIncrement:
            return TableChange.update_column_auto_increment(
                self._field_name, self._auto_increment
            )
