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
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from dataclasses_json import config, dataclass_json

from gravitino.api.rel.expressions.expression import Expression
from gravitino.api.rel.indexes.index import Index
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
from gravitino.api.rel.types.json_serdes import TypeSerdes
from gravitino.api.rel.types.type import Type
from gravitino.dto.rel.expressions.json_serdes.column_default_value_serdes import (
    ColumnDefaultValueSerdes,
)
from gravitino.dto.rel.indexes.json_serdes.index_serdes import IndexSerdes
from gravitino.dto.rel.json_serdes.column_position_serdes import ColumnPositionSerdes
from gravitino.rest.rest_message import RESTRequest
from gravitino.utils import StringUtils
from gravitino.utils.precondition import Precondition

from ...api.rel.table_change import AddColumn


@dataclass_json
@dataclass
class TableUpdateRequestBase(RESTRequest, ABC):
    """Base class for all table update requests."""

    _type: str = field(init=False, metadata=config(field_name="@type"))

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
        _new_schema_name: typing.Optional[str] = field(
            default=None,
            metadata=config(
                field_name="newSchemaName",
                exclude=lambda value: value is None,
            ),
        )

        def __post_init__(self) -> None:
            self._type = "rename"

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            Precondition.check_string_not_empty(
                self._new_name,
                '"newName" field is required and cannot be empty',
            )

        @property
        def new_name(self) -> str:
            return self._new_name

        @property
        def new_schema_name(self) -> typing.Optional[str]:
            return self._new_schema_name

        def table_change(self) -> TableChange.RenameTable:
            return TableChange.rename(self._new_name, self._new_schema_name)

    @dataclass_json
    @dataclass
    class UpdateTableCommentRequest(TableUpdateRequestBase):
        """
        Update request to change a table comment
        """

        _new_comment: str = field(metadata=config(field_name="newComment"))

        def __post_init__(self) -> None:
            self._type = "updateComment"

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            # Validates the fields of the request. Always pass.
            pass

        @property
        def new_comment(self) -> str:
            return self._new_comment

        def table_change(self) -> TableChange.UpdateComment:
            return TableChange.update_comment(self._new_comment)

    @dataclass_json
    @dataclass
    class SetTablePropertyRequest(TableUpdateRequestBase):
        """
        Update request to set a table property
        """

        _prop: str = field(metadata=config(field_name="property"))
        _prop_value: str = field(metadata=config(field_name="value"))

        def __post_init__(self) -> None:
            self._type = "setProperty"

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            Precondition.check_string_not_empty(
                self._prop,
                '"property" field is required',
            )

            Precondition.check_string_not_empty(
                self._prop_value,
                '"value" field is required',
            )

        @property
        def prop(self) -> str:
            return self._prop

        @property
        def prop_value(self) -> str:
            return self._prop_value

        def table_change(self) -> TableChange.SetProperty:
            return TableChange.set_property(self._prop, self._prop_value)

    @dataclass_json
    @dataclass
    class RemoveTablePropertyRequest(TableUpdateRequestBase):
        """
        Update request to remove a table property
        """

        _property: str = field(metadata=config(field_name="property"))

        def __post_init__(self) -> None:
            self._type = "removeProperty"

        def validate(self) -> None:
            """
            Validates the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            Precondition.check_string_not_empty(
                self._property,
                '"property" field is required',
            )

        @property
        def property(self) -> str:
            return self._property

        def table_change(self) -> TableChange.RemoveProperty:
            return TableChange.remove_property(self._property)

    @dataclass_json
    @dataclass
    # pylint: disable=too-many-instance-attributes
    class AddTableColumnRequest(TableUpdateRequestBase):
        """Represents a request to add a column to a table."""

        _field_name: list[str] = field(metadata=config(field_name="fieldName"))
        _data_type: Type = field(
            metadata=config(
                field_name="type",
                encoder=TypeSerdes.serialize,
                decoder=TypeSerdes.deserialize,
            )
        )
        _comment: typing.Optional[str] = field(metadata=config(field_name="comment"))
        _position: typing.Optional[TableChange.ColumnPosition] = field(
            metadata=config(
                field_name="position",
                encoder=ColumnPositionSerdes.serialize,
                decoder=ColumnPositionSerdes.deserialize,
            )
        )
        _default_value: typing.Optional[Expression] = field(
            metadata=config(
                field_name="defaultValue",
                encoder=ColumnDefaultValueSerdes.serialize,
                decoder=ColumnDefaultValueSerdes.deserialize,
                exclude=lambda v: v is None,
            )
        )
        _nullable: bool = field(default=True, metadata=config(field_name="nullable"))
        _auto_increment: bool = field(
            default=False, metadata=config(field_name="autoIncrement")
        )

        def __post_init__(self) -> None:
            self._type = "addColumn"

        def validate(self) -> None:
            """
            Validates the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            Precondition.check_argument(
                self._field_name,
                "Field name must be specified",
            )
            Precondition.check_argument(
                all(StringUtils.is_not_blank(name) for name in self._field_name),
                'elements in "field_name" cannot be empty',
            )
            Precondition.check_argument(
                self._data_type is not None,
                '"type" field is required and cannot be empty',
            )

        @property
        def field_name(self) -> list[str]:
            return self._field_name

        @property
        def data_type(self) -> Type:
            return self._data_type

        @property
        def comment(self) -> typing.Optional[str]:
            return self._comment

        @property
        def position(self) -> typing.Optional[TableChange.ColumnPosition]:
            return self._position

        @property
        def default_value(self) -> typing.Optional[Expression]:
            return self._default_value

        @property
        def is_nullable(self) -> bool:
            return self._nullable

        @property
        def is_auto_increment(self) -> bool:
            return self._auto_increment

        def table_change(self) -> AddColumn:
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

        def __post_init__(self) -> None:
            self._type = "renameColumn"

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            Precondition.check_argument(
                self._old_field_name,
                '"old_field_name" field is required and must contain at least one element',
            )
            Precondition.check_argument(
                all(StringUtils.is_not_blank(name) for name in self._old_field_name),
                'elements in "old_field_name" cannot be empty',
            )
            Precondition.check_string_not_empty(
                self._new_field_name,
                '"newFieldName" field is required and cannot be empty',
            )

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
                encoder=ColumnDefaultValueSerdes.serialize,
                decoder=ColumnDefaultValueSerdes.deserialize,
            )
        )

        def __post_init__(self) -> None:
            self._type = "updateColumnDefaultValue"

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            Precondition.check_argument(
                self._field_name,
                '"field_name" field is required and must contain at least one element',
            )
            Precondition.check_argument(
                all(StringUtils.is_not_blank(name) for name in self._field_name),
                'elements in "field_name" cannot be empty',
            )
            Precondition.check_argument(
                self._new_default_value is not None
                and self._new_default_value != Expression.EMPTY_EXPRESSION,
                '"newDefaultValue" field is required and cannot be empty',
            )

        @property
        def field_name(self) -> list[str]:
            return self._field_name

        @property
        def new_default_value(self) -> Expression:
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
                encoder=TypeSerdes.serialize,
                decoder=TypeSerdes.deserialize,
            )
        )

        def __post_init__(self) -> None:
            self._type = "updateColumnType"

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            Precondition.check_argument(
                self._field_name,
                '"field_name" field is required and must contain at least one element',
            )
            Precondition.check_argument(
                all(StringUtils.is_not_blank(name) for name in self._field_name),
                'elements in "field_name" cannot be empty',
            )
            Precondition.check_argument(
                self._new_type is not None,
                '"newType" field is required and cannot be null',
            )

        @property
        def field_name(self) -> list[str]:
            return self._field_name

        @property
        def new_type(self) -> Type:
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

        def __post_init__(self) -> None:
            self._type = "updateColumnComment"

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            Precondition.check_argument(
                self._field_name,
                '"field_name" field is required and must contain at least one element',
            )
            Precondition.check_argument(
                all(StringUtils.is_not_blank(name) for name in self._field_name),
                'elements in "field_name" cannot be empty',
            )
            Precondition.check_string_not_empty(
                self._new_comment,
                '"newComment" field is required and cannot be empty',
            )

        @property
        def field_name(self) -> list[str]:
            return self._field_name

        @property
        def new_comment(self) -> str:
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
                encoder=ColumnPositionSerdes.serialize,
                decoder=ColumnPositionSerdes.deserialize,
            )
        )

        def __post_init__(self) -> None:
            self._type = "updateColumnPosition"

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            Precondition.check_argument(
                self._field_name,
                '"field_name" field is required and must contain at least one element',
            )
            Precondition.check_argument(
                all(StringUtils.is_not_blank(name) for name in self._field_name),
                'elements in "field_name" cannot be empty',
            )
            Precondition.check_argument(
                self._new_position is not None,
                '"newPosition" field is required and cannot be null',
            )

        @property
        def field_name(self) -> list[str]:
            return self._field_name

        @property
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

        def __post_init__(self) -> None:
            self._type = "updateColumnNullability"

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            Precondition.check_argument(
                self._field_name,
                '"field_name" field is required and must contain at least one element',
            )
            Precondition.check_argument(
                all(StringUtils.is_not_blank(name) for name in self._field_name),
                'elements in "field_name" cannot be empty',
            )

        @property
        def field_name(self) -> list[str]:
            return self._field_name

        @property
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

        def __post_init__(self) -> None:
            self._type = "deleteColumn"

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            Precondition.check_argument(
                self._field_name,
                '"field_name" field is required and must contain at least one element',
            )
            Precondition.check_argument(
                all(StringUtils.is_not_blank(name) for name in self._field_name),
                'elements in "field_name" cannot be empty',
            )

        @property
        def field_name(self) -> list[str]:
            return self._field_name

        @property
        def if_exists(self) -> bool:
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
                encoder=IndexSerdes.serialize,
                decoder=IndexSerdes.deserialize,
            )
        )

        def __post_init__(self) -> None:
            self._type = "addTableIndex"

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            Precondition.check_argument(self._index is not None, "Index cannot be null")
            Precondition.check_argument(
                self._index.type() is not None,
                "Index type cannot be null",
            )
            Precondition.check_string_not_empty(
                self._index.name(),
                '"name" field is required',
            )
            Precondition.check_argument(
                self._index.field_names() is not None
                and len(self._index.field_names()) > 0,
                "The index must be set with corresponding column names",
            )

        @property
        def index(self) -> Index:
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

        def __post_init__(self) -> None:
            self._type = "deleteTableIndex"

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            Precondition.check_string_not_empty(
                self._name,
                '"name" field is required',
            )

        @property
        def name(self) -> str:
            return self._name

        @property
        def if_exists(self) -> bool:
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

        def __post_init__(self) -> None:
            self._type = "updateColumnAutoIncrement"

        def validate(self) -> None:
            """
            Validate the request.

            Raises:
                ValueError: If the request is invalid, this exception is thrown.
            """
            Precondition.check_argument(
                self._field_name,
                '"field_name" field is required and must contain at least one element',
            )
            Precondition.check_argument(
                all(StringUtils.is_not_blank(name) for name in self._field_name),
                'elements in "field_name" cannot be empty',
            )

        @property
        def field_name(self) -> list[str]:
            return self._field_name

        @property
        def auto_increment(self) -> bool:
            return self._auto_increment

        def table_change(self) -> UpdateColumnAutoIncrement:
            return TableChange.update_column_auto_increment(
                self._field_name, self._auto_increment
            )
