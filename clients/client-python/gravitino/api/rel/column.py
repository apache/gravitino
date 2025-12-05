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
from typing import Optional

from gravitino.api.rel.expressions.expression import Expression
from gravitino.api.rel.expressions.function_expression import FunctionExpression
from gravitino.api.rel.types.type import Type
from gravitino.api.tag.supports_tags import SupportsTags
from gravitino.exceptions.base import UnsupportedOperationException
from gravitino.utils.precondition import Precondition


class Column(ABC):
    """An interface representing a column of a `Table`.

    It defines basic properties of a column, such as name and data type.

    Catalog implementation needs to implement it. They should consume it in APIs like
    `TableCatalog.createTable(NameIdentifier, List[Column], str, Dict)`, and report it in
    `Table.columns()` a default value and a generation expression.
    """

    DEFAULT_VALUE_NOT_SET = Expression.EMPTY_EXPRESSION
    """A default value that indicates the default value is not set. This is used in `default_value()`"""

    DEFAULT_VALUE_OF_CURRENT_TIMESTAMP: Expression = FunctionExpression.of(
        "current_timestamp"
    )
    """
    A default value that indicates the default value will be set to the current timestamp.
    This is used in `default_value()`
    """

    @abstractmethod
    def name(self) -> str:
        """Get the name of this column.

        Returns:
            str: The name of this column.
        """
        pass

    @abstractmethod
    def data_type(self) -> Type:
        """Get the name of this column.

        Returns:
            Type: The data type of this column.
        """
        pass

    @abstractmethod
    def comment(self) -> Optional[str]:
        """Get the comment of this column.

        Returns:
            Optional[str]: The comment of this column, `None` if not specified.
        """
        pass

    @abstractmethod
    def nullable(self) -> bool:
        """Indicate if this column may produce null values.

        Returns:
            bool: `True` if this column may produce null values. Default is `True`.
        """
        return True

    @abstractmethod
    def auto_increment(self) -> bool:
        """Indicate if this column is an auto-increment column.

        Returns:
            bool: `True` if this column is an auto-increment column. Default is `False`.
        """
        return False

    @abstractmethod
    def default_value(self) -> Expression:
        """Get the default value of this column

        Returns:
            Expression:
                The default value of this column, `Column.DEFAULT_VALUE_NOT_SET` if not specified.
        """
        pass

    def supports_tags(self) -> SupportsTags:
        """Return the `SupportsTags` if the column supports tag operations.

        Raises:
            UnsupportedOperationException: if the column does not support tag operations.

        Returns:
            SupportsTags: the `SupportsTags` if the column supports tag operations.
        """
        raise UnsupportedOperationException("Column does not support tag operations.")

    @staticmethod
    def of(
        name: str,
        data_type: Type,
        comment: Optional[str] = None,
        nullable: bool = True,
        auto_increment: bool = False,
        default_value: Optional[Expression] = None,
    ) -> ColumnImpl:
        """Create a `Column` instance.

        Args:
            name (str):
                The name of the column.
            data_type (Type):
                The data type of the column.
            comment (Optional[str], optional):
                The comment of the column. Defaults to `None`.
            nullable (bool, optional):
                `True` if the column may produce null values. Defaults to `True`.
            auto_increment (bool, optional):
                `True` if the column is an auto-increment column. Defaults to `False`.
            default_value (Optional[Expression], optional):
                The default value of this column, `Column.DEFAULT_VALUE_NOT_SET` if `None`. Defaults to `None`.

        Returns:
            ColumnImpl: A `Column` instance.
        """
        return ColumnImpl(
            name=name,
            data_type=data_type,
            comment=comment,
            nullable=nullable,
            auto_increment=auto_increment,
            default_value=(
                Column.DEFAULT_VALUE_NOT_SET if default_value is None else default_value
            ),
        )


class ColumnImpl(Column):
    """The implementation of `Column` for users to use API."""

    def __init__(
        self,
        name: str,
        data_type: Type,
        comment: Optional[str],
        nullable: bool,
        auto_increment: bool,
        default_value: Optional[Expression],
    ):
        Precondition.check_string_not_empty(name, "Column name cannot be null")
        Precondition.check_argument(
            data_type is not None, "Column data type cannot be null"
        )
        self._name = name
        self._data_type = data_type
        self._comment = comment
        self._nullable = nullable
        self._auto_increment = auto_increment
        self._default_value = default_value

    def name(self) -> str:
        return self._name

    def data_type(self) -> Type:
        return self._data_type

    def comment(self) -> Optional[str]:
        return self._comment

    def nullable(self) -> bool:
        return self._nullable

    def auto_increment(self) -> bool:
        return self._auto_increment

    def default_value(self) -> Expression:
        return self._default_value

    def __eq__(self, other: ColumnImpl) -> bool:
        if not isinstance(other, ColumnImpl):
            return False
        return all(
            [
                self._name == other.name(),
                self._data_type == other.data_type(),
                self._comment == other.comment(),
                self._nullable == other.nullable(),
                self._auto_increment == other.auto_increment(),
                self._default_value == other.default_value(),
            ]
        )

    def __hash__(self):
        return hash(
            (
                self._name,
                self._data_type,
                self._comment,
                self._nullable,
                self._auto_increment,
                (
                    tuple(self._default_value)
                    if self._default_value is Column.DEFAULT_VALUE_NOT_SET
                    else self._default_value
                ),
            )
        )
