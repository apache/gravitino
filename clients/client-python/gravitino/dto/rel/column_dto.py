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
from typing import Optional

from dataclasses_json import config

from gravitino.api.rel.column import Column
from gravitino.api.rel.expressions.expression import Expression
from gravitino.api.rel.types.type import Type
from gravitino.exceptions.base import (
    IllegalArugmentException,
    IllegalNameIdentifierException,
)


@dataclass
class ColumnDTO(Column):
    """
    A Data Transfer Object (DTO) representing a column of a Table.
    """

    _name: str = field(metadata=config(field_name="name"))
    """The name of the column."""

    _data_type: str = field(metadata=config(field_name="type"))
    """The data type simple string of the column."""

    _comment: Optional[str] = field(metadata=config(field_name="comment"))
    """The comment associated with the column."""

    _nullable: bool = field(metadata=config(field_name="nullable"), default=True)
    """Whether the column value can be null."""

    _auto_increment: bool = field(
        metadata=config(field_name="autoIncrement"), default=False
    )
    """Whether the column is an auto-increment column."""

    _default_value: Optional[Expression] = field(
        metadata=config(field_name="defaultValue"),
        default_factory=lambda: Expression.EMPTY_EXPRESSION,
    )
    """The default value of the column."""

    def __post_init__(self):
        self.validate()

    def name(self) -> str:
        return self._name

    def data_type(self) -> str:
        return self._data_type

    def comment(self) -> Optional[str]:
        return self._comment

    def nullable(self) -> bool:
        return self._nullable

    def auto_increment(self) -> bool:
        return self._auto_increment

    def default_value(self) -> Optional[Expression]:
        return self._default_value

    def validate(self):
        if not self._name:
            raise IllegalNameIdentifierException("Column name cannot be null or empty.")
        if self._data_type is None:
            raise IllegalArugmentException("Column data type cannot be null.")
        # TODO: Implement check for non-nullable columns with a null default value, since Literal is not yet implemented, this check is commented out
        # if not self.nullable and isinstance(self.default_value, Literal) and isinstance(self.default_value.data_type, NullType):
        #     raise ValueError(f"Column cannot be non-nullable with a null default value: {self.name}")

    @staticmethod
    def builder():
        return Builder()


class Builder:
    def __init__(self):
        self._name = None
        self._data_type = None
        self._comment = None
        self._nullable = True
        self._auto_increment = False
        self._default_value = None

    def with_name(self, name: str):
        self._name = name
        return self

    def with_data_type(self, data_type: Type):
        self._data_type = data_type.simple_string()
        return self

    def with_comment(self, comment: Optional[str]):
        self._comment = comment
        return self

    def with_nullable(self, nullable: bool):
        self._nullable = nullable
        return self

    def with_auto_increment(self, auto_increment: bool):
        self._auto_increment = auto_increment
        return self

    def with_default_value(self, default_value: Optional[Expression]):
        self._default_value = default_value
        return self

    def build(self) -> ColumnDTO:
        if self._name is None:
            raise IllegalArugmentException("Column name cannot be null.")
        if self._data_type is None:
            raise IllegalArugmentException("Column data type cannot be null.")
        return ColumnDTO(
            _name=self._name,
            _data_type=self._data_type,
            _comment=self._comment,
            _nullable=self._nullable,
            _auto_increment=self._auto_increment,
            _default_value=(
                self._default_value
                if self._default_value is not None
                else Expression.EMPTY_EXPRESSION
            ),
        )
