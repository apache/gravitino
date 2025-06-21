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

from dataclasses import dataclass, field
from typing import List, Optional, Union, cast

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.column import Column
from gravitino.api.expressions.expression import Expression
from gravitino.api.types.json_serdes.type_serdes import TypeSerdes
from gravitino.api.types.type import Type
from gravitino.api.types.types import Types
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO
from gravitino.utils.precondition import Precondition


@dataclass
class ColumnDTO(Column, DataClassJsonMixin):
    """Represents a Model DTO (Data Transfer Object)."""

    _name: str = field(metadata=config(field_name="name"))
    """The name of the column."""

    _data_type: Type = field(
        metadata=config(
            field_name="type",
            encoder=TypeSerdes.serialize,
            decoder=TypeSerdes.deserialize,
        )
    )
    """The data type of the column."""

    _comment: str = field(metadata=config(field_name="comment"))
    """The comment associated with the column."""

    # TODO: We shall specify encoder/decoder in the future PR. They're now dummy serdes.
    _default_value: Optional[Union[Expression, List[Expression]]] = field(
        default_factory=lambda: Column.DEFAULT_VALUE_NOT_SET,
        metadata=config(
            field_name="defaultValue",
            encoder=lambda _: None,
            decoder=lambda _: Column.DEFAULT_VALUE_NOT_SET,
            exclude=lambda value: value is None
            or value is Column.DEFAULT_VALUE_NOT_SET,
        ),
    )
    """The default value of the column."""

    _nullable: bool = field(default=True, metadata=config(field_name="nullable"))
    """Whether the column value can be null."""

    _auto_increment: bool = field(
        default=False, metadata=config(field_name="autoIncrement")
    )
    """Whether the column is an auto-increment column."""

    def name(self) -> str:
        return self._name

    def data_type(self) -> Type:
        return self._data_type

    def comment(self) -> str:
        return self._comment

    def nullable(self) -> bool:
        return self._nullable

    def auto_increment(self) -> bool:
        return self._auto_increment

    def default_value(self) -> Union[Expression, List[Expression]]:
        return self._default_value

    def validate(self) -> None:
        Precondition.check_string_not_empty(
            self._name, "Column name cannot be null or empty."
        )
        Precondition.check_argument(
            self._data_type is not None, "Column data type cannot be null."
        )
        non_nullable_condition = (
            not self._nullable
            and isinstance(self._default_value, LiteralDTO)
            and cast(LiteralDTO, self._default_value).data_type()
            == Types.NullType.get()
        )
        Precondition.check_argument(
            not non_nullable_condition,
            f"Column cannot be non-nullable with a null default value: {self._name}.",
        )

    @classmethod
    def builder(
        cls,
        name: str,
        data_type: Type,
        comment: str,
        nullable: bool = True,
        auto_increment: bool = False,
        default_value: Optional[Expression] = None,
    ) -> ColumnDTO:
        Precondition.check_argument(name is not None, "Column name cannot be null")
        Precondition.check_argument(
            data_type is not None, "Column data type cannot be null"
        )
        return ColumnDTO(
            _name=name,
            _data_type=data_type,
            _comment=comment,
            _nullable=nullable,
            _auto_increment=auto_increment,
            _default_value=(
                Column.DEFAULT_VALUE_NOT_SET if default_value is None else default_value
            ),
        )

    def __eq__(self, other: ColumnDTO) -> bool:
        if not isinstance(other, ColumnDTO):
            return False
        return (
            self._name == other._name
            and self._data_type == other._data_type
            and self._comment == other._comment
            and self._nullable == other._nullable
            and self._auto_increment == other._auto_increment
            and self._default_value == other._default_value
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._name,
                self._data_type,
                self._comment,
                self._nullable,
                self._auto_increment,
                (
                    None
                    if self._default_value is Column.DEFAULT_VALUE_NOT_SET
                    else self._default_value
                ),
            )
        )
