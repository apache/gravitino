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

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import config

from gravitino.api.rel.types.type import Type
from gravitino.api.rel.expressions.expression import Expression

@dataclass
class Column(ABC):
    """
    An interface representing a column of a Table. It defines basic properties of a column,
    such as name and data type.
    """
    _name: str = field(metadata=config(field_name="name"))
    """The name of the column."""

    _data_type: Type = field(metadata=config(field_name="dataType"))
    """The data type of the column."""

    _comment: Optional[str] = field(metadata=config(field_name="comment"))
    """The comment associated with the column."""

    _nullable: bool = field(metadata=config(field_name="nullable"), default=True)
    """Whether the column value can be null."""

    _auto_increment: bool = field(metadata=config(field_name="autoIncrement"), default=False)
    """Whether the column is an auto-increment column."""

    _default_value: Optional[Expression] = field(
        metadata=config(field_name="defaultValue"),
        default_factory=lambda: Expression.EMPTY_EXPRESSION,
    )
    """The default value of the column."""

    @abstractmethod
    def name(self):
        pass

    @abstractmethod
    def data_type(self):
        pass

    @abstractmethod
    def comment(self):
        pass

    @abstractmethod
    def nullable(self):
        pass

    @abstractmethod
    def auto_increment(self):
        pass

    @abstractmethod
    def default_value(self):
        pass

    @staticmethod
    def of(name, data_type, comment=None, nullable=True, auto_increment=False, default_value=Expression.EMPTY_EXPRESSION):
        return ColumnImpl(name, data_type, comment, nullable, auto_increment, default_value)

class ColumnImpl(Column):
    def name(self):
        return self._name

    def data_type(self):
        return self._data_type

    def comment(self):
        return self._comment

    def nullable(self):
        return self._nullable

    def auto_increment(self):
        return self._auto_increment

    def default_value(self):
        return self._default_value
