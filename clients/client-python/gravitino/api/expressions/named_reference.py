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

from gravitino.api.expressions.expression import Expression


class NamedReference(Expression):
    """
    Represents a field or column reference in the public logical expression API.
    """

    @staticmethod
    def field(field_name: list[str]) -> FieldReference:
        """
        Returns a FieldReference for the given field name(s). The array of field name(s) is
        used to reference nested fields. For example, if we have a struct column named "student" with a
        data type of StructType{"name": StringType, "age": IntegerType}, we can reference the field
        "name" by calling field("student", "name").

        @param field_name the field name(s)
        @return a FieldReference for the given field name(s)
        """
        return FieldReference(field_name)

    @staticmethod
    def field_from_column(column_name: str) -> FieldReference:
        """Returns a FieldReference for the given column name."""
        return FieldReference([column_name])

    def field_name(self) -> list[str]:
        """
        Returns the referenced field name as a list of string parts.
        Must be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def children(self) -> list[Expression]:
        """Named references do not have children."""
        return Expression.EMPTY_EXPRESSION

    def references(self) -> list[NamedReference]:
        """Named references reference themselves."""
        return [self]


class FieldReference(NamedReference):
    """
    A NamedReference that references a field or column.
    """

    _field_names: list[str]

    def __init__(self, field_names: list[str]):
        super().__init__()
        self._field_names = field_names

    def field_name(self) -> list[str]:
        return self._field_names

    def __eq__(self, other: object) -> bool:
        if isinstance(other, FieldReference):
            return self._field_names == other._field_names
        return False

    def __hash__(self) -> int:
        return hash(tuple(self._field_names))

    def __str__(self) -> str:
        """Returns the string representation of the field reference."""
        return ".".join(self._field_names)
