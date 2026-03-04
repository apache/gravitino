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

from typing import Optional

from gravitino.api.rel.types.type import Type


class FunctionColumn:
    """Represents a return column of a table-valued function."""

    def __init__(self, name: str, data_type: Type, comment: Optional[str] = None):
        """Create a FunctionColumn instance.

        Args:
            name: The column name.
            data_type: The column type.
            comment: The optional comment of the column.

        Raises:
            ValueError: If name is null or empty, or data_type is null.
        """
        if not name or not name.strip():
            raise ValueError("Function column name cannot be null or empty")
        self._name = name

        if data_type is None:
            raise ValueError("Function column type cannot be null")
        self._data_type = data_type

        self._comment = comment

    @classmethod
    def of(
        cls, name: str, data_type: Type, comment: Optional[str] = None
    ) -> "FunctionColumn":
        """Create a FunctionColumn instance.

        Args:
            name: The column name.
            data_type: The column type.
            comment: The optional comment of the column.

        Returns:
            A FunctionColumn instance.
        """
        return cls(name, data_type, comment)

    def name(self) -> str:
        """Returns the column name."""
        return self._name

    def data_type(self) -> Type:
        """Returns the column type."""
        return self._data_type

    def comment(self) -> Optional[str]:
        """Returns the optional column comment, None if not provided."""
        return self._comment

    def __eq__(self, other) -> bool:
        if not isinstance(other, FunctionColumn):
            return False
        return (
            self._name == other._name
            and self._data_type == other._data_type
            and self._comment == other._comment
        )

    def __hash__(self) -> int:
        return hash((self._name, self._data_type, self._comment))

    def __repr__(self) -> str:
        return (
            f"FunctionColumn(name='{self._name}', dataType={self._data_type}, "
            f"comment='{self._comment}')"
        )
