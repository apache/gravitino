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

from typing import TYPE_CHECKING, ClassVar

from gravitino.api.expressions.literals.literal import Literal
from gravitino.api.types.types import Types
from gravitino.dto.rel.expressions.function_arg import FunctionArg

if TYPE_CHECKING:
    from gravitino.api.types.type import Type


class LiteralDTO(Literal[str], FunctionArg):
    """Represents a Literal Data Transfer Object (DTO) that implements the Literal interface."""

    NULL: ClassVar[LiteralDTO]
    """An instance of LiteralDTO with a value of "NULL" and a data type of Types.NullType.get()."""

    def __init__(self, value: str, data_type: Type):
        self._value = value
        self._data_type = data_type

    def value(self) -> str:
        """The literal value.

        Returns:
            str: The value of the literal.
        """
        return self._value

    def data_type(self) -> Type:
        """The data type of the literal.

        Returns:
            Type: The data type of the literal.
        """
        return self._data_type

    def arg_type(self) -> FunctionArg.ArgType:
        return self.ArgType.LITERAL

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, LiteralDTO):
            return False
        return (
            self._data_type == other.data_type()
            and self._value == other.value()
            and self.arg_type() == other.arg_type()
        )

    def __hash__(self) -> int:
        return hash((self.arg_type(), self._data_type, self._value))

    def __str__(self) -> str:
        return f"LiteralDTO(value='{self._value}', data_type={self._data_type})"

    @staticmethod
    def builder() -> Builder:
        """the builder for creating a new instance of LiteralDTO.

        Returns:
            Builder: the builder for creating a new instance of LiteralDTO.
        """
        return LiteralDTO.Builder()

    class Builder:
        """Builder for LiteralDTO."""

        def __init__(self):
            self._data_type = None
            self._value = None

        def with_value(self, value: str) -> LiteralDTO.Builder:
            """Set the value of the literal.

            Args:
                value (str): The value of the literal.

            Returns:
                Builder: The builder.
            """
            self._value = value
            return self

        def with_data_type(self, data_type: Type) -> LiteralDTO.Builder:
            """Set the data type of the literal.

            Args:
                data_type (Type): The data type of the literal.

            Returns:
                Builder: The builder.
            """
            self._data_type = data_type
            return self

        def build(self) -> LiteralDTO:
            """Builds a `LiteralDTO` instance.

            Returns:
                LiteralDTO: The `LiteralDTO` instance.
            """
            return LiteralDTO(value=self._value, data_type=self._data_type)


LiteralDTO.NULL = LiteralDTO("NULL", Types.NullType.get())
