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

from abc import abstractmethod
from enum import Enum, unique
from typing import TYPE_CHECKING, ClassVar, List

from gravitino.api.expressions.expression import Expression
from gravitino.dto.rel.partition_utils import PartitionUtils

if TYPE_CHECKING:
    from gravitino.dto.rel.column_dto import ColumnDTO


class FunctionArg(Expression):
    """An argument of a function."""

    EMPTY_ARGS: ClassVar[List[FunctionArg]] = []

    @abstractmethod
    def arg_type(self) -> ArgType:
        """Arguments type of the function.

        Returns:
            ArgType: The type of this argument.
        """
        pass

    def validate(self, columns: List[ColumnDTO]) -> None:
        """Validates the function argument.

        Args:
            columns (List[ColumnDTO]): The columns of the table.

        Raises:
            IllegalArgumentException: If the function argument is invalid.
        """
        validate_field_existence = PartitionUtils.validate_field_existence
        for ref in self.references():
            validate_field_existence(columns, ref.field_name())

    @unique
    class ArgType(str, Enum):
        """The type of the argument.

        The supported types are:

        - `LITERAL`: A literal argument.
        - `FIELD`: A field argument.
        - `FUNCTION`: A function argument.
        - `UNPARSED`: An unparsed argument.
        """

        LITERAL = "literal"
        FIELD = "field"
        FUNCTION = "function"
        UNPARSED = "unparsed"
