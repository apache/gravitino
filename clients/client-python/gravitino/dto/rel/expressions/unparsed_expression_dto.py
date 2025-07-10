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

from gravitino.api.expressions.unparsed_expression import UnparsedExpression
from gravitino.dto.rel.expressions.function_arg import FunctionArg


class UnparsedExpressionDTO(UnparsedExpression, FunctionArg):
    """Data transfer object representing an unparsed expression."""

    def __init__(self, unparsed_expression: str):
        self._unparsed_expression = unparsed_expression

    def unparsed_expression(self) -> str:
        """Returns the unparsed expression as a string.

        Returns:
            str: The value of the unparsed expression.
        """
        return self._unparsed_expression

    def arg_type(self) -> FunctionArg.ArgType:
        return self.ArgType.UNPARSED

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, UnparsedExpressionDTO):
            return False
        return self._unparsed_expression == other.unparsed_expression()

    def __hash__(self) -> int:
        return hash((self.arg_type(), self._unparsed_expression))

    def __str__(self) -> str:
        return (
            f"UnparsedExpressionDTO{{unparsedExpression='{self._unparsed_expression}'}}"
        )

    @staticmethod
    def builder() -> Builder:
        """A builder instance for `UnparsedExpressionDTO`.

        Returns:
            Builder: A builder instance for `UnparsedExpressionDTO`.
        """
        return UnparsedExpressionDTO.Builder()

    class Builder:
        """Builder for `UnparsedExpressionDTO`."""

        def __init__(self):
            self._unparsed_expression = None

        def with_unparsed_expression(
            self, unparsed_expression: str
        ) -> UnparsedExpressionDTO.Builder:
            """Set the unparsed expression.

            Args:
                unparsed_expression (str): The unparsed expression.

            Returns:
                Builder: The builder.
            """

            self._unparsed_expression = unparsed_expression
            return self

        def build(self) -> UnparsedExpressionDTO:
            """Build the unparsed expression.

            Returns:
                UnparsedExpressionDTO: The unparsed expression.
            """

            return UnparsedExpressionDTO(unparsed_expression=self._unparsed_expression)
