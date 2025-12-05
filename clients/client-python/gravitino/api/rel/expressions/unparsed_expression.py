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

from gravitino.api.rel.expressions.expression import Expression


class UnparsedExpression(Expression):
    """
    Represents an expression that is not parsed yet.
    The parsed expression is represented by FunctionExpression, literal.py, or NamedReference.
    """

    def unparsed_expression(self) -> str:
        """
        Returns the unparsed expression as a string.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def children(self) -> list[Expression]:
        """
        Unparsed expressions do not have children.
        """
        return Expression.EMPTY_EXPRESSION

    @staticmethod
    def of(unparsed_expression: str) -> UnparsedExpressionImpl:
        """
        Creates a new UnparsedExpression with the given unparsed expression.


        :param unparsed_expression: The unparsed expression as a string.
        :return: The created UnparsedExpression.
        """
        return UnparsedExpressionImpl(unparsed_expression)


class UnparsedExpressionImpl(UnparsedExpression):
    """
    An implementation of the UnparsedExpression interface.
    """

    def __init__(self, unparsed_expression: str):
        super().__init__()
        self._unparsed_expression = unparsed_expression

    def unparsed_expression(self) -> str:
        return self._unparsed_expression

    def __eq__(self, other: object) -> bool:
        if isinstance(other, UnparsedExpressionImpl):
            return self._unparsed_expression == other._unparsed_expression
        return False

    def __hash__(self) -> int:
        return hash(self._unparsed_expression)

    def __str__(self) -> str:
        """
        Returns the string representation of the unparsed expression.
        """
        return f"UnparsedExpressionImpl{{unparsedExpression='{self._unparsed_expression}'}}"
