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

from gravitino.api.expressions.expression import Expression


class FunctionExpression(Expression):
    """
    The interface of a function expression. A function expression is an expression that takes a
    function name and a list of arguments.
    """

    @staticmethod
    def of(function_name: str, *arguments: Expression) -> FuncExpressionImpl:
        """
        Creates a new FunctionExpression with the given function name.
        If no arguments are provided, it uses an empty expression.

        :param function_name: The name of the function.
        :param arguments: The arguments to the function (optional).
        :return: The created FunctionExpression.
        """
        arguments = list(arguments) if arguments else Expression.EMPTY_EXPRESSION
        return FuncExpressionImpl(function_name, arguments)

    @abstractmethod
    def function_name(self) -> str:
        """Returns the function name."""

    @abstractmethod
    def arguments(self) -> list[Expression]:
        """Returns the arguments passed to the function."""

    def children(self) -> list[Expression]:
        """Returns the arguments as children."""
        return self.arguments()


class FuncExpressionImpl(FunctionExpression):
    """
    A concrete implementation of the FunctionExpression interface.
    """

    _function_name: str
    _arguments: list[Expression]

    def __init__(self, function_name: str, arguments: list[Expression]):
        super().__init__()
        self._function_name = function_name
        self._arguments = arguments

    def function_name(self) -> str:
        return self._function_name

    def arguments(self) -> list[Expression]:
        return self._arguments

    def __str__(self) -> str:
        if not self._arguments:
            return f"{self._function_name}()"
        arguments_str = ", ".join(map(str, self._arguments))
        return f"{self._function_name}({arguments_str})"

    def __eq__(self, other: FuncExpressionImpl) -> bool:
        if self is other:
            return True
        if other is None or self.__class__ is not other.__class__:
            return False
        return (
            self._function_name == other.function_name()
            and self._arguments == other.arguments()
        )

    def __hash__(self) -> int:
        return hash((self._function_name, tuple(self._arguments)))
